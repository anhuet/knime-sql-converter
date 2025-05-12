// src/functions/convertConcatenateNodeToSQL.js

// Assuming these helper functions are available in your project's common directory
// If not, you'll need to ensure they are correctly imported or defined.
import { getEntryValue } from "../common/getEntryValue";
import { findConfigByKey } from "../common/findConfigByKey";

/**
 * Finds the direct predecessor node(s) for a given node ID from a list of processed nodes.
 * (This is a common helper function, also used by other converters)
 * @param {string | number} currentNodeId - The ID of the current node.
 * @param {Array<object>} allProcessedNodes - An array of node objects that have already been processed,
 * each expected to have an 'id', 'nodes' (output columns), 'sqlAlias', and 'nextNodes' (successor IDs) property.
 * @returns {Array<object>} - An array of predecessor node objects found in allProcessedNodes.
 */
const findPredecessorNodeDetails = (currentNodeId, allProcessedNodes) => {
  if (!Array.isArray(allProcessedNodes)) return [];
  const currentIdStr = String(currentNodeId);

  const predecessorConnections = allProcessedNodes.filter(
    (node) =>
      node &&
      Array.isArray(node.nextNodes) &&
      node.nextNodes.some((nextNodeId) => String(nextNodeId) === currentIdStr)
  );

  // Now, from these connections, get the full detail from allProcessedNodes
  return predecessorConnections.map((connection) => {
    // Find the full node detail in allProcessedNodes using the connection's ID
    // This assumes allProcessedNodes contains entries with 'id', 'sqlAlias', and 'nodes' (schema)
    const fullNodeDetail = allProcessedNodes.find(
      (n) => n.id === connection.id
    );

    return {
      id: fullNodeDetail.id,
      sqlAlias: fullNodeDetail.nodeName,
      columns: fullNodeDetail.nodes, // This is the schema of the predecessor
      incomplete: false,
    };
  });
};

/**
 * Converts a KNIME Concatenate (AppendedRowsNodeFactory) node configuration to an SQL query.
 *
 * @param {object} nodeConfigJson - The node configuration JSON object from settings.xml.
 * MUST include an 'id' property for the current node.
 * @param {Array<object>} allProcessedNodes - Array of all previously processed nodes. Each object
 * should have 'id', 'sqlAlias' (its SQL output name),
 * 'nodes' (its output column names/schema), and 'nextNodes'.
 * @returns {string} - The generated SQL query (typically using UNION ALL or UNION).
 */
export function convertConcatenateNodeToSQL(
  nodeConfigJson,
  nodeId,
  allProcessedNodes
) {
  const factory = getEntryValue(nodeConfigJson?.entry, "factory");
  const CONCATENATE_FACTORY =
    "org.knime.base.node.preproc.append.row.AppendedRowsNodeFactory";
  if (factory !== CONCATENATE_FACTORY) {
    return `Error: Expected Concatenate node factory (${CONCATENATE_FACTORY}), but got ${
      factory || "N/A"
    }.`;
  }

  const currentNodeId = nodeId;
  if (currentNodeId === undefined) {
    console.error(
      "convertConcatenateNodeToSQL: nodeConfigJson is missing the 'id' property.",
      nodeConfigJson
    );
    return "Error: Node ID is missing from nodeConfigJson. Cannot process Concatenate node.";
  }

  const modelNode = findConfigByKey(nodeConfigJson.config, "model");
  if (!modelNode) {
    return `Error: Node ${currentNodeId} (Concatenate): Model configuration not found.`;
  }

  const intersectionOfColumns =
    getEntryValue(modelNode.entry, "intersection_of_columns") === "true";
  // Other settings like 'fail_on_duplicates', 'create_new_rowids', 'append_suffix'
  // are not directly translated into the SQL UNION structure but could be commented.
  // const failOnDuplicates = getEntryValue(modelNode.entry, "fail_on_duplicates") === "true";

  const predecessorDetails = findPredecessorNodeDetails(
    currentNodeId,
    allProcessedNodes
  );

  if (!predecessorDetails || predecessorDetails.length === 0) {
    return `Error: Node ${currentNodeId} (Concatenate): No predecessor inputs found or predecessor details (sqlAlias, columns) are missing. Cannot generate SQL.`;
  }
  // Check if any predecessor has incomplete info needed for the operation
  if (predecessorDetails.some((p) => !p.sqlAlias || p.incomplete)) {
    const missingInfoNodes = predecessorDetails
      .filter((p) => !p.sqlAlias || p.incomplete)
      .map((p) => p.id);
    return `Error: Node ${currentNodeId} (Concatenate): Predecessor(s) with ID(s) [${missingInfoNodes.join(
      ", "
    )}] are missing required 'sqlAlias' or 'columns' information in allProcessedNodes.`;
  }

  if (predecessorDetails.length === 1) {
    // If only one input, it's effectively a pass-through.
    // Select all columns from that single input.
    const singleInput = predecessorDetails[0];
    // Use SELECT * as the schema might be unknown or complex to list here.
    return `SELECT * FROM "${singleInput.sqlAlias.replace(/"/g, '""')}"; `;
  }

  let unionOperator = "";
  let unionParts = [];

  if (intersectionOfColumns) {
    // --- Intersection of Columns (UNION) ---
    unionOperator = "UNION"; // UNION implies DISTINCT rows over the common columns
    const allInputColumnSets = predecessorDetails.map(
      (p) => new Set(p.columns)
    );
    let commonColumns = [];

    // Find common columns (intersection)
    if (allInputColumnSets.length > 0) {
      commonColumns = [...allInputColumnSets[0]]; // Start with columns from the first predecessor
      for (let i = 1; i < allInputColumnSets.length; i++) {
        commonColumns = commonColumns.filter((col) =>
          allInputColumnSets[i].has(col)
        );
      }
    }

    if (commonColumns.length === 0) {
      return `Error: Node ${currentNodeId} (Concatenate): 'Intersection of columns' is true, but no common columns found across inputs.`;
    }

    const selectColsString = commonColumns
      .map((col) => `"${col.replace(/"/g, '""')}"`)
      .join(",\n  ");

    unionParts = predecessorDetails.map((predecessor) => {
      // Select only the common columns from each predecessor
      return `SELECT\n  ${selectColsString}\nFROM "${predecessor.sqlAlias.replace(
        /"/g,
        '""'
      )}"`;
    });
  } else {
    // --- Union of Rows (UNION ALL) ---
    // KNIME default behavior: Appends rows, assumes compatible structure.
    // Simplest SQL equivalent is UNION ALL with SELECT *.
    // This relies on the assumption that the input tables/CTEs have structures
    // compatible with UNION ALL (same number of columns, compatible types).
    unionOperator = "UNION ALL";

    unionParts = predecessorDetails.map((predecessor) => {
      // Select all columns from each predecessor.
      // NOTE: This assumes the schemas are compatible for UNION ALL.
      // If schemas differ, SQL will raise an error, mirroring KNIME's potential issues.
      return `SELECT *\nFROM "${predecessor.sqlAlias.replace(/"/g, '""')}"`;
    });
  }

  if (unionParts.length === 0) {
    return `Error: Node ${currentNodeId} (Concatenate): Could not construct any parts for the ${unionOperator} operation.`;
  }

  let sqlQuery = unionParts.join(`\n${unionOperator}\n`);

  return sqlQuery.trim();
}
