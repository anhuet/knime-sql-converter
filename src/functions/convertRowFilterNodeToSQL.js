// src/functions/convertRowFilterNodeToSQL.js

// Assuming these helper functions are available in your project's common directory
import { getEntryValue } from "../common/getEntryValue";
import { findConfigByKey } from "../common/findConfigByKey";
import { getArrayValuesFromConfig } from "../common/getArrayValuesFromConfig"; // May be needed for IN/NOT IN

/**
 * Finds the direct predecessor node details for a given node ID.
 * Row Filter node typically has exactly one predecessor.
 * (User-provided version)
 *
 * @param {string | number} currentNodeId - The ID of the current node (passed as argument).
 * @param {Array<object>} allProcessedNodes - Array of processed nodes with 'id', 'nodeName' (used as sqlAlias), 'nodes', 'nextNodes' (assumed array of strings).
 * @returns {object | null} - Object containing details for the predecessor, or null/error if issues occur.
 */
const findSinglePredecessorDetails = (currentNodeId, allProcessedNodes) => {
  // --- User Provided Version ---
  if (!Array.isArray(allProcessedNodes)) {
    console.error(
      "findSinglePredecessorDetails: allProcessedNodes is not an array."
    );
    return null;
  }
  const currentIdStr = String(currentNodeId);

  // Filter nodes where *any* entry in nextNodes matches the currentNodeId (as a string)
  const predecessors = allProcessedNodes.filter(
    (node) =>
      node &&
      Array.isArray(node.nextNodes) &&
      node.nextNodes.some((conn) => {
        // console.log(conn, "conn - checking connection to:", currentIdStr); // Debug log
        // Assumes 'conn' is a simple string ID based on user code.
        // If 'conn' is an object like { targetNodeId: ... }, this check needs modification.
        return (
          conn !== null && conn !== undefined && String(conn) === currentIdStr
        );
      })
  );
  // console.log(predecessors, "predecessors found for node:", currentIdStr); // Debug log

  // --- Added Checks for Robustness ---
  if (predecessors.length === 0) {
    console.warn(
      `Row Filter Node ${currentNodeId}: No predecessors found connecting to this node.`
    );
    return null;
  }
  if (predecessors.length > 1) {
    console.warn(
      `Row Filter Node ${currentNodeId}: Expected 1 predecessor, but found ${predecessors.length}. Using the first one found: ${predecessors[0].id}.`
    );
    // Proceeding with the first one, but this might indicate a workflow parsing issue.
  }

  const predecessorId = predecessors[0].id;
  const fullNodeDetail = allProcessedNodes.find((n) => n.id === predecessorId);

  if (!fullNodeDetail) {
    console.error(
      `Row Filter Node ${currentNodeId}: Could not find full details in allProcessedNodes for predecessor ID ${predecessorId}.`
    );
    return null;
  }
  if (!fullNodeDetail.nodeName) {
    console.error(
      `Row Filter Node ${currentNodeId}: Predecessor node ${predecessorId} is missing the 'nodeName' property (used as sqlAlias).`
    );
    return null;
  }
  if (!Array.isArray(fullNodeDetail.nodes)) {
    console.error(
      `Row Filter Node ${currentNodeId}: Predecessor node ${predecessorId} is missing the 'nodes' array (schema).`
    );
    return null;
  }
  // --- End Added Checks ---

  // Return details using nodeName as sqlAlias as per user's code
  return {
    id: fullNodeDetail.id,
    sqlAlias: fullNodeDetail.nodeName, // Using nodeName as the SQL alias
    columns: fullNodeDetail.nodes, // Schema
  };
  // --- End User Provided Version ---
};

/**
 * Translates a single KNIME Row Filter predicate config to an SQL condition string.
 *
 * @param {object} predicateConfig - The config object for a single predicate,
 * which contains an 'entry' array with keys like 'operator'.
 * @returns {string | null} - The SQL condition string or null if conversion fails.
 */
const translatePredicateToSQL = (predicateConfig) => {
  console.log(predicateConfig, "predicateConfig");
  // Correctly get column name from within predicateConfig.column.entry
  const columnName = getEntryValue(predicateConfig.config[0].entry, "selected");

  // *** THIS LINE WAS CORRECTED in the previous update ***
  // Get operator from within predicateConfig.entry (the array of entries)
  const operator = getEntryValue(predicateConfig.entry, "operator");

  console.log(columnName, operator, "kk");

  const quotedColName = `"${columnName.replace(/"/g, '""')}"`;

  // Translate based on the operator
  switch (operator) {
    case "IS_NOT_MISSING":
      return `${quotedColName} IS NOT NULL`;
    case "IS_MISSING":
      return `${quotedColName} IS NULL`;
    // Add more cases here for other operators (EQUALS, GREATER, IN, LIKE, etc.)
    // Remember to parse values from predicateConfig.predicateValues.values when needed
    default:
      console.warn(`Unsupported Row Filter operator: ${operator}`);
      return null; // Return null for unsupported operators
  }
};

/**
 * Converts a KNIME Row Filter (RowFilterNodeFactory) node configuration to an SQL query.
 *
 * @param {object} nodeConfigJson - The node configuration JSON object from settings.xml.
 * @param {string | number} currentNodeId - The unique ID of the Row Filter node being processed.
 * @param {Array<object>} allProcessedNodes - Array of all previously processed nodes. Each object
 * MUST have 'id', 'nodeName' (used as sqlAlias), 'nodes' (schema), and 'nextNodes' (assumed array of strings).
 * @returns {string} - The generated SQL query (SELECT ... FROM ... WHERE ...).
 */
export function convertRowFilterNodeToSQL(
  nodeConfigJson,
  currentNodeId,
  allProcessedNodes
) {
  if (currentNodeId === undefined || currentNodeId === null) {
    console.error(
      "convertRowFilterNodeToSQL: currentNodeId parameter is missing.",
      nodeConfigJson
    );
    return "Error: Node ID was not provided to the conversion function.";
  }

  const factory = getEntryValue(nodeConfigJson?.entry, "factory");
  const ROWFILTER_FACTORY =
    "org.knime.base.node.preproc.filter.row3.RowFilterNodeFactory";
  if (factory !== ROWFILTER_FACTORY) {
    return `Error: Expected Row Filter node factory (${ROWFILTER_FACTORY}), but got ${
      factory || "N/A"
    }.`;
  }

  const modelNode = findConfigByKey(nodeConfigJson.config, "model");
  if (!modelNode) {
    return `Error: Node ${currentNodeId} (RowFilter): Model configuration not found.`;
  }

  // --- Get Predecessor Details ---
  const predecessor = findSinglePredecessorDetails(
    currentNodeId,
    allProcessedNodes
  );
  if (!predecessor) {
    return `Error: Node ${currentNodeId} (RowFilter): Could not find valid predecessor details. Check console warnings and workflow parsing / 'allProcessedNodes' structure (expecting 'nodeName' as sqlAlias, 'nodes' as schema, and string IDs in 'nextNodes').`;
  }

  // --- Parse Filter Logic ---
  const matchCriteria = getEntryValue(modelNode.entry, "matchCriteria"); // AND or OR
  const outputMode = getEntryValue(modelNode.entry, "outputMode"); // MATCHING or NON_MATCHING
  const predicatesNode = findConfigByKey(modelNode.config, "predicates");
  const sqlConditions = [];

  if (predicatesNode && predicatesNode.config) {
    const predicateConfigs = Array.isArray(predicatesNode.config)
      ? predicatesNode.config
      : [predicatesNode.config];
    console.log(predicateConfigs, "predicateConfigs");
    predicateConfigs.forEach((conf) => {
      // Call the corrected translatePredicateToSQL
      const sqlCondition = translatePredicateToSQL(conf);
      if (sqlCondition) {
        sqlConditions.push(`(${sqlCondition})`);
      } else {
        // Log warning and use fallback ONLY if translation failed for a specific reason
        console.warn(
          `Node ${currentNodeId} (RowFilter): Failed to translate predicate (check details above):`,
          conf
        );
        // Consider if '1=1' is the right fallback or if an error should be thrown
        sqlConditions.push("1 = 1"); // Fallback to TRUE (no filter for this condition)
      }
    });
  }

  if (sqlConditions.length === 0) {
    return `SELECT * FROM "${predecessor.sqlAlias.replace(
      /"/g,
      '""'
    )}"; -- Node ID: ${currentNodeId} (RowFilter - No valid conditions found)`;
  }

  const combinedConditions = sqlConditions.join(` ${matchCriteria} `);
  let whereClause = "";
  if (outputMode === "MATCHING") {
    whereClause = combinedConditions;
  } else if (outputMode === "NON_MATCHING") {
    whereClause = `NOT (${combinedConditions})`;
  } else {
    return `Error: Node ${currentNodeId} (RowFilter): Unsupported outputMode: ${outputMode}`;
  }

  // --- Assemble Final Query ---
  const quotedPredecessorName = `"${predecessor.sqlAlias.replace(/"/g, '""')}"`;
  const sqlQuery = `
SELECT *
FROM ${quotedPredecessorName}
WHERE ${whereClause};
`;

  return sqlQuery.trim();
}
