// src/functions/convertColumnRenamerNodeToSQL.js

// Assuming these helper functions are available in your project's common directory
// If not, you'll need to ensure they are correctly imported or defined.
import { getEntryValue } from "../common/getEntryValue";
import { findConfigByKey } from "../common/findConfigByKey";

/**
 * Finds the direct predecessor node(s) for a given node ID from a list of processed nodes.
 * (This is a common helper function, also used by other converters)
 * @param {string | number} currentNodeId - The ID of the current node.
 * @param {Array<object>} allProcessedNodes - An array of node objects that have already been processed,
 * each expected to have an 'id', 'nodes' (output columns), and 'nextNodes' (successor IDs) property.
 * @returns {Array<object>} - An array of predecessor node objects.
 */
const findPredecessorNodes = (currentNodeId, allProcessedNodes) => {
  if (!Array.isArray(allProcessedNodes)) return [];
  const currentIdStr = String(currentNodeId);
  return allProcessedNodes.filter(
    (node) =>
      node &&
      Array.isArray(node.nextNodes) &&
      node.nextNodes.some((nextNodeId) => String(nextNodeId) === currentIdStr)
  );
};

/**
 * Converts a KNIME Column Renamer node configuration (JSON from settings.xml) to an SQL query.
 *
 * @param {object} nodeConfigJson - The node configuration JSON object from settings.xml.
 * It MUST have an 'id' property assigned during workflow parsing.
 * @param {string} [previousNodeName="input_table"] - The name/alias of the table/view from the predecessor node.
 * @param {Array<object>} [allProcessedNodes=[]] - Array of all previously processed nodes in the workflow.
 * Each object should have 'id' (node ID), 'nodes' (array of its output column names),
 * and 'nextNodes' (array of IDs of its direct successor nodes).
 * @returns {string} - The generated SQL query or an error message.
 */
export function convertColumnRenamerNodeToSQL(
  nodeConfigJson,
  nodeId,
  previousNodeName = "input_table",
  allProcessedNodes = []
) {
  // Verify node factory
  const factory = getEntryValue(nodeConfigJson?.entry, "factory");
  const RENAMER_FACTORY =
    "org.knime.base.node.preproc.column.renamer.ColumnRenamerNodeFactory";
  if (factory !== RENAMER_FACTORY) {
    return `Error: Expected Column Renamer node factory (${RENAMER_FACTORY}), but got ${
      factory || "N/A"
    }.`;
  }

  const modelNode = findConfigByKey(nodeConfigJson.config, "model");
  if (!modelNode) {
    return "Error: Model configuration not found in Column Renamer node.";
  }

  const currentNodeId = nodeId;

  // --- Determine Input Columns ---
  const predecessors = findPredecessorNodes(currentNodeId, allProcessedNodes);
  let inputColumnNames = [];

  if (predecessors.length === 1) {
    if (
      Array.isArray(predecessors[0].nodes) &&
      predecessors[0].nodes.length > 0
    ) {
      inputColumnNames = predecessors[0].nodes;
    } else {
      console.warn(
        `Node ${currentNodeId} (ColumnRenamer): Predecessor node ${predecessors[0].id} exists but did not provide output column names (its 'nodes' property was empty or not an array).`
      );
      // Continue, but inputColumnNames will be empty.
    }
  } else if (predecessors.length > 1) {
    console.warn(
      `Node ${currentNodeId} (ColumnRenamer): Multiple direct predecessors found. Using columns from the first one found: ${predecessors[0].id}. This might be incorrect for complex workflows.`
    );
    if (
      Array.isArray(predecessors[0].nodes) &&
      predecessors[0].nodes.length > 0
    ) {
      inputColumnNames = predecessors[0].nodes;
    } // else: inputColumnNames remains empty
  } else {
    // predecessors.length === 0
    console.warn(
      `Node ${currentNodeId} (ColumnRenamer): No predecessors found. Input columns are unknown.`
    );
    // inputColumnNames remains empty.
  }

  if (inputColumnNames.length === 0) {
    return `Error: Node ${currentNodeId} (ColumnRenamer): Input columns from predecessor are unknown. Cannot reliably rename columns. Ensure predecessor node (${
      predecessors.length > 0 ? predecessors[0].id : "N/A"
    }) provides its output column schema.`;
  }

  // --- Parse Renaming Rules ---
  const renamingMap = new Map();
  const renamingsNode = findConfigByKey(modelNode.config, "renamings");

  if (renamingsNode && renamingsNode.config) {
    const renamingConfigs = Array.isArray(renamingsNode.config)
      ? renamingsNode.config
      : [renamingsNode.config];

    renamingConfigs.forEach((conf) => {
      if (conf && conf.entry) {
        const oldName = getEntryValue(conf.entry, "oldName");
        const newName = getEntryValue(conf.entry, "newName");
        if (oldName && newName) {
          renamingMap.set(oldName, newName);
        } else {
          console.warn(
            `Node ${currentNodeId} (ColumnRenamer): Invalid renaming rule found (missing oldName or newName):`,
            conf
          );
        }
      }
    });
  }

  // If no renaming rules are defined, but input columns are known, select all input columns as they are.
  if (renamingMap.size === 0) {
    const quotedInputCols = inputColumnNames
      .map((name) => `"${name.replace(/"/g, '""')}"`)
      .join(",\n  ");
    return `SELECT\n  ${quotedInputCols}\nFROM "${previousNodeName.replace(
      /"/g,
      '""'
    )}"; -- Node ID: ${currentNodeId} (ColumnRenamer - No renaming rules defined or parsed)`;
  }

  // --- Construct SELECT Clause ---
  const selectParts = inputColumnNames.map((originalColName) => {
    const newColName = renamingMap.get(originalColName);
    const quotedOriginalColName = `"${originalColName.replace(/"/g, '""')}"`;
    if (newColName) {
      // If the new name is the same as old, just select it. Otherwise, use AS.
      if (newColName === originalColName) {
        return quotedOriginalColName;
      }
      return `${quotedOriginalColName} AS "${newColName.replace(/"/g, '""')}"`;
    }
    return quotedOriginalColName; // Select as is if not in renaming map
  });

  if (selectParts.length === 0) {
    // This should not happen if inputColumnNames.length > 0
    return `Error: Node ${currentNodeId} (ColumnRenamer): Could not construct SELECT clause. Input columns might be empty or an unexpected issue occurred.`;
  }

  const quotedPreviousNodeName = `"${previousNodeName.replace(/"/g, '""')}"`;
  const sqlQuery = `
SELECT
  ${selectParts.join(",\n  ")}
FROM ${quotedPreviousNodeName};
`;

  return sqlQuery.trim();
}
