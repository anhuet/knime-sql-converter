// src/functions/convertJoinerNodeToSQL.js

// Assuming these helper functions are available in your project's common directory
import { getEntryValue } from "../common/getEntryValue";
import { findConfigByKey } from "../common/findConfigByKey";
import { getArrayValuesFromConfig } from "../common/getArrayValuesFromConfig"; // Assuming this exists

/**
 * Finds the direct predecessor node details for a Joiner node, correctly identifying
 * left (port 0) and right (port 1) inputs based on connection information.
 *
 * *** Requirement ***: The `allProcessedNodes` array MUST contain detailed connection
 * information. Specifically, for each node that connects to the Joiner, its `nextNodes`
 * array should contain objects like:
 * { targetNodeId: 'joiner_id', sourcePortIndex: S, targetPortIndex: T }
 * where T=0 indicates connection to the Joiner's left input, and T=1 to the right input.
 *
 * @param {string | number} currentNodeId - The ID of the Joiner node.
 * @param {Array<object>} allProcessedNodes - Array of processed nodes. Each should have
 * 'id', 'sqlAlias', 'nodes' (schema), and 'nextNodes' (array of connection objects as described above).
 * @returns {{left: object | null, right: object | null}} - Object containing details for left and right predecessors.
 */
const findJoinerPredecessorDetails = (currentNodeId, allProcessedNodes) => {
  if (!Array.isArray(allProcessedNodes)) return { left: null, right: null };
  const currentIdStr = String(currentNodeId);
  let leftInputDetail = null;
  let rightInputDetail = null;

  // Iterate through all processed nodes to find those that connect TO the current Joiner node
  allProcessedNodes.forEach((potentialPredecessor) => {
    if (
      !potentialPredecessor ||
      !Array.isArray(potentialPredecessor.nextNodes)
    ) {
      return; // Skip nodes without valid nextNodes array
    }

    // Find the specific connection from this potential predecessor to the current Joiner node
    const connectionToJoiner = potentialPredecessor.nextNodes.find(
      (conn) => conn && String(conn.targetNodeId) === currentIdStr
    );

    if (connectionToJoiner) {
      // Check if targetPortIndex is defined
      if (
        connectionToJoiner.targetPortIndex === undefined ||
        connectionToJoiner.targetPortIndex === null
      ) {
        console.error(
          `Joiner Node ${currentNodeId}: Connection from predecessor ${potentialPredecessor.id} is missing 'targetPortIndex'. Cannot determine Left/Right input.`
        );
        // Mark as invalid, potentially throwing error later if needed
        if (leftInputDetail !== "error") leftInputDetail = "error";
        return;
      }

      // Get the full details of this predecessor node
      const fullNodeDetail = allProcessedNodes.find(
        (n) => n.id === potentialPredecessor.id
      );
      if (
        !fullNodeDetail ||
        !fullNodeDetail.sqlAlias ||
        !Array.isArray(fullNodeDetail.nodes)
      ) {
        console.warn(
          `Joiner Predecessor Detail: Could not find full details (sqlAlias, nodes) for predecessor ID ${potentialPredecessor.id}.`
        );
        // Mark as invalid
        if (leftInputDetail !== "error") leftInputDetail = "error";
        return;
      }

      const predecessorDetail = {
        id: fullNodeDetail.id,
        sqlAlias: fullNodeDetail.sqlAlias, // This is the FROM alias (e.g., CTE name)
        columns: fullNodeDetail.nodes, // Schema
      };

      // Assign to left or right based on the target port index on the Joiner
      if (connectionToJoiner.targetPortIndex === 0) {
        // Port 0 is LEFT input
        if (leftInputDetail && leftInputDetail !== "error") {
          console.warn(
            `Joiner Node ${currentNodeId}: Multiple predecessors found connecting to LEFT input (port 0). Using the last one found: ${predecessorDetail.id}.`
          );
        }
        leftInputDetail = predecessorDetail;
      } else if (connectionToJoiner.targetPortIndex === 1) {
        // Port 1 is RIGHT input
        if (rightInputDetail && rightInputDetail !== "error") {
          console.warn(
            `Joiner Node ${currentNodeId}: Multiple predecessors found connecting to RIGHT input (port 1). Using the last one found: ${predecessorDetail.id}.`
          );
        }
        rightInputDetail = predecessorDetail;
      } else {
        console.warn(
          `Joiner Node ${currentNodeId}: Connection from predecessor ${predecessorDetail.id} has unexpected targetPortIndex: ${connectionToJoiner.targetPortIndex}. Ignoring.`
        );
      }
    }
  });

  // Check for errors during processing
  if (leftInputDetail === "error" || rightInputDetail === "error") {
    return { left: null, right: null }; // Indicate failure due to missing info
  }

  return { left: leftInputDetail, right: rightInputDetail };
};

/**
 * Converts a KNIME Joiner (Joiner3NodeFactory) node configuration to an SQL query.
 *
 * @param {object} nodeConfigJson - The node configuration JSON object from settings.xml.
 * MUST include an 'id' property for the current node.
 * @param {Array<object>} allProcessedNodes - Array of all previously processed nodes. Each object
 * MUST have 'id', 'sqlAlias', 'nodes' (schema), and 'nextNodes' (array of connection objects
 * including 'targetNodeId' and 'targetPortIndex').
 * @returns {string} - The generated SQL query (using JOIN).
 */
export function convertJoinerNodeToSQL(nodeConfigJson, allProcessedNodes) {
  const factory = getEntryValue(nodeConfigJson?.entry, "factory");
  const JOINER_FACTORY =
    "org.knime.base.node.preproc.joiner3.Joiner3NodeFactory";
  if (factory !== JOINER_FACTORY) {
    return `Error: Expected Joiner node factory (${JOINER_FACTORY}), but got ${
      factory || "N/A"
    }.`;
  }

  const currentNodeId = nodeConfigJson.id;
  if (currentNodeId === undefined) {
    console.error(
      "convertJoinerNodeToSQL: nodeConfigJson is missing the 'id' property.",
      nodeConfigJson
    );
    return "Error: Node ID is missing from nodeConfigJson. Cannot process Joiner node.";
  }

  const modelNode = findConfigByKey(nodeConfigJson.config, "model");
  if (!modelNode) {
    return `Error: Node ${currentNodeId} (Joiner): Model configuration not found.`;
  }

  // --- Get Predecessor Details (Left and Right Inputs) ---
  // This now relies on findJoinerPredecessorDetails using targetPortIndex
  const { left: leftInput, right: rightInput } = findJoinerPredecessorDetails(
    currentNodeId,
    allProcessedNodes
  );

  if (!leftInput || !rightInput) {
    return `Error: Node ${currentNodeId} (Joiner): Could not reliably determine both left (port 0) and right (port 1) predecessor inputs. Check workflow parsing provides 'targetPortIndex' in 'nextNodes' connections and full details ('sqlAlias', 'nodes') in 'allProcessedNodes'.`;
  }

  // --- Determine Join Type ---
  const includeMatches =
    getEntryValue(modelNode.entry, "includeMatchesInOutput") === "true";
  const includeLeftUnmatched =
    getEntryValue(modelNode.entry, "includeLeftUnmatchedInOutput") === "true";
  const includeRightUnmatched =
    getEntryValue(modelNode.entry, "includeRightUnmatchedInOutput") === "true";

  let joinType = "";
  // Determine Join Type Logic (remains the same)
  if (includeMatches && !includeLeftUnmatched && !includeRightUnmatched) {
    joinType = "INNER JOIN";
  } else if (includeMatches && includeLeftUnmatched && !includeRightUnmatched) {
    joinType = "LEFT JOIN";
  } else if (includeMatches && !includeLeftUnmatched && includeRightUnmatched) {
    joinType = "RIGHT JOIN";
  } else if (includeMatches && includeLeftUnmatched && includeRightUnmatched) {
    joinType = "FULL OUTER JOIN";
  } else if (
    !includeMatches &&
    includeLeftUnmatched &&
    !includeRightUnmatched
  ) {
    joinType = "LEFT JOIN"; // Left Anti Join needs WHERE R.key IS NULL
  } else if (
    !includeMatches &&
    !includeLeftUnmatched &&
    includeRightUnmatched
  ) {
    joinType = "RIGHT JOIN"; // Right Anti Join needs WHERE L.key IS NULL
  } else {
    return `Error: Node ${currentNodeId} (Joiner): Unsupported join type configuration (Matches:${includeMatches}, LeftUnmatched:${includeLeftUnmatched}, RightUnmatched:${includeRightUnmatched}).`;
  }

  // Handle Anti-Join WHERE clause (remains the same)
  let antiJoinCondition = "";
  let antiJoinKeyPlaceholder = "%KEY%"; // Placeholder to find the correct key column later
  if (!includeMatches && includeLeftUnmatched && !includeRightUnmatched) {
    // Left Anti
    antiJoinCondition = `WHERE R."${antiJoinKeyPlaceholder}" IS NULL`;
  }
  if (!includeMatches && !includeLeftUnmatched && includeRightUnmatched) {
    // Right Anti
    antiJoinCondition = `WHERE L."${antiJoinKeyPlaceholder}" IS NULL`;
  }

  // --- Parse Join Criteria ---
  const matchingCriteriaNode = findConfigByKey(
    modelNode.config,
    "matchingCriteria"
  );
  const joinConditions = [];
  let firstRightKeyColumn = null; // Needed for Left Anti-Join WHERE clause
  let firstLeftKeyColumn = null; // Needed for Right Anti-Join WHERE clause

  if (matchingCriteriaNode && matchingCriteriaNode.config) {
    const criteriaConfigs = Array.isArray(matchingCriteriaNode.config)
      ? matchingCriteriaNode.config
      : [matchingCriteriaNode.config];

    criteriaConfigs.forEach((conf, index) => {
      // Added index
      if (conf && conf.entry) {
        const leftCol = getEntryValue(conf.entry, "leftTableColumn");
        const rightCol = getEntryValue(conf.entry, "rightTableColumn");
        if (leftCol && rightCol) {
          const condition = `L."${leftCol.replace(
            /"/g,
            '""'
          )}" = R."${rightCol.replace(/"/g, '""')}"`;
          joinConditions.push(condition);
          // Store the first key columns found for potential anti-join WHERE clause
          if (index === 0) {
            firstLeftKeyColumn = leftCol;
            firstRightKeyColumn = rightCol;
          }
        }
      }
    });
  }

  if (joinConditions.length === 0) {
    return `Error: Node ${currentNodeId} (Joiner): No valid join criteria found.`;
  }
  const onClause = joinConditions.join(" AND ");

  // Fill in the correct key column in the anti-join WHERE clause, if needed
  if (antiJoinCondition) {
    if (antiJoinCondition.includes('R."%KEY%"') && firstRightKeyColumn) {
      // Left Anti uses Right Key
      antiJoinCondition = antiJoinCondition.replace(
        antiJoinKeyPlaceholder,
        firstRightKeyColumn.replace(/"/g, '""')
      );
    } else if (antiJoinCondition.includes('L."%KEY%"') && firstLeftKeyColumn) {
      // Right Anti uses Left Key
      antiJoinCondition = antiJoinCondition.replace(
        antiJoinKeyPlaceholder,
        firstLeftKeyColumn.replace(/"/g, '""')
      );
    } else {
      console.warn(
        `Node ${currentNodeId} (Joiner): Could not determine key column for Anti-Join WHERE clause.`
      );
      antiJoinCondition = ""; // Clear the condition if key is missing
    }
  }

  // --- Parse Column Selection ---
  // Logic for getSelectedColumns and constructing finalSelectParts remains the same...
  const leftSelectionNode = findConfigByKey(
    modelNode.config,
    "leftColumnSelectionConfig"
  );
  const rightSelectionNode = findConfigByKey(
    modelNode.config,
    "rightColumnSelectionConfig"
  );
  const duplicateSuffix =
    getEntryValue(modelNode.entry, "suffix") || " (Right)";
  const mergeJoinCols =
    getEntryValue(modelNode.entry, "mergeJoinColumns") === "true";

  const getSelectedColumns = (selectionNode, availableColumns) => {
    if (!selectionNode) return [];
    const filterType = getEntryValue(selectionNode.entry, "filter-type");
    if (filterType !== "STANDARD") {
      console.warn(
        `Node ${currentNodeId} (Joiner): Only STANDARD column selection filter-type is handled. Found: ${filterType}. Selecting all available.`
      );
      return availableColumns;
    }
    const includedNamesConfig = findConfigByKey(
      selectionNode.config,
      "included_names"
    );
    const excludedNamesConfig = findConfigByKey(
      selectionNode.config,
      "excluded_names"
    );
    const enforceOption = getEntryValue(selectionNode.entry, "enforce_option");

    const included = getArrayValuesFromConfig(includedNamesConfig);
    const excluded = getArrayValuesFromConfig(excludedNamesConfig);

    if (enforceOption === "EnforceInclusion") {
      return included.filter((col) => availableColumns.includes(col));
    } else if (enforceOption === "EnforceExclusion") {
      if (included.length > 0) {
        return included.filter((col) => availableColumns.includes(col));
      } else {
        return availableColumns.filter((col) => !excluded.includes(col));
      }
    }
    return availableColumns;
  };

  const selectedLeftCols = getSelectedColumns(
    leftSelectionNode,
    leftInput.columns
  );
  const selectedRightCols = getSelectedColumns(
    rightSelectionNode,
    rightInput.columns
  );

  const finalSelectParts = [];
  const selectedColumnNames = new Set();

  selectedLeftCols.forEach((col) => {
    const quotedCol = `"${col.replace(/"/g, '""')}"`;
    finalSelectParts.push(`L.${quotedCol}`);
    selectedColumnNames.add(col);
  });

  selectedRightCols.forEach((col) => {
    const isJoinColumn = joinConditions.some((cond) =>
      cond.includes(`R."${col.replace(/"/g, '""')}"`)
    );
    if (mergeJoinCols && isJoinColumn) return;

    let alias = col;
    if (selectedColumnNames.has(col)) {
      alias = `${col}${duplicateSuffix}`;
    }
    const quotedOriginalCol = `"${col.replace(/"/g, '""')}"`;
    const quotedAlias = `"${alias.replace(/"/g, '""')}"`;

    finalSelectParts.push(
      alias === col
        ? `R.${quotedOriginalCol}`
        : `R.${quotedOriginalCol} AS ${quotedAlias}`
    );
    selectedColumnNames.add(alias);
  });

  if (finalSelectParts.length === 0) {
    return `Error: Node ${currentNodeId} (Joiner): No columns selected for output. Check column selection configuration.`;
  }

  // --- Assemble Final Query ---
  const leftAlias = `"L"`;
  const rightAlias = `"R"`;
  const quotedLeftInputName = `"${leftInput.sqlAlias.replace(/"/g, '""')}"`;
  const quotedRightInputName = `"${rightInput.sqlAlias.replace(/"/g, '""')}"`;

  let sqlQuery = `
SELECT
  ${finalSelectParts.join(",\n  ")}
FROM ${quotedLeftInputName} AS ${leftAlias}
${joinType} ${quotedRightInputName} AS ${rightAlias}
  ON ${onClause}`;

  if (antiJoinCondition) {
    sqlQuery += `\n${antiJoinCondition}`;
  }

  sqlQuery += `\n-- Node ID: ${currentNodeId} (Joiner)`;

  return sqlQuery.trim();
}

// ==========================================================
// Helper Function: getArrayValuesFromConfig (Example Implementation)
// You should place this in your common utilities (e.g., src/common/)
// ==========================================================
/**
 * Extracts string values from a KNIME config array structure.
 * Assumes structure like:
 * <config key="array_name">
 * <entry key="array-size" type="xint" value="N"/>
 * <entry key="0" type="xstring" value="Value0"/>
 * <entry key="1" type="xstring" value="Value1"/>
 * ...
 * </config>
 *
 * @param {object} configNode - The config object for the array (e.g., the object for "included_names").
 * @returns {Array<string>} - An array of the string values.
 */
export function getArrayValuesFromConfig(configNode) {
  const values = [];
  if (!configNode || !configNode.entry) {
    return values; // Return empty if node or entries are missing
  }
  const entries = Array.isArray(configNode.entry)
    ? configNode.entry
    : [configNode.entry];
  const sizeEntry = entries.find((e) => e.key === "array-size");
  const size = sizeEntry ? parseInt(sizeEntry.value, 10) : 0;
  if (isNaN(size) || size <= 0) return values;

  for (let i = 0; i < size; i++) {
    const entryKey = String(i);
    const valueEntry = entries.find((e) => e.key === entryKey);
    if (valueEntry && valueEntry.type === "xstring") {
      values.push(valueEntry.value);
    } else {
      console.warn(
        `getArrayValuesFromConfig: Expected xstring entry for key "${entryKey}" not found or invalid type.`
      );
    }
  }
  return values;
}
// Make sure to import this helper in convertJoinerNodeToSQL if it's separate:
// import { getArrayValuesFromConfig } from '../common/getArrayValuesFromConfig';
// ==========================================================
