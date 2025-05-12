// src/functions/convertStringManipulationNodeToSQL.js

import { getEntryValue } from "../common/getEntryValue";
import { findConfigByKey } from "../common/findConfigByKey";

/**
 * Finds the direct predecessor node(s) for a given node ID from a list of processed nodes.
 * @param {number} currentNodeId - The ID of the current node.
 * @param {Array<object>} allProcessedNodes - An array of node objects that have already been processed.
 * @returns {Array<object>} - An array of predecessor node objects.
 */
const findPredecessorNodes = (currentNodeId, allProcessedNodes) => {
  if (!Array.isArray(allProcessedNodes)) return [];
  return allProcessedNodes.filter(
    (node) =>
      node &&
      Array.isArray(node.nextNodes) &&
      node.nextNodes.includes(currentNodeId)
  );
};

/**
 * Parses a KNIME regexReplace expression string.
 * @param {string} expression - The expression string.
 * @returns {object|null} - An object with { columnName, pattern, replacement } or null if parsing fails.
 */
function parseRegexReplace(expression) {
  if (!expression || !expression.startsWith("regexReplace(")) {
    return null;
  }
  const regex =
    /regexReplace\(\s*\$(.*?)\$\s*,\s*"((?:\\"|[^"])*)"\s*,\s*"((?:\\"|[^"])*)"\s*\)/;
  const match = expression.match(regex);

  if (match && match.length === 4) {
    return {
      columnName: match[1], // This is the column name used *inside* the expression, e.g., "Discounts" from $Discounts$
      pattern: match[2].replace(/\\"/g, '"'),
      replacement: match[3].replace(/\\"/g, '"'),
    };
  }
  return null;
}

/**
 * Converts a KNIME String Manipulation node to SQL based on user's specified logic
 * for 'append_column' and 'replaced_column' settings.
 *
 * @param {object} nodeSettingsJson - JSON from settings.xml.
 * @param {number} currentNodeId - ID of the current node.
 * @param {string} previousNodeName - Name of the input table/view.
 * @param {Array<object>} allProcessedNodes - Array of previously processed nodes.
 * @returns {string} - Generated SQL query or an error message.
 */
export function convertStringManipulationNodeToSQL(
  nodeSettingsJson,
  currentNodeId,
  previousNodeName = "input_table",
  allProcessedNodes
) {
  // 1. Verify node type
  const factory = getEntryValue(nodeSettingsJson?.entry, "factory");
  const STRING_MANIPULATION_FACTORY =
    "org.knime.base.node.preproc.stringmanipulation.StringManipulationNodeFactory";
  if (factory !== STRING_MANIPULATION_FACTORY) {
    return `Error: Expected String Manipulation node factory, but got ${
      factory || "N/A"
    }.`;
  }

  // 2. Extract parameters from KNIME XML settings
  const modelNode = findConfigByKey(nodeSettingsJson.config, "model");
  if (!modelNode || !modelNode.entry) {
    return "Error: Model configuration not found in String Manipulation node.";
  }

  const knimeExpressionString = getEntryValue(modelNode.entry, "expression");
  // knimeReplacedColumnSetting is the value of the "replaced_column" entry in the XML.
  // User's logic:
  // - If knimeAppendColumnXmlSetting is true, this should match the column in the expression.
  // - If knimeAppendColumnXmlSetting is false, this is the NAME of the NEW column to be added.
  const knimeReplacedColumnXmlSetting = getEntryValue(
    modelNode.entry,
    "replaced_column"
  );
  // knimeAppendColumnXmlSetting is the boolean value of "append_column" entry in XML.
  const knimeAppendColumnXmlSetting = getEntryValue(
    modelNode.entry,
    "append_column"
  );

  if (!knimeExpressionString || !knimeReplacedColumnXmlSetting) {
    return "Error: Missing 'expression' or 'replaced_column' in String Manipulation node configuration.";
  }

  // 3. Find Predecessor and derive Input Columns
  const predecessors = findPredecessorNodes(currentNodeId, allProcessedNodes);
  let inputColumnNames = [];
  if (predecessors.length === 0) {
    console.warn(
      `Node ${currentNodeId} (String Manip, UserLogic): No predecessors found. Input column list might be incomplete.`
    );
  } else if (predecessors.length > 1) {
    console.warn(
      `Node ${currentNodeId} (String Manip, UserLogic): Multiple predecessors. Using columns from first one: ${predecessors[0].id}.`
    );
    inputColumnNames = predecessors[0].nodes || [];
  } else {
    inputColumnNames = predecessors[0].nodes || [];
  }

  // 4. Parse the KNIME expression (currently only regexReplace)
  const parsedKnimeExpression = parseRegexReplace(knimeExpressionString);
  if (!parsedKnimeExpression) {
    return `Error: Unsupported or malformed expression: "${knimeExpressionString}". Only regexReplace is currently supported.`;
  }
  // parsedKnimeExpression.columnName is the column targeted by the expression, e.g., "Sales" from regexReplace($Sales$,...)

  // 5. Translate to SQL REGEXP_REPLACE
  const sqlPattern = parsedKnimeExpression.pattern.replace(/'/g, "''");
  const sqlReplacement = parsedKnimeExpression.replacement.replace(/'/g, "''");
  const sqlManipulationExpression = `REGEXP_REPLACE("${parsedKnimeExpression.columnName}", '${sqlPattern}', '${sqlReplacement}')`;

  // 6. Build SELECT clause based on User's interpretation of knimeAppendColumnXmlSetting
  const quotedPreviousNodeName = `"${previousNodeName.replace(/"/g, '""')}"`;
  let selectParts = [];
  let finalAliasForManipulation;

  if (knimeAppendColumnXmlSetting) {
    // USER LOGIC for append_column=true: "We just replace the Sales by the regex"
    // The column mentioned *inside* the expression (parsedKnimeExpression.columnName) is replaced.
    // The alias for the manipulated column will be parsedKnimeExpression.columnName itself.
    finalAliasForManipulation = `"${parsedKnimeExpression.columnName.replace(
      /"/g,
      '""'
    )}"`;

    if (parsedKnimeExpression.columnName !== knimeReplacedColumnXmlSetting) {
      console.warn(
        `Node ${currentNodeId} (String Manip, UserLogic, XML append_column=true): ` +
          `Column in expression ($${parsedKnimeExpression.columnName}$) ` +
          `differs from 'replaced_column' XML setting (${knimeReplacedColumnXmlSetting}). ` +
          `Proceeding to replace column from expression: "${parsedKnimeExpression.columnName}".`
      );
    }

    if (inputColumnNames.length === 0) {
      // If no input columns known, can only select the manipulated column.
      selectParts.push(
        `${sqlManipulationExpression} AS ${finalAliasForManipulation}`
      );
      console.warn(
        `Node ${currentNodeId} (String Manip, UserLogic, XML append_column=true): No input columns known. SQL will only select the manipulated column: ${finalAliasForManipulation}.`
      );
    } else {
      inputColumnNames.forEach((col) => {
        const quotedCol = `"${col.replace(/"/g, '""')}"`;
        if (col === parsedKnimeExpression.columnName) {
          selectParts.push(
            `${sqlManipulationExpression} AS ${finalAliasForManipulation}`
          );
        } else {
          selectParts.push(quotedCol);
        }
      });
      // Ensure the replaced column is added if it wasn't in inputColumnNames but was the target
      if (!inputColumnNames.includes(parsedKnimeExpression.columnName)) {
        const existingPart = selectParts.find((part) =>
          part.endsWith(`AS ${finalAliasForManipulation}`)
        );
        if (!existingPart) {
          // Only add if not already added (e.g. via inputColumnNames being empty)
          selectParts.push(
            `${sqlManipulationExpression} AS ${finalAliasForManipulation}`
          );
          console.warn(
            `Node ${currentNodeId} (String Manip, UserLogic, XML append_column=true): Target column "${parsedKnimeExpression.columnName}" not in derived input columns. Adding it explicitly.`
          );
        }
      }
    }
  } else {
    // USER LOGIC for append_column=false: "You can know we added column called [knimeReplacedColumnXmlSetting]"
    // A new column is added. Its name is taken from the knimeReplacedColumnXmlSetting.
    // All original columns are preserved.
    finalAliasForManipulation = `"${knimeReplacedColumnXmlSetting.replace(
      /"/g,
      '""'
    )}"`;

    if (inputColumnNames.length === 0) {
      // If no specific input columns known, select all from previous table and add the new one.
      selectParts.push("*");
    } else {
      inputColumnNames.forEach((col) => {
        selectParts.push(`"${col.replace(/"/g, '""')}"`);
      });
    }
    selectParts.push(
      `${sqlManipulationExpression} AS ${finalAliasForManipulation}`
    );
  }

  if (selectParts.length === 0) {
    // This case should ideally be handled by the logic above, but as a fallback:
    console.error(
      `Node ${currentNodeId} (String Manip, UserLogic): selectParts array is empty. This indicates an issue in logic. Defaulting to selecting only the manipulated expression.`
    );
    selectParts.push(
      `${sqlManipulationExpression} AS ${
        finalAliasForManipulation || '"manipulated_column"'.replace(/"/g, '""')
      }`
    );
  }

  const selectClause = `SELECT\n  ${selectParts.join(",\n  ")}`;

  // 7. Construct the final SQL query
  const sqlQuery = `
${selectClause}
FROM ${quotedPreviousNodeName};
`;
  return sqlQuery.trim();
}
