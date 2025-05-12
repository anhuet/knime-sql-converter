// src/functions/convertRuleEngineNodeToSQL.js

// Assume these helper functions are correctly imported or defined elsewhere
// import { findConfigByKey } from "../common/findConfigByKey";
// import { getArrayValuesFromConfig } from "../common/getArrayValuesFromConfig";
// import { getEntryValue } from "../common/getEntryValue";

// --- Mock/Example Helper Functions (Replace with your actual imports) ---
// Mock implementation for demonstration if imports are not set up
const findConfigByKey = (config, key) => {
  if (!config) return null;
  if (Array.isArray(config)) {
    return config.find(
      (node) => node._attributes && node._attributes.key === key
    );
  } else if (config._attributes && config._attributes.key === key) {
    return config;
  }
  // Look in nested config if top-level isn't an array and doesn't match
  if (config.config) {
    return findConfigByKey(config.config, key);
  }
  return null;
};

const getArrayValuesFromConfig = (entryArray, keyPrefix = "") => {
  if (!entryArray) return [];
  const entries = Array.isArray(entryArray) ? entryArray : [entryArray];
  const values = [];
  entries.forEach((entry) => {
    if (
      entry &&
      entry._attributes &&
      entry._attributes.key !== "array-size" &&
      entry._attributes.key.startsWith(keyPrefix)
    ) {
      values.push(entry._attributes.value);
    }
  });
  return values;
};

const getEntryValue = (entryArray, key) => {
  if (!entryArray) return "";
  const entries = Array.isArray(entryArray) ? entryArray : [entryArray];
  const entry = entries.find(
    (e) => e && e._attributes && e._attributes.key === key
  );
  return entry?._attributes?.value || "";
};
// --- End Mock Helper Functions ---

/**
 * Finds the direct predecessor node(s) for a given node ID from a list of processed nodes.
 * @param {number} currentNodeId - The ID of the current node.
 * @param {Array<object>} allProcessedNodes - An array of node objects that have already been processed.
 * @returns {Array<object>} - An array of predecessor node objects.
 */
const findPredecessorNodes = (currentNodeId, allProcessedNodes) => {
  // Ensure allProcessedNodes is an array before filtering
  if (!Array.isArray(allProcessedNodes)) return [];
  // Filter nodes that list currentNodeId in their nextNodes array
  return allProcessedNodes.filter(
    (node) =>
      node && // Check if node exists
      Array.isArray(node.nextNodes) && // Check if nextNodes is an array
      node.nextNodes.includes(currentNodeId) // Check if currentNodeId is included
  );
};

/**
 * Parses a KNIME expression snippet used in Rule Engine conditions or results.
 * Handles: $ColumnName$, "String Literal", NumericLiteral, TRUE
 * Limited support for operators, currently handles '=' comparison.
 * @param {string} expression - The KNIME expression snippet.
 * @returns {string} - The SQL equivalent snippet or an error string.
 */
function parseKnimeRuleExpressionToSQL(expression) {
  const trimmedExpr = expression.trim();

  // Handle TRUE keyword (case-insensitive)
  if (trimmedExpr.toUpperCase() === "TRUE") {
    return "TRUE"; // Special value handled later for ELSE clause
  }

  // Handle simple equality comparison: $Column$ = "Value" or $Col$ = 123
  const equalityRegex = /^\$(.+?)\$\s*=\s*(.*)$/; // Regex to capture column name and value part
  const equalityMatch = trimmedExpr.match(equalityRegex);
  if (equalityMatch) {
    const columnName = equalityMatch[1].trim().replace(/""/g, '"'); // Extract column name, handle escaped quotes
    const valuePart = equalityMatch[2].trim(); // Extract the value part
    // Parse the value part recursively to handle literals or other columns
    const sqlValue = parseKnimeRuleExpressionToSQL(valuePart);
    // If parsing the value part resulted in an error, propagate it
    if (sqlValue.startsWith("Error:")) return sqlValue;
    // Construct the SQL equality expression, quoting the column name
    return `"${columnName.replace(/"/g, '""')}" = ${sqlValue}`;
  }

  // Handle column reference: $ColumnName$
  const columnRefRegex = /^\$(.+?)\$$/; // Regex to capture column name within $...$
  const columnRefMatch = trimmedExpr.match(columnRefRegex);
  if (columnRefMatch) {
    const columnName = columnRefMatch[1].trim().replace(/""/g, '"'); // Extract column name
    // Return the SQL-quoted column name
    return `"${columnName.replace(/"/g, '""')}"`;
  }

  // Handle string literal: "Value"
  const stringLiteralRegex = /^"((?:\\"|[^"])*)"$/; // Regex to capture content within "...", handling escaped \"
  const stringLiteralMatch = trimmedExpr.match(stringLiteralRegex);
  if (stringLiteralMatch) {
    const literalValue = stringLiteralMatch[1].replace(/\\"/g, '"'); // Unescape KNIME's \" to "
    // Return SQL single-quoted string, escaping internal single quotes ' -> ''
    return `'${literalValue.replace(/'/g, "''")}'`;
  }

  // Handle numeric literal (integer or decimal)
  const numericLiteralRegex = /^[+-]?\d+(\.\d+)?$/;
  if (numericLiteralRegex.test(trimmedExpr)) {
    return trimmedExpr; // Return numeric literals as they are
  }

  // Placeholder comment: Add parsing for more operators (LIKE, IN, >, <, AND, OR) here if needed

  // If none of the above patterns match, return an error
  return `Error: Unsupported expression syntax: "${trimmedExpr}"`;
}

/**
 * Parses a single KNIME rule string (CONDITION => RESULT) into SQL parts.
 * @param {string} ruleString - The rule string, e.g., "$State$ = \"CA\" => \"California\"".
 * @returns {object|null} - Object with { conditionSQL, resultSQL, isDefault } or null if parsing fails.
 */
function parseKnimeRuleToSQLParts(ruleString) {
  // Split the rule string by the '=>' delimiter
  const parts = ruleString.split("=>");
  // A valid rule must have exactly two parts (condition and result)
  if (parts.length !== 2) {
    console.error(
      `Invalid rule format (missing or too many '=>'): "${ruleString}"`
    );
    return null; // Indicate failure
  }

  // Trim whitespace from condition and result strings
  const conditionStr = parts[0].trim();
  const resultStr = parts[1].trim();

  // Parse the condition and result strings into SQL snippets
  const conditionSQL = parseKnimeRuleExpressionToSQL(conditionStr);
  const resultSQL = parseKnimeRuleExpressionToSQL(resultStr);

  // Check if either parsing step resulted in an error
  if (conditionSQL.startsWith("Error:") || resultSQL.startsWith("Error:")) {
    console.error(
      `Error parsing rule "${ruleString}": Condition Error: ${conditionSQL}, Result Error: ${resultSQL}`
    );
    return null; // Indicate failure
  }

  // Determine if this is the default rule (condition is TRUE)
  const isDefault = conditionSQL === "TRUE";

  // Return the parsed SQL parts and default status
  return { conditionSQL, resultSQL, isDefault };
}

/**
 * Converts a KNIME Rule Engine node configuration (compact JSON from settings.xml) to an SQL query.
 * Handles both appending a new column and replacing an existing column based on node settings.
 *
 * @param {object} nodeSettingsJson - The JSON configuration object for the Rule Engine node.
 * @param {number} currentNodeId - The unique ID of the current Rule Engine node in the workflow.
 * @param {string} previousNodeName - The name of the table/view providing input to this node.
 * @param {Array<object>} allProcessedNodes - Array of all previously processed node objects in the workflow,
 * used to determine input columns for the current node.
 * @returns {string} - The generated SQL query string, or an error message string starting with '-- Error:'.
 */
export function convertRuleEngineNodeToSQL(
  nodeSettingsJson,
  currentNodeId,
  previousNodeName = "input_table", // Default input table name if not specified
  allProcessedNodes // Context of previously processed nodes is required
) {
  try {
    // 1. Verify Node Type: Ensure the factory attribute matches the Rule Engine node factory.
    const factory = getEntryValue(nodeSettingsJson.entry, "factory");
    const RULE_ENGINE_FACTORY =
      "org.knime.base.node.rules.engine.RuleEngineNodeFactory";
    if (factory !== RULE_ENGINE_FACTORY) {
      // Return an error comment and default query if the node type is incorrect
      return `-- Error: Expected Rule Engine node factory, but got ${
        factory || "N/A"
      }.\nSELECT * FROM "${previousNodeName.replace(/"/g, '""')}";`;
    }

    // 2. Locate Model Configuration and Extract Settings: Find the 'model' config and get rules, output mode, etc.
    const modelNode = findConfigByKey(nodeSettingsJson.config, "model");
    // Check if modelNode exists and contains necessary sub-configurations or entries
    if (!modelNode || (!modelNode.config && !modelNode.entry)) {
      return `-- Error: Model configuration not found or invalid in Rule Engine node.\nSELECT * FROM "${previousNodeName.replace(
        /"/g,
        '""'
      )}";`;
    }

    const rulesConfig = findConfigByKey(modelNode.config, "rules");
    // Extract the array of rule strings from the configuration
    const ruleStrings =
      rulesConfig && rulesConfig.entry
        ? getArrayValuesFromConfig(rulesConfig.entry, "")
        : [];

    // If no rules are defined, there's nothing to convert, return SELECT *
    if (ruleStrings.length === 0) {
      return `-- Warning: No rules found in Rule Engine node ${currentNodeId}. Returning SELECT *.\nSELECT * FROM "${previousNodeName.replace(
        /"/g,
        '""'
      )}";`;
    }

    // Determine operation mode: append new column or replace existing one
    const appendColumn =
      getEntryValue(modelNode.entry, "append-column") === "true";
    const newColumnName = getEntryValue(modelNode.entry, "new-column-name"); // Name for the new column if appending
    const replaceColumnName = getEntryValue(
      modelNode.entry,
      "replace-column-name"
    ); // Name of the column to replace if not appending

    let outputColumnAlias = ""; // This will be the SQL alias for the generated CASE statement
    if (appendColumn) {
      // If appending, check if the new column name is provided
      if (!newColumnName)
        return "-- Error: Rule Engine is set to append, but 'new-column-name' is missing.";
      // Set the alias, ensuring it's quoted for SQL
      outputColumnAlias = `"${newColumnName.replace(/"/g, '""')}"`;
    } else {
      // **** THIS LOGIC HANDLES THE REPLACE SCENARIO ****
      // If replacing, check if the column name to replace is provided
      if (!replaceColumnName)
        return "-- Error: Rule Engine is set to replace, but 'replace-column-name' is missing.";
      // Set the alias to the name of the column being replaced, ensuring it's quoted
      outputColumnAlias = `"${replaceColumnName.replace(/"/g, '""')}"`;
    }

    // 3. Determine Input Columns: Find the predecessor node(s) to get the list of input columns.
    // This is crucial for constructing the SELECT list correctly, especially when replacing a column.
    const predecessors = findPredecessorNodes(currentNodeId, allProcessedNodes);
    let inputColumnNames = []; // Initialize empty array for input column names
    if (predecessors.length === 0) {
      // Log a warning if no predecessors are found (input schema unknown)
      console.warn(
        `Node ${currentNodeId} (Rule Engine): No predecessors found. Input column list unknown. SELECT * will be used if appending, but replacing might be inaccurate.`
      );
    } else if (predecessors.length > 1) {
      // Log a warning if multiple predecessors are found (using the first one)
      console.warn(
        `Node ${currentNodeId} (Rule Engine): Multiple predecessors found. Using columns from the first one: ${predecessors[0].id}.`
      );
      inputColumnNames = predecessors[0].nodes || []; // Get columns from the first predecessor
    } else {
      // Get columns from the single predecessor
      inputColumnNames = predecessors[0].nodes || [];
    }

    // 4. Parse Rules and Build SQL CASE Statement: Convert each KNIME rule into a part of the CASE statement.
    let caseClauses = []; // Array to store "WHEN condition THEN result" parts
    let elseClause = null; // Variable to store the "ELSE result" part
    let parseErrors = []; // Array to collect any errors encountered during rule parsing

    ruleStrings.forEach((ruleStr) => {
      const parsed = parseKnimeRuleToSQLParts(ruleStr); // Attempt to parse the rule
      if (parsed) {
        // If parsing succeeded
        if (parsed.isDefault) {
          // Check if it's the default rule (condition is TRUE)
          // Handle multiple default rules (use the last one found)
          if (elseClause)
            console.warn(
              `Rule Engine Node ${currentNodeId}: Multiple default (TRUE => ...) rules found. Using the last one.`
            );
          // Set the ELSE clause using the parsed result SQL
          elseClause = `ELSE ${parsed.resultSQL}`;
        } else {
          // Add the standard "WHEN condition THEN result" clause
          caseClauses.push(
            `WHEN ${parsed.conditionSQL} THEN ${parsed.resultSQL}`
          );
        }
      } else {
        // If parsing failed, add an error message to the collection
        parseErrors.push(`Failed to parse rule: "${ruleStr}"`);
      }
    });

    // If any errors occurred during parsing, return an error message immediately
    if (parseErrors.length > 0) {
      return `-- Error parsing rules in Rule Engine Node ${currentNodeId}:\n-- ${parseErrors.join(
        "\n-- "
      )}\nSELECT * FROM "${previousNodeName.replace(/"/g, '""')}";`;
    }

    // Ensure an ELSE clause exists (required by standard SQL CASE statements)
    if (!elseClause) {
      // If no 'TRUE => ...' rule was found in KNIME, default the SQL ELSE to NULL
      elseClause = "ELSE NULL";
      console.warn(
        `Rule Engine Node ${currentNodeId}: No default rule (TRUE => ...) found. SQL CASE statement will default to NULL.`
      );
    }

    // Construct the complete CASE statement string with indentation
    const caseStatement = `CASE\n    ${caseClauses.join(
      "\n    "
    )}\n    ${elseClause}\n  END`;

    // 5. Build Final SELECT Statement: Construct the SELECT clause based on input columns and operation mode.
    const quotedPreviousNodeName = `"${previousNodeName.replace(/"/g, '""')}"`; // Quote the FROM table name
    let selectParts = []; // Array to hold the final list of selected columns/expressions

    if (inputColumnNames.length === 0) {
      // Fallback logic if input column names could not be determined
      selectParts.push("*"); // Default to selecting all columns
      if (appendColumn) {
        // If appending, add the CASE statement as a new column
        selectParts.push(`${caseStatement} AS ${outputColumnAlias}`);
      } else {
        // If replacing, we can't accurately replace within SELECT *, so log a warning.
        console.warn(
          `Node ${currentNodeId} (Rule Engine): Replacing column ${outputColumnAlias} but input columns unknown. Result includes '*' which has the original column; the CASE statement is not explicitly replacing it.`
        );
        // Optionally, add the CASE statement anyway, resulting in both the original column (via *) and the aliased CASE result.
        // selectParts.push(`${caseStatement} AS ${outputColumnAlias}`);
      }
    } else {
      // Build the SELECT list explicitly using the known input column names
      inputColumnNames.forEach((col) => {
        const quotedCol = `"${col.replace(/"/g, '""')}"`; // Quote the current input column name
        // **** THIS LOGIC HANDLES REPLACEMENT ****
        // Check if we are replacing AND if the current column is the one to be replaced
        if (!appendColumn && quotedCol === outputColumnAlias) {
          // If yes, use the CASE statement instead of the original column, aliased correctly
          selectParts.push(`${caseStatement} AS ${outputColumnAlias}`);
        } else {
          // Otherwise (either appending or not the column to replace), keep the original column
          selectParts.push(quotedCol);
        }
      });

      // Handle appending the new column if necessary
      if (appendColumn) {
        // Check if the new column name conflicts with an existing input column name
        const conflictingInputCol = inputColumnNames
          .map((c) => `"${c.replace(/"/g, '""')}"`)
          .includes(outputColumnAlias);
        if (conflictingInputCol) {
          // If conflict, log warning and effectively replace the existing column in the list
          console.warn(
            `Node ${currentNodeId} (Rule Engine): Appending column ${outputColumnAlias} which conflicts with an existing input column name. The CASE statement will effectively replace it in the output.`
          );
          selectParts = selectParts.map((part) =>
            part === outputColumnAlias
              ? `${caseStatement} AS ${outputColumnAlias}`
              : part
          );
        } else {
          // If no conflict, simply add the CASE statement as a new column
          selectParts.push(`${caseStatement} AS ${outputColumnAlias}`);
        }
      }
      // Handle case where the column intended for replacement wasn't found in the input
      else if (
        !inputColumnNames
          .map((c) => `"${c.replace(/"/g, '""')}"`)
          .includes(outputColumnAlias)
      ) {
        // This can happen if 'replace-column-name' refers to a column not present in the input stream
        // derived from the predecessor. Log a warning and add the CASE statement anyway.
        console.warn(
          `Node ${currentNodeId} (Rule Engine): Column to replace ${outputColumnAlias} not found in derived input columns. Adding CASE statement as ${outputColumnAlias}.`
        );
        selectParts.push(`${caseStatement} AS ${outputColumnAlias}`);
      }
    }

    // Construct the final SQL query string
    // Join the select parts with commas and newlines for readability
    const selectClause = `SELECT\n  ${selectParts.join(",\n  ")}`;
    // Combine SELECT clause and FROM clause
    const sqlQuery = `${selectClause}\nFROM ${quotedPreviousNodeName};`;
    // Return the final, trimmed SQL query
    return sqlQuery.trim();
  } catch (error) {
    // Catch any unexpected errors during the conversion process
    console.error("Error converting Rule Engine node to SQL:", error);
    // Return an error comment and a default SELECT * query
    return `-- Error processing Rule Engine node: ${
      error.message
    }\nSELECT * FROM "${previousNodeName.replace(/"/g, '""')}";`;
  }
}
