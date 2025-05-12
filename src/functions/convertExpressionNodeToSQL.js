// src/functions/convertExpressionNodeToSQL.js

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
 * Parses a single KNIME expression token.
 * For this version, it keeps KNIME functions and column references as literal strings,
 * but converts KNIME string literals to SQL string literals.
 * @param {string} token - A part of the KNIME expression script.
 * @returns {string} - The SQL snippet or an error/unsupported message.
 */
function parseKnimeExpressionTokenToSQL(token) {
  const trimmedToken = token.trim();

  // Regex for KNIME literal string "string literal" -> 'string literal' (SQL style)
  // Handles escaped quotes inside the literal: "a\"b" -> 'a''b'
  const knimeLiteralStringRegex = /^"((?:\\"|[^"])*)"$/;
  const literalMatch = trimmedToken.match(knimeLiteralStringRegex);
  if (literalMatch && typeof literalMatch[1] === "string") {
    const literalValue = literalMatch[1].replace(/\\"/g, '"'); // Unescape KNIME's \"
    return `'${literalValue.replace(/'/g, "''")}'`; // Escape for SQL
  }

  // Regex for numeric literal (integer or decimal)
  const numericLiteralRegex = /^[+-]?\d+(\.\d+)?$/;
  if (numericLiteralRegex.test(trimmedToken)) {
    return trimmedToken; // Return as is
  }

  // For KNIME functions like string($["Column Name"]) or direct column refs $["Column Name"],
  // treat them as literal parts of the expression to be included as-is.
  // This also covers other potential KNIME functions or constructs.
  // We need to be careful if these tokens themselves contain characters that need SQL escaping
  // if they are not standard SQL. However, per user request, we are keeping them as is.
  if (trimmedToken.startsWith("string($[") || trimmedToken.startsWith("$[")) {
    // No transformation, return the token as is.
    // If the KNIME column names inside $["..."] could contain SQL special characters
    // and are NOT intended to be treated as SQL identifiers, this is fine.
    // If they ARE intended to be SQL identifiers, they might need quoting, e.g.
    // $["My Col"] -> "My Col". But user wants to keep `string($["..."])` as is.
    return trimmedToken;
  }

  // If it's not a recognized KNIME string literal, numeric, or specific KNIME construct,
  // and it's part of a concatenation, it's likely a KNIME column reference or function.
  // Return it as is. This is a broad fallback.
  // A more sophisticated parser would identify these more explicitly.
  if (trimmedToken.length > 0) {
    return trimmedToken;
  }

  return `Error: Unsupported or empty expression token: "${trimmedToken}"`;
}

/**
 * Parses a KNIME expression script string involving concatenations (+)
 * and attempts to convert it to an SQL snippet.
 * Example: string($["Col1"]) + " " + $["Col2"]
 * Output: string($["Col1"]) || ' ' || $["Col2"]
 * @param {string} script - The KNIME expression script string.
 * @returns {string} - The SQL snippet or an error/unsupported message.
 */
function parseKnimeExpressionScriptToSQL(script) {
  if (typeof script !== "string") {
    return `Error: Script is not a string (got ${typeof script})`;
  }
  const trimmedScript = script.trim();

  const parts = [];
  let currentPart = "";
  let inKnimeStringLiteral = false; // To track if we are inside KNIME's "..." string literals

  for (let i = 0; i < trimmedScript.length; i++) {
    const char = trimmedScript[i];

    if (char === '"') {
      // Toggle for being inside a KNIME string literal "..."
      // Handles KNIME's escaped quote \" within its string literals
      if (i > 0 && trimmedScript[i - 1] === "\\" && inKnimeStringLiteral) {
        // This is an escaped quote inside a KNIME string, part of currentPart
        currentPart += char;
      } else {
        inKnimeStringLiteral = !inKnimeStringLiteral;
        currentPart += char;
      }
    } else if (char === "+" && !inKnimeStringLiteral) {
      // '+' is a concatenation operator if not inside a KNIME string literal
      if (currentPart.trim().length > 0) {
        parts.push(currentPart.trim());
      }
      parts.push("+"); // Add the operator itself as a part to be replaced later
      currentPart = "";
    } else {
      currentPart += char;
    }
  }
  if (currentPart.trim().length > 0) {
    parts.push(currentPart.trim());
  }

  if (parts.length === 0 && trimmedScript.length > 0) {
    // If no '+' was found, the whole script is a single token
    parts.push(trimmedScript);
  } else if (parts.length === 0 && trimmedScript.length === 0) {
    return `Error: Empty expression script: "${trimmedScript}"`;
  }

  const sqlParts = [];
  for (let i = 0; i < parts.length; i++) {
    const part = parts[i];
    if (part === "+") {
      // Replace KNIME's + with SQL's ||
      // Ensure that we don't add || if the previous part was already an operator or it's the beginning
      if (sqlParts.length > 0 && sqlParts[sqlParts.length - 1] !== "||") {
        sqlParts.push("||");
      }
    } else {
      const parsedToken = parseKnimeExpressionTokenToSQL(part);
      if (parsedToken.startsWith("Error:")) {
        return `Error: Could not parse part of expression script "${part}". ${parsedToken}`;
      }
      sqlParts.push(parsedToken);
    }
  }

  // Filter out any potential empty strings that might result from splitting, though trim should handle most.
  // Also, remove trailing "||" if any.
  const finalSqlParts = sqlParts.filter((p) => p.trim() !== "");
  if (
    finalSqlParts.length > 0 &&
    finalSqlParts[finalSqlParts.length - 1] === "||"
  ) {
    finalSqlParts.pop();
  }

  // Join SQL parts. If only one part, no join needed.
  // The logic for adding "||" should ensure correct spacing.
  // We join by space, as "||" is an operator that typically has spaces around it.
  return finalSqlParts.join(" ");
}

/**
 * Converts a KNIME Expression node configuration (compact JSON) to an SQL query.
 *
 * @param {object} nodeSettingsJson - The JSON configuration of the Expression node.
 * @param {number} currentNodeId - The ID of the current Expression node.
 * @param {string} previousNodeName - The name of the table/view representing the input data.
 * @param {Array<object>} allProcessedNodes - Array of all previously processed nodes.
 * @returns {string} - The generated SQL query or an error/unsupported message.
 */
export function convertExpressionNodeToSQL(
  nodeSettingsJson,
  currentNodeId,
  previousNodeName = "input_table",
  allProcessedNodes
) {
  // 1. Verify node type
  const factory = getEntryValue(nodeSettingsJson?.entry, "factory");
  const EXPRESSION_NODE_FACTORY =
    "org.knime.base.expressions.node.row.mapper.ExpressionRowMapperNodeFactory";

  if (factory !== EXPRESSION_NODE_FACTORY) {
    return `Error: Expected Expression node factory, but got ${
      factory || "N/A"
    }.`;
  }

  // 2. Locate the model node
  const modelNode = findConfigByKey(nodeSettingsJson.config, "model");
  if (!modelNode || (!modelNode.entry && !modelNode.config)) {
    // model can have entries or configs for additional expressions
    return "Error: Model configuration not found or invalid in Expression node.";
  }

  // 3. Find Predecessor and derive Input Columns
  const predecessors = findPredecessorNodes(currentNodeId, allProcessedNodes);
  let inputColumnNames = [];
  if (predecessors.length === 0) {
    console.warn(
      `Node ${currentNodeId} (Expression): No predecessors found. Assuming SELECT * for input columns if needed, but this might be incomplete.`
    );
  } else if (predecessors.length > 1) {
    console.warn(
      `Node ${currentNodeId} (Expression): Found multiple predecessors. Using columns from the first one found (${predecessors[0].id}).`
    );
    inputColumnNames = predecessors[0].nodes || [];
  } else {
    inputColumnNames = predecessors[0].nodes || [];
  }

  // 4. Collect all expressions (main and additional)
  const expressionsToProcess = [];
  // Main expression might be directly under modelNode.entry
  const mainScript = getEntryValue(modelNode.entry, "script");
  const mainOutputMode = getEntryValue(modelNode.entry, "columnOutputMode"); // APPEND or REPLACE
  const mainCreatedColumn = getEntryValue(modelNode.entry, "createdColumn");
  const mainReplacedColumn = getEntryValue(modelNode.entry, "replacedColumn");

  if (mainScript) {
    expressionsToProcess.push({
      script: mainScript,
      outputMode: mainOutputMode,
      createdColumn: mainCreatedColumn,
      replacedColumn: mainReplacedColumn,
      isMain: true,
      source: "main",
    });
  }

  const additionalExpressionsConfig = findConfigByKey(
    modelNode.config,
    "additionalExpressions"
  );
  if (
    additionalExpressionsConfig &&
    Array.isArray(additionalExpressionsConfig.config)
  ) {
    additionalExpressionsConfig.config.forEach((additionalExprConf, index) => {
      const script = getEntryValue(additionalExprConf.entry, "script");
      const outputMode = getEntryValue(
        additionalExprConf.entry,
        "columnOutputMode"
      );
      const createdColumn = getEntryValue(
        additionalExprConf.entry,
        "createdColumn"
      );
      const replacedColumn = getEntryValue(
        additionalExprConf.entry,
        "replacedColumn"
      );
      if (script) {
        expressionsToProcess.push({
          script,
          outputMode,
          createdColumn,
          replacedColumn,
          isMain: false,
          source: `additional[${index}]`,
        });
      }
    });
  }

  if (expressionsToProcess.length === 0) {
    // If no expressions, just select all from the previous node
    const quotedPrevName = `"${previousNodeName.replace(/"/g, '""')}"`;
    console.warn(
      `Node ${currentNodeId} (Expression): No expressions found. Returning SELECT * from ${quotedPrevName}.`
    );
    return `SELECT * FROM ${quotedPrevName};`;
  }

  // 5. Build the SELECT clause
  const quotedPreviousNodeName = `"${previousNodeName.replace(/"/g, '""')}"`;
  let errorMessages = [];

  // Initialize selectParts with input columns if known
  let currentSelectParts = [];
  if (inputColumnNames.length > 0) {
    currentSelectParts = inputColumnNames.map(
      (col) => `"${col.replace(/"/g, '""')}"`
    );
  } else {
    console.warn(
      `Node ${currentNodeId} (Expression): No input columns derived. SELECT list will be built solely from expressions. If expressions only REPLACE, this might lead to issues.`
    );
  }

  const expressionResults = []; // To store {sql, alias, outputMode, originalColumn (for replace)}

  expressionsToProcess.forEach((expr) => {
    const sqlSnippet = parseKnimeExpressionScriptToSQL(expr.script);
    if (sqlSnippet.startsWith("Error:")) {
      errorMessages.push(
        `Expression (${expr.source}): ${sqlSnippet} (Original script: ${expr.script})`
      );
      return; // Skip this expression
    }

    if (expr.outputMode === "APPEND") {
      if (!expr.createdColumn) {
        errorMessages.push(
          `Expression (${expr.source}): APPEND mode missing 'createdColumn' name.`
        );
        return;
      }
      const alias = `"${expr.createdColumn.replace(/"/g, '""')}"`;
      expressionResults.push({
        sql: sqlSnippet,
        alias: alias,
        outputMode: "APPEND",
      });
    } else if (expr.outputMode === "REPLACE") {
      if (!expr.replacedColumn) {
        errorMessages.push(
          `Expression (${expr.source}): REPLACE mode missing 'replacedColumn' name.`
        );
        return;
      }
      const alias = `"${expr.replacedColumn.replace(/"/g, '""')}"`; // Alias is the name of the column being replaced
      expressionResults.push({
        sql: sqlSnippet,
        alias: alias,
        outputMode: "REPLACE",
        originalColumn: expr.replacedColumn,
      });
    } else {
      errorMessages.push(
        `Expression (${expr.source}): Unknown columnOutputMode "${expr.outputMode}".`
      );
    }
  });

  if (errorMessages.length > 0) {
    return `Error in Expression Node ${currentNodeId}:\n${errorMessages.join(
      "\n"
    )}`;
  }

  // Construct final select list by applying expressions
  // Start with input columns if available, or an empty list if not.
  let finalSelectExpressions =
    inputColumnNames.length > 0
      ? inputColumnNames.map((col) => `"${col.replace(/"/g, '""')}"`)
      : [];

  const processedAliases = new Set(); // Keep track of aliases already defined by expressions

  expressionResults.forEach((res) => {
    if (res.outputMode === "APPEND") {
      // If an input column already has this name, the new expression should effectively replace it in the list.
      // Or, if a previous expression in this same node created this alias, it should be updated.
      const existingIndex = finalSelectExpressions.findIndex((part) => {
        const aliasMatch = part.match(/AS\s+("?)([^"]+)\1$/i);
        const currentName = aliasMatch ? `"${aliasMatch[2]}"` : part;
        return currentName === res.alias;
      });

      if (existingIndex !== -1) {
        // Column with this name/alias already exists
        console.warn(
          `Node ${currentNodeId} (Expression): APPEND expression for ${res.alias} is overwriting an existing column or previous expression result with the same name.`
        );
        finalSelectExpressions[existingIndex] = `${res.sql} AS ${res.alias}`;
      } else {
        // New column to append
        finalSelectExpressions.push(`${res.sql} AS ${res.alias}`);
      }
      processedAliases.add(res.alias);
    } else if (res.outputMode === "REPLACE") {
      const originalColQuoted = `"${res.originalColumn.replace(/"/g, '""')}"`;
      let replaced = false;
      finalSelectExpressions = finalSelectExpressions.map((part) => {
        const aliasMatch = part.match(/AS\s+("?)([^"]+)\1$/i);
        const currentName = aliasMatch ? `"${aliasMatch[2]}"` : part;
        if (currentName === originalColQuoted) {
          replaced = true;
          return `${res.sql} AS ${res.alias}`; // Alias is same as originalColQuoted
        }
        return part;
      });
      if (!replaced) {
        // If the column to be replaced was not in the input set (e.g. inputColumnNames was empty or didn't include it)
        // or not created by a prior expression in this node, we add it as a new column.
        console.warn(
          `Node ${currentNodeId} (Expression): Column "${res.originalColumn}" for REPLACE not found in current select list. Adding it as a new column with the expression.`
        );
        finalSelectExpressions.push(`${res.sql} AS ${res.alias}`);
      }
      processedAliases.add(res.alias);
    }
  });

  // If finalSelectExpressions is empty (e.g., no input columns and only REPLACE expressions for non-existent columns)
  if (finalSelectExpressions.length === 0) {
    if (inputColumnNames.length === 0 && expressionResults.length > 0) {
      return `Error: Node ${currentNodeId} (Expression): No columns to select. Input columns were unknown and expressions (e.g., all REPLACE for non-existent columns) did not yield selectable output.`;
    } else if (inputColumnNames.length > 0 && expressionResults.length === 0) {
      // This case should be handled earlier (no expressions to process)
      finalSelectExpressions.push("*");
    } else if (
      inputColumnNames.length === 0 &&
      expressionResults.length === 0
    ) {
      // This case should be handled earlier
      return `Error: Node ${currentNodeId} (Expression): No input columns and no expressions defined. Cannot generate SQL.`;
    } else {
      // Fallback, though ideally logic above should prevent this.
      console.warn(
        `Node ${currentNodeId} (Expression): finalSelectExpressions was empty unexpectedly. Defaulting to SELECT *. Check expression logic.`
      );
      finalSelectExpressions.push("*");
    }
  }

  const selectClause = `SELECT\n  ${finalSelectExpressions.join(",\n  ")}`;

  // 6. Construct the final SQL query
  const sqlQuery = `
${selectClause}
FROM ${quotedPreviousNodeName};
`;
  return sqlQuery.trim();
}
