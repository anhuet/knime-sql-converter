// src/functions/convertColumnFilterNodeToSQL.js

// Utility function to get a value from an entry array by its key.
// This version is provided by the user.
const getEntryValue = (data, key) => {
  // Check if data or data.entry is undefined/null
  if (!data || !data.entry) return "";
  // Ensure data.entry is an array
  const entries = Array.isArray(data.entry) ? data.entry : [data.entry];
  // Find the entry with the matching key
  const entry = entries.find((e) => e._attributes && e._attributes.key === key);
  // Return the value of the found entry, or an empty string if not found
  return entry && entry._attributes ? entry._attributes.value : "";
};

// Utility function to find a configuration node by its _attributes.key.
// This version is provided by the user.
const findConfigByKey = (config, key) => {
  // Check if the config is undefined/null
  if (!config) return null;
  // If config is an array, find the node with the matching key
  if (Array.isArray(config)) {
    return config.find(
      (node) => node._attributes && node._attributes.key === key
    );
  }
  // If config is a single object, check if its key matches
  else if (config._attributes && config._attributes.key === key) {
    return config;
  }
  // Return null if no matching node is found
  return null;
};

/**
 * Converts a KNIME Column Filter node configuration to an SQL query.
 * The configuration is expected to have included_names and excluded_names.
 * Each selected column will be on a new line in the SQL query.
 *
 * @param {object} nodeConfig - The full node configuration object (JSON from settings.xml).
 * @param {string} previousNodeName - The name of the previous node (for table name).
 * @returns {string} - The generated SQL query or an error message if something is missing.
 */
export function convertColumnFilterNodeToSQL(nodeConfig, previousNodeName) {
  console.log(nodeConfig);
  // Step 1: Ensure the node is a Column Filter by checking the "factory" entry.
  // Note: getEntryValue expects nodeConfig to directly contain the 'entry' array/object.
  const factory = getEntryValue(nodeConfig, "factory");
  const COLUMN_FILTER_FACTORY =
    "org.knime.base.node.preproc.filter.column.DataColumnSpecFilterNodeFactory";
  if (factory !== COLUMN_FILTER_FACTORY) {
    return "-- This function only converts Column Filter nodes. Incorrect factory found.";
  }

  // Step 2: Locate the "model" node from the top-level config.
  // nodeConfig.config should be the array of config elements.
  const modelNode = findConfigByKey(nodeConfig.config, "model");
  if (!modelNode || !modelNode.config) {
    return "-- Model node not found or invalid in the configuration.";
  }

  // Step 3: Locate the "column-filter" node within the model.
  const columnFilterNode = findConfigByKey(modelNode.config, "column-filter");
  if (!columnFilterNode || !columnFilterNode.config) {
    return "-- Column filter configuration ('column-filter' node) not found.";
  }

  // Step 4: Extract included and excluded column names.
  const includedNamesNode = findConfigByKey(
    columnFilterNode.config,
    "included_names"
  );
  const excludedNamesNode = findConfigByKey(
    columnFilterNode.config,
    "excluded_names"
  );

  const includedColumns = [];
  const excludedColumns = [];

  // Extract included column names
  if (includedNamesNode && includedNamesNode.entry) {
    const includedEntries = Array.isArray(includedNamesNode.entry)
      ? includedNamesNode.entry
      : [includedNamesNode.entry];
    includedEntries.forEach((entry) => {
      // Check if entry and _attributes exist, and key is not "array-size"
      if (
        entry &&
        entry._attributes &&
        entry._attributes.key !== "array-size"
      ) {
        includedColumns.push(entry._attributes.value);
      }
    });
  }

  // Extract excluded column names
  if (excludedNamesNode && excludedNamesNode.entry) {
    const excludedEntries = Array.isArray(excludedNamesNode.entry)
      ? excludedNamesNode.entry
      : [excludedNamesNode.entry];
    excludedEntries.forEach((entry) => {
      // Check if entry and _attributes exist, and key is not "array-size"
      if (
        entry &&
        entry._attributes &&
        entry._attributes.key !== "array-size"
      ) {
        excludedColumns.push(entry._attributes.value);
      }
    });
  }

  // Step 5: Build the SQL query
  // Filter out excluded columns from included columns
  const finalColumns = includedColumns.filter(
    (col) => !excludedColumns.includes(col)
  );

  // Determine the FROM clause, ensuring previousNodeName is quoted if it contains special characters or spaces
  const fromClause = `"${(previousNodeName || "input_table").replace(
    /"/g,
    '""'
  )}"`;

  // If no columns are left after filtering, select all columns (*)
  if (finalColumns.length === 0) {
    return `SELECT\n  *\nFROM ${fromClause};`;
  }

  // Create the SELECT clause with each column on a new line and indented
  // Also, ensure column names are properly quoted
  const selectClause = finalColumns
    .map((col) => `  "${col.replace(/"/g, '""')}"`) // Add indentation and quote column names
    .join(",\n"); // Join with comma and newline

  return `SELECT\n${selectClause}\nFROM ${fromClause};`;
}
