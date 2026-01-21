// Copyright (C) 2025 Kyle Bartholomew

// This file is part of PlotPurr.

// PlotPurr is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

// PlotPurr is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
// You should have received a copy of the GNU General Public License along with PlotPurr. If not, see <https://www.gnu.org/licenses/>. 

const {
  isTimestampType,
  categorizeType,
  toEpoch,
  buildTimeFilter,
  getTimeSelectExpr,
  buildLttbQuery,
  buildMinMaxQuery,
  buildAvgQuery,
  resultsToColumnar,
} = window.Utils;

const API_BASE = "";
const FORMAT_BY_EXT = {
  ".parquet": "Parquet",
  ".csv": "CSVWithNames",
  ".tsv": "TSVWithNames",
  ".json": "JSONEachRow",
  ".jsonl": "JSONEachRow",
  ".ndjson": "JSONEachRow",
  ".arrow": "Arrow",
  ".feather": "Arrow",
  ".orc": "ORC",
  ".avro": "Avro",
};

const inferFormat = (fileName, hint) => {
  if (hint) return hint;
  if (!fileName) return null;
  const lower = fileName.toLowerCase();
  const extMatch = lower.match(/(\.[^.]+)$/);
  const ext = extMatch ? extMatch[1] : "";
  return FORMAT_BY_EXT[ext] || null;
};

const escapeFilePath = (file) => String(file || "").replace(/'/g, "\\'");

const fileTable = (file, formatHint) => {
  const fmt = inferFormat(file, formatHint) || "Parquet";
  const safeFormat = fmt.replace(/[^A-Za-z]/g, "") || "Parquet";
  return `file('${escapeFilePath(file)}', '${safeFormat}')`;
};
const needsFileSetting = (sql) => /\bfile\s*\(/i.test(sql);
const hasSettingsClause = (sql) => /\bsettings\b/i.test(sql);
const hasIntrospectionSetting = (sql) => /allow_introspection_functions\s*=\s*1/i.test(sql);

const finalizeQuery = (sql) => {
  const cleaned = (sql ?? "").toString().trim().replace(/;+$/, "");
  if (!needsFileSetting(cleaned)) return cleaned;
  if (hasIntrospectionSetting(cleaned)) return cleaned;
  if (hasSettingsClause(cleaned)) return `${cleaned}, allow_introspection_functions=1`;
  return `${cleaned} SETTINGS allow_introspection_functions=1`;
};

const api = {
  async sql(query, params = []) {
    const finalQuery = finalizeQuery(query);

    const res = await fetch(`${API_BASE}/api/sql`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query: finalQuery, params }),
    });

    if (!res.ok) {
      const text = await res.text();
      throw new Error(`SQL request failed with ${res.status}: ${text || res.statusText}`);
    }

    return await res.json();
  },

  async getFiles() {
    const res = await fetch(`${API_BASE}/api/files`);
    if (!res.ok) {
      throw new Error(`Failed to fetch files: ${res.statusText}`);
    }
    const data = await res.json();
    const files = data.files || [];

    const filesWithCounts = await Promise.all(
      files.map(async (file) => {
        try {
          const tableExpr = fileTable(file.path || file.name, file.format);
          const result = await api.sql(`SELECT COUNT(*) AS cnt FROM ${tableExpr}`);
          const colIndex = result.columns?.indexOf("cnt") ?? (result.columns?.length ? 0 : 0);
          const row = result.rows?.[0] || [0];
          const rowCount = colIndex >= 0 && row[colIndex] != null ? row[colIndex] : row[0];

          return { ...file, row_count: rowCount, format: inferFormat(file.name, file.format) };
        } catch (err) {
          console.error("Error getting row count for", file.name, err);
          return { ...file, row_count: 0, format: inferFormat(file.name, file.format) };
        }
      })
    );

    return filesWithCounts;
  },

  async setPaths(paths) {
    const res = await fetch(`${API_BASE}/api/set_paths`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ paths }),
    });

    if (!res.ok) {
      const text = await res.text();
      throw new Error(`Failed to set paths: ${text || res.statusText}`);
    }

    return res.json();
  },

  async getColumns(file, format) {
    const tableExpr = fileTable(file, format);
    const result = await api.sql(`DESCRIBE TABLE ${tableExpr}`);

    const nameIndex = result.columns?.indexOf("name") ?? 0;
    const typeIndex = result.columns?.indexOf("type") ?? 1;

    return (result.rows || []).map((row) => {
      const name = row[nameIndex];
      const type = row[typeIndex];
      return {
        name,
        type,
        category: categorizeType(type),
      };
    });
  },

  async getTimeRange(file, timeColumn, columnType, valueColumns = [], format, timeUnit = "none") {
    const isTs = isTimestampType(columnType);
    const nonNullFilter =
      valueColumns && valueColumns.length
        ? `WHERE ${valueColumns.map((c) => `\`${c}\` IS NOT NULL`).join(" OR ")}`
        : "";
    const tableExpr = fileTable(file, format);
    let query;

    if (isTs && timeUnit && timeUnit !== "none") {
      let epochExpr;
      if (timeUnit === "unix_s") {
        epochExpr = `toUnixTimestamp(\`${timeColumn}\`)`;
      } else if (timeUnit === "unix_ms") {
        epochExpr = `toUnixTimestamp64Milli(\`${timeColumn}\`)`;
      } else if (timeUnit === "unix_us") {
        epochExpr = `toUnixTimestamp64Micro(\`${timeColumn}\`)`;
      } else if (timeUnit === "unix_ns") {
        epochExpr = `toUnixTimestamp64Nano(\`${timeColumn}\`)`;
      } else {
        epochExpr = `toUnixTimestamp(\`${timeColumn}\`)`;
      }

      query = `
        SELECT 
          MIN(${epochExpr}) AS min_epoch,
          MAX(${epochExpr}) AS max_epoch,
          COUNT(*) AS total_count
        FROM ${tableExpr}
        ${nonNullFilter}
      `;
    } else if (isTs) {
      query = `
        SELECT 
          MIN(\`${timeColumn}\`) AS min_time,
          MAX(\`${timeColumn}\`) AS max_time,
          toUnixTimestamp(MIN(\`${timeColumn}\`)) AS min_epoch,
          toUnixTimestamp(MAX(\`${timeColumn}\`)) AS max_epoch,
          COUNT(*) AS total_count
        FROM ${tableExpr}
        ${nonNullFilter}
      `;
    } else {
      query = `
        SELECT 
          MIN(\`${timeColumn}\`) AS min_time,
          MAX(\`${timeColumn}\`) AS max_time,
          COUNT(*) AS total_count
        FROM ${tableExpr}
        ${nonNullFilter}
      `;
    }

    const result = await api.sql(query);
    const row = result.rows?.[0];

    if (!row) {
      return {
        min: null,
        max: null,
        min_epoch: null,
        max_epoch: null,
        total_count: 0,
        is_timestamp: isTs,
      };
    }

    const cols = result.columns || [];
    const idxMin = cols.indexOf("min_time");
    const idxMax = cols.indexOf("max_time");
    const idxMinEpoch = cols.indexOf("min_epoch");
    const idxMaxEpoch = cols.indexOf("max_epoch");
    const idxTotal = cols.indexOf("total_count");

    const minVal = idxMin >= 0 ? row[idxMin] : row[0];
    const maxVal = idxMax >= 0 ? row[idxMax] : row[1];
    const totalCount = row[idxTotal >= 0 ? idxTotal : 2];

    let minEpoch;
    let maxEpoch;

    if (isTs && timeUnit && timeUnit !== "none") {
      const idxMinEp = cols.indexOf("min_epoch");
      const idxMaxEp = cols.indexOf("max_epoch");
      minEpoch = idxMinEp >= 0 ? row[idxMinEp] : null;
      maxEpoch = idxMaxEp >= 0 ? row[idxMaxEp] : null;
    } else {
      minEpoch = isTs
        ? row[idxMinEpoch >= 0 ? idxMinEpoch : idxMin >= 0 ? idxMin : 0]
        : toEpoch(minVal);
      maxEpoch = isTs
        ? row[idxMaxEpoch >= 0 ? idxMaxEpoch : idxMax >= 0 ? idxMax : 1]
        : toEpoch(maxVal);
    }

    return {
      min: String(minVal),
      max: String(maxVal),
      min_epoch: minEpoch,
      max_epoch: maxEpoch,
      total_count: totalCount,
      is_timestamp: isTs,
    };
  },

  async validateSql(sql, startTime, endTime) {
    // Replace template placeholders with actual values for validation
    let processedSql = sql
      .replace(/\{\{START_TIME\}\}/g, String(startTime ?? 0))
      .replace(/\{\{END_TIME\}\}/g, String(endTime ?? 0));

    const testQuery = `${processedSql.trim().replace(/;+$/, "")} LIMIT 0`;
    try {
      await api.sql(testQuery);
      return { valid: true, error: null };
    } catch (err) {
      return { valid: false, error: err.message || String(err) };
    }
  },

  async queryData(params) {
    const {
      file,
      time_column,
      value_columns,
      start_time,
      end_time,
      max_points,
      downsample_method,
      columnsMeta,
      format,
      timeUnit = "none",
      customSql = null,
    } = params;

    if (!file || !time_column || !value_columns?.length) {
      return {
        data: {},
        total_points: 0,
        returned_points: 0,
        downsampled: false,
        downsample_method: null,
      };
    }

    // If custom SQL is provided, execute it with template replacement
    if (customSql) {
      try {
        // Replace template placeholders with actual time values
        let processedSql = customSql
          .replace(/\{\{START_TIME\}\}/g, String(start_time))
          .replace(/\{\{END_TIME\}\}/g, String(end_time));

        const res = await api.sql(processedSql);
        const dataRows = res.rows || [];
        const resultColumns = res.columns || [];

        // Map columns by name from result, not by position
        // This handles cases where columns may be reordered or renamed
        const data = {};
        const allExpectedCols = [time_column, ...value_columns];

        // Build index map: find which result column index corresponds to each expected column
        const colIndexMap = {};
        allExpectedCols.forEach((expectedCol) => {
          // Try exact match first
          let idx = resultColumns.indexOf(expectedCol);
          if (idx === -1) {
            // Try case-insensitive match
            idx = resultColumns.findIndex(
              (rc) => rc.toLowerCase() === expectedCol.toLowerCase()
            );
          }
          colIndexMap[expectedCol] = idx;
        });

        // Initialize data arrays
        allExpectedCols.forEach((col) => {
          data[col] = [];
        });

        // Populate data using the index map
        dataRows.forEach((row) => {
          allExpectedCols.forEach((col) => {
            const idx = colIndexMap[col];
            let val = idx >= 0 ? row[idx] : null;
            if (val !== null && typeof val !== "number") {
              const n = Number(val);
              if (!Number.isNaN(n)) val = n;
            }
            data[col].push(val);
          });
        });

        return {
          data,
          total_points: dataRows.length,
          returned_points: dataRows.length,
          downsampled: true,
          downsample_method: "custom",
          customSql: true,
        };
      } catch (err) {
        throw new Error(`Custom SQL error: ${err.message || err}`);
      }
    }

    let colType = null;
    if (columnsMeta?.length) {
      const meta = columnsMeta.find((c) => c.name === time_column);
      if (meta) colType = meta.type;
    }
    if (!colType) {
      const cols = await api.getColumns(file);
      const meta = cols.find((c) => c.name === time_column);
      colType = meta ? meta.type : "DOUBLE";
    }

    const isTs = isTimestampType(colType);
    const whereSql = buildTimeFilter(time_column, start_time, end_time, isTs, timeUnit);
    const tableExpr = fileTable(file, format);

    const countQuery = `
      SELECT COUNT(*) AS cnt
      FROM ${tableExpr}
      ${whereSql}
    `;
    const countResult = await api.sql(countQuery);
    const countRow = countResult.rows?.[0] || [0];
    const countIndex = countResult.columns?.indexOf("cnt") ?? 0;
    const totalPoints =
      countIndex >= 0 && countRow[countIndex] != null ? countRow[countIndex] : countRow[0] || 0;

    const valueColsSql = value_columns.map((c) => `\`${c}\``).join(", ");
    const timeSelect = getTimeSelectExpr(time_column, isTs, timeUnit);

    let dataRows = [];
    let downsampled = false;
    const method = downsample_method || "lttb";

    if (totalPoints <= max_points) {
      const query = `
        SELECT ${timeSelect}, ${valueColsSql}
        FROM ${tableExpr}
        ${whereSql}
        ORDER BY \`${time_column}\`
      `;
      const res = await api.sql(query);
      dataRows = res.rows || [];
    } else {
      let query;
      if (method === "minmax") {
        query = buildMinMaxQuery(tableExpr, time_column, value_columns, whereSql, max_points, isTs);
      } else if (method === "avg") {
        query = buildAvgQuery(tableExpr, time_column, value_columns, whereSql, max_points, isTs);
      } else {
        query = buildLttbQuery(tableExpr, time_column, value_columns, whereSql, max_points, isTs);
      }
      const res = await api.sql(query);
      dataRows = res.rows || [];
      downsampled = true;
    }

    const data = resultsToColumnar(dataRows, time_column, value_columns);

    return {
      data,
      total_points: totalPoints,
      returned_points: dataRows.length,
      downsampled,
      downsample_method: downsampled ? method : null,
    };
  },
};

window.api = api;
