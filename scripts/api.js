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

const api = {
  async sql(query, params = []) {
    const res = await fetch(`${API_BASE}/api/sql`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query, params }),
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
          const result = await api.sql(`SELECT COUNT(*) AS cnt FROM '${file.name}'`);
          const colIndex = result.columns?.indexOf("cnt") ?? (result.columns?.length ? 0 : 0);
          const row = result.rows?.[0] || [0];
          const rowCount = colIndex >= 0 && row[colIndex] != null ? row[colIndex] : row[0];

          return { ...file, row_count: rowCount };
        } catch (err) {
          console.error("Error getting row count for", file.name, err);
          return { ...file, row_count: 0 };
        }
      })
    );

    return filesWithCounts;
  },

  async getColumns(file) {
    const result = await api.sql(`DESCRIBE SELECT * FROM '${file}'`);

    const nameIndex = result.columns?.indexOf("column_name") ?? 0;
    const typeIndex = result.columns?.indexOf("column_type") ?? 1;

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

  async getTimeRange(file, timeColumn, columnType, valueColumns = []) {
    const isTs = isTimestampType(columnType);
    const nonNullFilter =
      valueColumns && valueColumns.length
        ? `WHERE ${valueColumns.map((c) => `"${c}" IS NOT NULL`).join(" OR ")}`
        : "";
    let query;

    if (isTs) {
      query = `
        SELECT 
          MIN("${timeColumn}") AS min_time,
          MAX("${timeColumn}") AS max_time,
          EPOCH(MIN("${timeColumn}")) AS min_epoch,
          EPOCH(MAX("${timeColumn}")) AS max_epoch,
          COUNT(*) AS total_count
        FROM '${file}'
        ${nonNullFilter}
      `;
    } else {
      query = `
        SELECT 
          MIN("${timeColumn}") AS min_time,
          MAX("${timeColumn}") AS max_time,
          COUNT(*) AS total_count
        FROM '${file}'
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

    const minVal = row[idxMin >= 0 ? idxMin : 0];
    const maxVal = row[idxMax >= 0 ? idxMax : 1];
    const totalCount = row[idxTotal >= 0 ? idxTotal : 2];

    const minEpoch = isTs
      ? row[idxMinEpoch >= 0 ? idxMinEpoch : idxMin >= 0 ? idxMin : 0]
      : toEpoch(minVal);
    const maxEpoch = isTs
      ? row[idxMaxEpoch >= 0 ? idxMaxEpoch : idxMax >= 0 ? idxMax : 1]
      : toEpoch(maxVal);

    return {
      min: String(minVal),
      max: String(maxVal),
      min_epoch: minEpoch,
      max_epoch: maxEpoch,
      total_count: totalCount,
      is_timestamp: isTs,
    };
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
    const whereSql = buildTimeFilter(time_column, start_time, end_time, isTs);

    const countQuery = `
      SELECT COUNT(*) AS cnt 
      FROM '${file}'
      ${whereSql}
    `;
    const countResult = await api.sql(countQuery);
    const countRow = countResult.rows?.[0] || [0];
    const countIndex = countResult.columns?.indexOf("cnt") ?? 0;
    const totalPoints =
      countIndex >= 0 && countRow[countIndex] != null ? countRow[countIndex] : countRow[0] || 0;

    const valueColsSql = value_columns.map((c) => `"${c}"`).join(", ");
    const timeSelect = getTimeSelectExpr(time_column, isTs);

    let dataRows = [];
    let downsampled = false;
    const method = downsample_method || "lttb";

    if (totalPoints <= max_points) {
      const query = `
        SELECT ${timeSelect}, ${valueColsSql}
        FROM '${file}'
        ${whereSql}
        ORDER BY "${time_column}"
      `;
      const res = await api.sql(query);
      dataRows = res.rows || [];
    } else {
      let query;
      if (method === "minmax") {
        query = buildMinMaxQuery(file, time_column, value_columns, whereSql, max_points, isTs);
      } else if (method === "avg") {
        query = buildAvgQuery(file, time_column, value_columns, whereSql, max_points, isTs);
      } else {
        query = buildLttbQuery(file, time_column, value_columns, whereSql, max_points, isTs);
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
