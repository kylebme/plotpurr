// Copyright (C) 2025 Kyle Bartholomew

// This file is part of PlotPurr.

// PlotPurr is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

// PlotPurr is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
// You should have received a copy of the GNU General Public License along with PlotPurr. If not, see <https://www.gnu.org/licenses/>. 

const Utils = (() => {
  const formatBytes = (bytes) => {
    if (bytes === 0) return "0 Bytes";
    const k = 1024;
    const sizes = ["Bytes", "KB", "MB", "GB"];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + " " + sizes[i];
  };

  const formatNumber = (num) => {
    return new Intl.NumberFormat().format(num);
  };

  const debounce = (func, wait) => {
    let timeout;
    return (...args) => {
      clearTimeout(timeout);
      timeout = setTimeout(() => func.apply(this, args), wait);
    };
  };

  function isTimestampType(dtype = "") {
    const lower = String(dtype).toLowerCase();
    return (
      lower.includes("timestamp") ||
      lower.includes("datetime") ||
      lower.includes("date") ||
      lower.includes("time")
    );
  }

  function categorizeType(dtype = "") {
    const lower = String(dtype).toLowerCase();

    if (
      lower.includes("timestamp") ||
      lower.includes("date") ||
      lower.includes("time")
    ) {
      return "temporal";
    }

    if (
      [
        "int",
        "float",
        "double",
        "decimal",
        "numeric",
        "bigint",
        "smallint",
        "tinyint",
        "real",
      ].some((t) => lower.includes(t))
    ) {
      return "numeric";
    }

    if (["varchar", "char", "string", "text"].some((t) => lower.includes(t))) {
      return "string";
    }

    if (lower.includes("bool")) {
      return "boolean";
    }

    return "other";
  }

  function toEpoch(value) {
    if (value == null) return null;
    if (typeof value === "number") return value;
    if (typeof value === "string") {
      const asNum = Number(value);
      if (!Number.isNaN(asNum)) return asNum;
      const asDate = new Date(value);
      if (!Number.isNaN(asDate.getTime())) {
        return asDate.getTime() / 1000;
      }
    }
    return null;
  }

  function buildTimeFilter(timeColumn, startTime, endTime, isTimestamp, timeUnit = "none") {
    const clauses = [];

    // start/end are expressed in the selected timeUnit (seconds, ms, µs, ns).
    const buildTimestampExpr = (v) => {
      if (!isTimestamp) return v;
      if (timeUnit === "unix_ms") return `toDateTime64(${v} / 1000, 3)`;
      if (timeUnit === "unix_us") return `toDateTime64(${v} / 1e6, 6)`;
      if (timeUnit === "unix_ns") return `toDateTime64(${v} / 1e9, 9)`;
      return `toDateTime(${v})`; // seconds
    };

    if (startTime != null) {
      const v = Number(startTime);
      if (Number.isFinite(v)) {
        const bound = isTimestamp ? buildTimestampExpr(v) : v;
        clauses.push(`\`${timeColumn}\` >= ${bound}`);
      }
    }

    if (endTime != null) {
      const v = Number(endTime);
      if (Number.isFinite(v)) {
        const bound = isTimestamp ? buildTimestampExpr(v) : v;
        clauses.push(`\`${timeColumn}\` <= ${bound}`);
      }
    }

    return clauses.length ? `WHERE ${clauses.join(" AND ")}` : "";
  }

  function getTimeSelectExpr(timeColumn, isTimestamp, timeUnit) {
    // Always emit numeric seconds for timestamp columns (with sub-second precision when requested)
    if (!isTimestamp) {
      return `\`${timeColumn}\``;
    }

    if (!timeUnit || timeUnit === "none" || timeUnit === "unix_s") {
      return `toUnixTimestamp(\`${timeColumn}\`) as \`${timeColumn}\``;
    }
    if (timeUnit === "unix_ms") {
      return `toUnixTimestamp64Milli(\`${timeColumn}\`) as \`${timeColumn}\``;
    }
    if (timeUnit === "unix_us") {
      return `toUnixTimestamp64Micro(\`${timeColumn}\`) as \`${timeColumn}\``;
    }
    if (timeUnit === "unix_ns") {
      return `toUnixTimestamp64Nano(\`${timeColumn}\`) as \`${timeColumn}\``;
    }

    return `toUnixTimestamp(\`${timeColumn}\`) as \`${timeColumn}\``;
  }

  function buildLttbQuery(source, timeCol, valueCols, whereSql, maxPoints, isTimestamp) {
    const valueSelects = valueCols.map((c) => `\`${c}\``).join(", ");
    const timeOrderExpr = isTimestamp ? `toUnixTimestamp(\`${timeCol}\`)` : `toFloat64(\`${timeCol}\`)`;
    const lttbValueCol = valueCols[0];
    const lttbValueExpr = lttbValueCol ? `toFloat64OrZero(toString(\`${lttbValueCol}\`))` : "0";

    return `
      WITH ordered AS (
        SELECT
          ${timeOrderExpr} AS t_order,
          \`${timeCol}\` AS t_value,
          ${valueSelects},
          row_number() OVER (ORDER BY ${timeOrderExpr}) AS rn,
          toFloat64(row_number() OVER (ORDER BY ${timeOrderExpr})) AS rn_f
        FROM ${source}
        ${whereSql}
      ),
      sampled AS (
        SELECT arrayJoin(lttb(${maxPoints})(rn_f, ${lttbValueExpr})) AS point
        FROM ordered
      )
      SELECT
        o.t_value AS \`${timeCol}\`,
        ${valueCols.map((c) => `o.\`${c}\``).join(", ")}
      FROM sampled s
      JOIN ordered o ON o.rn = toUInt64(point.1)
      ORDER BY o.t_value
    `;
  }

  function buildMinMaxQuery(source, timeCol, valueCols, whereSql, maxPoints, isTimestamp) {
    const numBuckets = Math.floor(maxPoints / 2);
    const valueSelects = valueCols.map((c) => `\`${c}\``).join(", ");
    const primaryValueCol = valueCols[0] || timeCol;

    let timeOrder;
    let timeOutExpr;
    if (isTimestamp) {
      timeOrder = `\`${timeCol}\``;
      timeOutExpr = `toUnixTimestamp(\`${timeCol}\`)`;
    } else {
      timeOrder = `\`${timeCol}\``;
      timeOutExpr = `\`${timeCol}\``;
    }

    return `
      WITH numbered AS (
        SELECT 
          \`${timeCol}\`,
          ${valueSelects},
          ROW_NUMBER() OVER (ORDER BY ${timeOrder}) as rn,
          COUNT(*) OVER () as total
        FROM ${source}
        ${whereSql}
      ),
      bucketed AS (
        SELECT 
          *,
          FLOOR((rn - 1) * CAST(${numBuckets} AS Float64) / NULLIF(total, 0)) as bucket
        FROM numbered
      ),
      ranked AS (
        SELECT
          bucket,
          \`${timeCol}\` as t_raw,
          ${timeOutExpr} as \`${timeCol}\`,
          ${valueSelects},
          ROW_NUMBER() OVER (PARTITION BY bucket ORDER BY \`${primaryValueCol}\` ASC, t_raw) as rmin,
          ROW_NUMBER() OVER (PARTITION BY bucket ORDER BY \`${primaryValueCol}\` DESC, t_raw) as rmax
        FROM bucketed
      )
      SELECT \`${timeCol}\`, ${valueSelects}
      FROM ranked
      WHERE rmin = 1 OR rmax = 1
      ORDER BY \`${timeCol}\`
    `;
  }

  function buildAvgQuery(source, timeCol, valueCols, whereSql, maxPoints, isTimestamp) {
    const numBuckets = maxPoints;
    const valueSelects = valueCols.map((c) => `\`${c}\``).join(", ");

    let timeAgg;
    let timeOrder;
    if (isTimestamp) {
      timeAgg = `toUnixTimestamp(min(\`${timeCol}\`)) as \`${timeCol}\``;
      timeOrder = `\`${timeCol}\``;
    } else {
      timeAgg = `AVG(\`${timeCol}\`) as \`${timeCol}\``;
      timeOrder = `\`${timeCol}\``;
    }

    const avgSelects = valueCols.map((c) => `AVG(\`${c}\`) as \`${c}\``).join(", ");

    return `
      WITH numbered AS (
        SELECT 
          \`${timeCol}\`,
          ${valueSelects},
          ROW_NUMBER() OVER (ORDER BY ${timeOrder}) as rn,
          COUNT(*) OVER () as total
        FROM ${source}
        ${whereSql}
      ),
      bucketed AS (
        SELECT 
          *,
          FLOOR((rn - 1) * CAST(${numBuckets} AS Float64) / NULLIF(total, 0)) as bucket
        FROM numbered
      )
      SELECT 
        ${timeAgg},
        ${avgSelects}
      FROM bucketed
      GROUP BY bucket
      ORDER BY \`${timeCol}\`
    `;
  }

  function resultsToColumnar(rows, timeCol, valueCols) {
    const columns = [timeCol, ...valueCols];
    const data = {};
    columns.forEach((c) => {
      data[c] = [];
    });

    (rows || []).forEach((row) => {
      columns.forEach((col, idx) => {
        let val = row[idx];
        if (val !== null && typeof val !== "number") {
          const n = Number(val);
          if (!Number.isNaN(n)) {
            val = n;
          }
        }
        data[col].push(val);
      });
    });

    return data;
  }

  const COLORS = [
    "#3b82f6",
    "#ef4444",
    "#10b981",
    "#f59e0b",
    "#8b5cf6",
    "#ec4899",
    "#06b6d4",
    "#84cc16",
    "#f97316",
    "#6366f1",
  ];

  function buildFullQueryString(params) {
    const {
      file,
      time_column,
      value_columns,
      format,
      timeUnit = "none",
      columnsMeta = [],
      maxPoints = 10000,
      downsampleMethod = "minmax",
    } = params;

    if (!file || !time_column || !value_columns?.length) {
      return "";
    }

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

    const inferFmt = (fileName, hint) => {
      if (hint) return hint;
      if (!fileName) return null;
      const lower = fileName.toLowerCase();
      const extMatch = lower.match(/(\.[^.]+)$/);
      const ext = extMatch ? extMatch[1] : "";
      return FORMAT_BY_EXT[ext] || null;
    };

    const escapeFilePath = (f) => String(f || "").replace(/'/g, "\\'");
    const fmt = inferFmt(file, format) || "Parquet";
    const safeFormat = fmt.replace(/[^A-Za-z]/g, "") || "Parquet";
    const tableExpr = `file('${escapeFilePath(file)}', '${safeFormat}')`;

    let colType = null;
    if (columnsMeta?.length) {
      const meta = columnsMeta.find((c) => c.name === time_column);
      if (meta) colType = meta.type;
    }
    const isTs = colType ? isTimestampType(colType) : false;

    // Use template placeholders for time range so zooming still works
    const whereSql = buildTimeFilterTemplated(time_column, isTs, timeUnit);

    // Generate the full downsampling query
    let query;
    if (downsampleMethod === "minmax") {
      query = buildMinMaxQueryTemplated(tableExpr, time_column, value_columns, whereSql, maxPoints, isTs);
    } else if (downsampleMethod === "avg") {
      query = buildAvgQueryTemplated(tableExpr, time_column, value_columns, whereSql, maxPoints, isTs);
    } else {
      query = buildLttbQueryTemplated(tableExpr, time_column, value_columns, whereSql, maxPoints, isTs);
    }

    return query;
  }

  function buildTimeFilterTemplated(timeColumn, isTimestamp, timeUnit = "none") {
    const clauses = [];

    const buildTimestampExpr = (placeholder) => {
      if (!isTimestamp) return placeholder;
      if (timeUnit === "unix_ms") return `toDateTime64(${placeholder} / 1000, 3)`;
      if (timeUnit === "unix_us") return `toDateTime64(${placeholder} / 1e6, 6)`;
      if (timeUnit === "unix_ns") return `toDateTime64(${placeholder} / 1e9, 9)`;
      return `toDateTime(${placeholder})`;
    };

    const startBound = isTimestamp ? buildTimestampExpr("{{START_TIME}}") : "{{START_TIME}}";
    const endBound = isTimestamp ? buildTimestampExpr("{{END_TIME}}") : "{{END_TIME}}";

    clauses.push(`\`${timeColumn}\` >= ${startBound}`);
    clauses.push(`\`${timeColumn}\` <= ${endBound}`);

    return `WHERE ${clauses.join(" AND ")}`;
  }

  function buildLttbQueryTemplated(source, timeCol, valueCols, whereSql, maxPoints, isTimestamp) {
    const valueSelects = valueCols.map((c) => `\`${c}\``).join(", ");
    const timeOrderExpr = isTimestamp ? `toUnixTimestamp(\`${timeCol}\`)` : `toFloat64(\`${timeCol}\`)`;

    // For multiple columns, use combined magnitude (sqrt of sum of squares) for LTTB
    // This preserves points that are significant in ANY dimension
    let lttbValueExpr;
    if (valueCols.length === 1) {
      lttbValueExpr = `toFloat64OrZero(toString(\`${valueCols[0]}\`))`;
    } else {
      const sumOfSquares = valueCols
        .map((c) => `pow(toFloat64OrZero(toString(\`${c}\`)), 2)`)
        .join(" + ");
      lttbValueExpr = `sqrt(${sumOfSquares})`;
    }

    return `-- LTTB downsampling (maxPoints: ${maxPoints})
-- Uses combined magnitude of all columns for point selection
-- {{START_TIME}} and {{END_TIME}} are replaced with actual values during zoom/pan
WITH ordered AS (
  SELECT
    ${timeOrderExpr} AS t_order,
    \`${timeCol}\` AS t_value,
    ${valueSelects},
    row_number() OVER (ORDER BY ${timeOrderExpr}) AS rn,
    toFloat64(row_number() OVER (ORDER BY ${timeOrderExpr})) AS rn_f
  FROM ${source}
  ${whereSql}
),
sampled AS (
  SELECT arrayJoin(lttb(${maxPoints})(rn_f, ${lttbValueExpr})) AS point
  FROM ordered
)
SELECT
  o.t_value AS \`${timeCol}\`,
  ${valueCols.map((c) => `o.\`${c}\``).join(", ")}
FROM sampled s
JOIN ordered o ON o.rn = toUInt64(point.1)
ORDER BY o.t_value`;
  }

  function buildMinMaxQueryTemplated(source, timeCol, valueCols, whereSql, maxPoints, isTimestamp) {
    // Adjust buckets based on number of columns to maintain similar point count
    const numBuckets = Math.floor(maxPoints / (2 * valueCols.length));
    const valueSelects = valueCols.map((c) => `\`${c}\``).join(", ");

    let timeOrder;
    let timeOutExpr;
    if (isTimestamp) {
      timeOrder = `\`${timeCol}\``;
      timeOutExpr = `toUnixTimestamp(\`${timeCol}\`)`;
    } else {
      timeOrder = `\`${timeCol}\``;
      timeOutExpr = `\`${timeCol}\``;
    }

    // Generate ranking for min/max of EACH value column
    const rankingCols = valueCols.map((col, idx) => `
    ROW_NUMBER() OVER (PARTITION BY bucket ORDER BY \`${col}\` ASC, t_raw) as rmin_${idx},
    ROW_NUMBER() OVER (PARTITION BY bucket ORDER BY \`${col}\` DESC, t_raw) as rmax_${idx}`
    ).join(",");

    // Generate WHERE condition to keep min/max rows for ANY column
    const whereConditions = valueCols.map((_, idx) =>
      `rmin_${idx} = 1 OR rmax_${idx} = 1`
    ).join(" OR ");

    return `-- Min/Max downsampling (maxPoints: ${maxPoints}, buckets: ${numBuckets})
-- Preserves min/max peaks for ALL value columns
-- {{START_TIME}} and {{END_TIME}} are replaced with actual values during zoom/pan
WITH numbered AS (
  SELECT
    \`${timeCol}\`,
    ${valueSelects},
    ROW_NUMBER() OVER (ORDER BY ${timeOrder}) as rn,
    COUNT(*) OVER () as total
  FROM ${source}
  ${whereSql}
),
bucketed AS (
  SELECT
    *,
    FLOOR((rn - 1) * CAST(${numBuckets} AS Float64) / NULLIF(total, 0)) as bucket
  FROM numbered
),
ranked AS (
  SELECT
    bucket,
    \`${timeCol}\` as t_raw,
    ${timeOutExpr} as \`${timeCol}\`,
    ${valueSelects},${rankingCols}
  FROM bucketed
)
SELECT \`${timeCol}\`, ${valueSelects}
FROM ranked
WHERE ${whereConditions}
ORDER BY \`${timeCol}\``;
  }

  function buildAvgQueryTemplated(source, timeCol, valueCols, whereSql, maxPoints, isTimestamp) {
    const numBuckets = maxPoints;
    const valueSelects = valueCols.map((c) => `\`${c}\``).join(", ");

    let timeAgg;
    let timeOrder;
    if (isTimestamp) {
      timeAgg = `toUnixTimestamp(min(\`${timeCol}\`)) as \`${timeCol}\``;
      timeOrder = `\`${timeCol}\``;
    } else {
      timeAgg = `AVG(\`${timeCol}\`) as \`${timeCol}\``;
      timeOrder = `\`${timeCol}\``;
    }

    const avgSelects = valueCols.map((c) => `AVG(\`${c}\`) as \`${c}\``).join(", ");

    return `-- Average downsampling (maxPoints: ${maxPoints})
-- Edit the WHERE clause or add filters as needed
-- {{START_TIME}} and {{END_TIME}} are replaced with actual values during zoom/pan
WITH numbered AS (
  SELECT
    \`${timeCol}\`,
    ${valueSelects},
    ROW_NUMBER() OVER (ORDER BY ${timeOrder}) as rn,
    COUNT(*) OVER () as total
  FROM ${source}
  ${whereSql}
),
bucketed AS (
  SELECT
    *,
    FLOOR((rn - 1) * CAST(${numBuckets} AS Float64) / NULLIF(total, 0)) as bucket
  FROM numbered
)
SELECT
  ${timeAgg},
  ${avgSelects}
FROM bucketed
GROUP BY bucket
ORDER BY \`${timeCol}\``;
  }

  return {
    formatBytes,
    formatNumber,
    debounce,
    isTimestampType,
    categorizeType,
    toEpoch,
    buildTimeFilter,
    getTimeSelectExpr,
    buildLttbQuery,
    buildMinMaxQuery,
    buildAvgQuery,
    resultsToColumnar,
    buildFullQueryString,
    COLORS,
  };
})();

window.Utils = Utils;
