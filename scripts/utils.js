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

  function buildTimeFilter(timeColumn, startTime, endTime, isTimestamp) {
    const clauses = [];

    if (startTime != null) {
      const v = Number(startTime);
      if (Number.isFinite(v)) {
        if (isTimestamp) {
          clauses.push(`"${timeColumn}" >= to_timestamp(${v})`);
        } else {
          clauses.push(`"${timeColumn}" >= ${v}`);
        }
      }
    }

    if (endTime != null) {
      const v = Number(endTime);
      if (Number.isFinite(v)) {
        if (isTimestamp) {
          clauses.push(`"${timeColumn}" <= to_timestamp(${v})`);
        } else {
          clauses.push(`"${timeColumn}" <= ${v}`);
        }
      }
    }

    return clauses.length ? `WHERE ${clauses.join(" AND ")}` : "";
  }

  function getTimeSelectExpr(timeColumn, isTimestamp) {
    if (isTimestamp) {
      return `EPOCH("${timeColumn}") as "${timeColumn}"`;
    }
    return `"${timeColumn}"`;
  }

  function buildLttbQuery(file, timeCol, valueCols, whereSql, maxPoints, isTimestamp) {
    const numBuckets = maxPoints;
    const valueSelects = valueCols.map((c) => `"${c}"`).join(", ");

    let timeOrder;
    let timeAgg;
    if (isTimestamp) {
      timeOrder = `"${timeCol}"`;
      timeAgg = `EPOCH(MIN("${timeCol}")) as bucket_min_time`;
    } else {
      timeOrder = `"${timeCol}"`;
      timeAgg = `MIN("${timeCol}") as bucket_min_time`;
    }

    const avgSelects = valueCols.map((c) => `AVG("${c}") as avg_${c}`).join(", ");
    const finalSelects = valueCols.map((c) => `avg_${c} as "${c}"`).join(", ");

    return `
      WITH numbered AS (
        SELECT 
          "${timeCol}",
          ${valueSelects},
          ROW_NUMBER() OVER (ORDER BY ${timeOrder}) as rn,
          COUNT(*) OVER () as total
        FROM '${file}'
        ${whereSql}
      ),
      bucketed AS (
        SELECT 
          *,
          FLOOR((rn - 1) * ${numBuckets}::DOUBLE / NULLIF(total, 0)) as bucket
        FROM numbered
      ),
      bucket_stats AS (
        SELECT 
          bucket,
          ${timeAgg},
          ${avgSelects}
        FROM bucketed
        GROUP BY bucket
      )
      SELECT 
        bucket_min_time as "${timeCol}",
        ${finalSelects}
      FROM bucket_stats
      ORDER BY bucket
    `;
  }

  function buildMinMaxQuery(file, timeCol, valueCols, whereSql, maxPoints, isTimestamp) {
    const numBuckets = Math.floor(maxPoints / 2);
    const valueSelects = valueCols.map((c) => `"${c}"`).join(", ");

    let timeOutput;
    let timeOrder;
    if (isTimestamp) {
      timeOutput = `EPOCH("${timeCol}") as "${timeCol}"`;
      timeOrder = `"${timeCol}"`;
    } else {
      timeOutput = `"${timeCol}"`;
      timeOrder = `"${timeCol}"`;
    }

    return `
      WITH numbered AS (
        SELECT 
          "${timeCol}",
          ${valueSelects},
          ROW_NUMBER() OVER (ORDER BY ${timeOrder}) as rn,
          COUNT(*) OVER () as total
        FROM '${file}'
        ${whereSql}
      ),
      bucketed AS (
        SELECT 
          *,
          FLOOR((rn - 1) * ${numBuckets}::DOUBLE / NULLIF(total, 0)) as bucket
        FROM numbered
      ),
      first_points AS (
        SELECT 
          bucket,
          ${timeOutput},
          ${valueSelects},
          ROW_NUMBER() OVER (PARTITION BY bucket ORDER BY "${timeCol}") as pos
        FROM bucketed
      )
      SELECT "${timeCol}", ${valueSelects}
      FROM first_points
      WHERE pos = 1
      ORDER BY "${timeCol}"
    `;
  }

  function buildAvgQuery(file, timeCol, valueCols, whereSql, maxPoints, isTimestamp) {
    const numBuckets = maxPoints;
    const valueSelects = valueCols.map((c) => `"${c}"`).join(", ");

    let timeAgg;
    let timeOrder;
    if (isTimestamp) {
      timeAgg = `EPOCH(MIN("${timeCol}")) as "${timeCol}"`;
      timeOrder = `"${timeCol}"`;
    } else {
      timeAgg = `AVG("${timeCol}") as "${timeCol}"`;
      timeOrder = `"${timeCol}"`;
    }

    const avgSelects = valueCols.map((c) => `AVG("${c}") as "${c}"`).join(", ");

    return `
      WITH numbered AS (
        SELECT 
          "${timeCol}",
          ${valueSelects},
          ROW_NUMBER() OVER (ORDER BY ${timeOrder}) as rn,
          COUNT(*) OVER () as total
        FROM '${file}'
        ${whereSql}
      ),
      bucketed AS (
        SELECT 
          *,
          FLOOR((rn - 1) * ${numBuckets}::DOUBLE / NULLIF(total, 0)) as bucket
        FROM numbered
      )
      SELECT 
        ${timeAgg},
        ${avgSelects}
      FROM bucketed
      GROUP BY bucket
      ORDER BY "${timeCol}"
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
    COLORS,
  };
})();

window.Utils = Utils;
