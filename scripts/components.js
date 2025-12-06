const { formatBytes, formatNumber, debounce, COLORS } = window.Utils;
const { useEffect, useRef } = React;

const Spinner = () => (
  <div className="flex items-center justify-center p-4">
    <div className="spinner"></div>
  </div>
);

const FileSelector = ({ files, selectedFile, onSelect, loading }) => (
  <div className="bg-gray-800 rounded-lg p-4 shadow-lg">
    <h2 className="text-lg font-semibold mb-3 text-blue-400 flex items-center gap-2">
      <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth={2}
          d="M3 7v10a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-6l-2-2H5a2 2 0 00-2 2z"
        />
      </svg>
      Parquet Files
    </h2>

    {loading ? (
      <Spinner />
    ) : (
      <div className="space-y-2 max-h-60 overflow-y-auto">
        {files.length === 0 ? (
          <p className="text-gray-400 text-sm">No parquet files found</p>
        ) : (
          files.map((file) => (
            <button
              key={file.name}
              onClick={() => onSelect(file)}
              className={`w-full text-left p-3 rounded-lg transition-all ${
                selectedFile?.name === file.name
                  ? "bg-blue-600 text-white"
                  : "bg-gray-700 hover:bg-gray-600 text-gray-200"
              }`}
            >
              <div className="font-medium truncate">{file.name}</div>
              <div className="text-xs opacity-75 mt-1">
                {formatBytes(file.size_bytes)} • {formatNumber(file.row_count)} rows
              </div>
            </button>
          ))
        )}
      </div>
    )}
  </div>
);

const ColumnSelector = ({
  columns,
  timeColumn,
  selectedColumns,
  onTimeColumnChange,
  onColumnToggle,
  loading,
}) => {
  const temporalColumns = columns.filter((c) => c.category === "temporal" || c.category === "numeric");
  const numericColumns = columns.filter((c) => c.category === "numeric");

  return (
    <div className="bg-gray-800 rounded-lg p-4 shadow-lg">
      <h2 className="text-lg font-semibold mb-3 text-blue-400 flex items-center gap-2">
        <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={2}
            d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2"
          />
        </svg>
        Columns
      </h2>

      {loading ? (
        <Spinner />
      ) : columns.length === 0 ? (
        <p className="text-gray-400 text-sm">Select a file first</p>
      ) : (
        <div className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-300 mb-2">X-Axis (Time Column)</label>
            <select
              value={timeColumn || ""}
              onChange={(e) => onTimeColumnChange(e.target.value)}
              className="w-full bg-gray-700 border border-gray-600 rounded-lg px-3 py-2 text-white focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            >
              <option value="">Select time column...</option>
              {temporalColumns.map((col) => (
                <option key={col.name} value={col.name}>
                  {col.name} ({col.type})
                </option>
              ))}
            </select>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-300 mb-2">
              Y-Axis Variables ({selectedColumns.length} selected)
            </label>
            <div className="max-h-64 overflow-y-auto space-y-1">
              {numericColumns.map((col, idx) => (
                <label
                  key={col.name}
                  className={`flex items-center gap-3 p-2 rounded cursor-pointer transition-colors ${
                    selectedColumns.includes(col.name) ? "bg-gray-700" : "hover:bg-gray-700/50"
                  }`}
                >
                  <input
                    type="checkbox"
                    checked={selectedColumns.includes(col.name)}
                    onChange={() => onColumnToggle(col.name)}
                    className="w-4 h-4 rounded border-gray-500 text-blue-600 focus:ring-blue-500"
                  />
                  <span className="w-3 h-3 rounded-full" style={{ backgroundColor: COLORS[idx % COLORS.length] }} />
                  <span className="flex-1 truncate text-sm">{col.name}</span>
                  <span className="text-xs text-gray-500">{col.type}</span>
                </label>
              ))}
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

const QuerySettings = ({ settings, onChange }) => (
  <div className="bg-gray-800 rounded-lg p-4 shadow-lg">
    <h2 className="text-lg font-semibold mb-3 text-blue-400 flex items-center gap-2">
      <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth={2}
          d="M12 6V4m0 2a2 2 0 100 4m0-4a2 2 0 110 4m-6 8a2 2 0 100-4m0 4a2 2 0 110-4m0 4v2m0-6V4m6 6v10m6-2a2 2 0 100-4m0 4a2 2 0 110-4m0 4v2m0-6V4"
        />
      </svg>
      Settings
    </h2>

    <div className="space-y-4">
      <div>
        <label className="block text-sm font-medium text-gray-300 mb-2">
          Max Points: {formatNumber(settings.maxPoints)}
        </label>
        <input
          type="range"
          min="500"
          max="10000"
          step="500"
          value={settings.maxPoints}
          onChange={(e) => onChange({ ...settings, maxPoints: parseInt(e.target.value) })}
          className="w-full"
        />
      </div>

      <div>
        <label className="block text-sm font-medium text-gray-300 mb-2">Downsample Method</label>
        <select
          value={settings.downsampleMethod}
          onChange={(e) => onChange({ ...settings, downsampleMethod: e.target.value })}
          className="w-full bg-gray-700 border border-gray-600 rounded-lg px-3 py-2 text-white focus:ring-2 focus:ring-blue-500"
        >
          <option value="lttb">LTTB (Best Visual)</option>
          <option value="minmax">Min/Max (Peak Preservation)</option>
          <option value="avg">Average</option>
        </select>
      </div>
    </div>
  </div>
);

const StatsDisplay = ({ stats }) => (
  <div className="bg-gray-800 rounded-lg p-4 shadow-lg">
    <h2 className="text-lg font-semibold mb-3 text-blue-400 flex items-center gap-2">
      <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth={2}
          d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"
        />
      </svg>
      Query Stats
    </h2>

    <div className="grid grid-cols-2 gap-4 text-sm">
      <div>
        <div className="text-gray-400">Total Points</div>
        <div className="text-xl font-bold">{formatNumber(stats.totalPoints || 0)}</div>
      </div>
      <div>
        <div className="text-gray-400">Displayed</div>
        <div className="text-xl font-bold">{formatNumber(stats.returnedPoints || 0)}</div>
      </div>
      <div>
        <div className="text-gray-400">Downsampled</div>
        <div className={`text-lg font-bold ${stats.downsampled ? "text-yellow-400" : "text-green-400"}`}>
          {stats.downsampled ? "Yes" : "No"}
        </div>
      </div>
      <div>
        <div className="text-gray-400">Query Time</div>
        <div className="text-lg font-bold">{stats.queryTime || 0}ms</div>
      </div>
    </div>
  </div>
);

const Chart = ({ data, timeColumn, valueColumns, onZoom, loading, timeRange }) => {
  const chartRef = useRef(null);
  const chartInstance = useRef(null);
  const isZooming = useRef(false);

  useEffect(() => {
    if (!chartRef.current) return;

    chartInstance.current = echarts.init(chartRef.current, "dark");

    const resizeObserver = new ResizeObserver(() => {
      chartInstance.current?.resize();
    });
    resizeObserver.observe(chartRef.current);

    return () => {
      resizeObserver.disconnect();
      chartInstance.current?.dispose();
    };
  }, []);

  useEffect(() => {
    if (!chartInstance.current || !data || !timeColumn) return;

    const timeData = data[timeColumn] || [];

    const series = valueColumns.map((col, idx) => ({
      name: col,
      type: "line",
      symbol: "none",
      sampling: "lttb",
      data: (data[col] || []).map((val, i) => [timeData[i], val]),
      lineStyle: {
        width: 1.5,
      },
      emphasis: {
        lineStyle: {
          width: 2,
        },
      },
    }));

    const option = {
      backgroundColor: "transparent",
      animation: false,
      tooltip: {
        trigger: "axis",
        backgroundColor: "rgba(30, 41, 59, 0.95)",
        borderColor: "#475569",
        textStyle: {
          color: "#e2e8f0",
        },
        formatter: (params) => {
          if (!params.length) return "";
          const time = new Date(params[0].value[0] * 1000).toISOString();
          let html = `<div class="font-semibold mb-2">${time}</div>`;
          params.forEach((p) => {
            const value = p.value[1]?.toFixed(6) ?? "N/A";
            html += `<div class="flex justify-between gap-4">
                          <span>${p.marker} ${p.seriesName}</span>
                          <span class="font-mono">${value}</span>
                      </div>`;
          });
          return html;
        },
      },
      legend: {
        type: "scroll",
        top: 10,
        textStyle: {
          color: "#9ca3af",
        },
        pageTextStyle: {
          color: "#9ca3af",
        },
      },
      grid: {
        top: 60,
        left: 60,
        right: 40,
        bottom: 80,
      },
      xAxis: {
        type: "value",
        min: timeRange?.min,
        max: timeRange?.max,
        axisLabel: {
          formatter: (val) => {
            const date = new Date(val * 1000);
            return date.toISOString().substr(11, 8);
          },
          color: "#9ca3af",
        },
        axisLine: {
          lineStyle: { color: "#374151" },
        },
        splitLine: {
          lineStyle: { color: "#1f2937" },
        },
      },
      yAxis: {
        type: "value",
        axisLabel: {
          color: "#9ca3af",
          formatter: (val) => val.toPrecision(4),
        },
        axisLine: {
          lineStyle: { color: "#374151" },
        },
        splitLine: {
          lineStyle: { color: "#1f2937" },
        },
      },
      dataZoom: [
        {
          type: "inside",
          xAxisIndex: 0,
          filterMode: "none",
          throttle: 100,
        },
        {
          type: "slider",
          xAxisIndex: 0,
          filterMode: "none",
          bottom: 20,
          height: 30,
          borderColor: "#374151",
          backgroundColor: "#1f2937",
          fillerColor: "rgba(59, 130, 246, 0.2)",
          handleStyle: {
            color: "#3b82f6",
          },
          textStyle: {
            color: "#9ca3af",
          },
          brushSelect: false,
        },
      ],
      series,
      color: COLORS,
    };

    chartInstance.current.setOption(option, true);

    chartInstance.current.off("datazoom");
    chartInstance.current.on(
      "datazoom",
      debounce((params) => {
        if (isZooming.current) return;

        const xAxis = chartInstance.current.getModel().getComponent("xAxis", 0);
        const extent = xAxis.axis.scale.getExtent();

        if (extent[0] !== undefined && extent[1] !== undefined) {
          onZoom?.(extent[0], extent[1]);
        }
      }, 300)
    );
  }, [data, timeColumn, valueColumns, timeRange]);

  return (
    <div className="relative bg-gray-800 rounded-lg shadow-lg overflow-hidden">
      {loading && (
        <div className="absolute inset-0 bg-gray-900/70 flex items-center justify-center z-10">
          <div className="flex items-center gap-3">
            <div className="spinner"></div>
            <span>Loading data...</span>
          </div>
        </div>
      )}
      <div ref={chartRef} className="w-full h-[500px]"></div>
    </div>
  );
};

window.Components = { Spinner, FileSelector, ColumnSelector, QuerySettings, StatsDisplay, Chart };
