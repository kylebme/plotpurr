const { formatBytes, formatNumber, debounce, COLORS } = window.Utils;
const { useEffect, useRef, useState, useCallback } = React;

const Spinner = () => (
  <div className="flex items-center justify-center p-4">
    <div className="spinner"></div>
  </div>
);

const FileSelector = ({ files, selectedFile, onSelect, loading, onBrowse }) => (
  <div className="bg-gray-800 rounded-lg p-4 shadow-lg">
    <div className="flex items-center justify-between gap-2 mb-3">
      <h2 className="text-lg font-semibold text-blue-400 flex items-center gap-2">
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

      {onBrowse && (
        <button
          onClick={onBrowse}
          className="px-3 py-1.5 bg-blue-600 hover:bg-blue-500 text-white text-sm rounded-md transition-colors"
        >
          Choose…
        </button>
      )}
    </div>

    {loading ? (
      <Spinner />
    ) : (
      <div className="space-y-2 max-h-60 overflow-y-auto">
        {files.length === 0 ? (
          <p className="text-gray-400 text-sm">
            {onBrowse ? "No parquet files selected yet" : "No parquet files found"}
          </p>
        ) : (
          files.map((file) => (
            <button
              key={file.id || file.path || file.name}
              onClick={() => onSelect(file)}
              className={`w-full text-left p-3 rounded-lg transition-all ${
                selectedFile?.id === file.id || selectedFile?.path === file.path || selectedFile?.name === file.name
                  ? "bg-blue-600 text-white"
                  : "bg-gray-700 hover:bg-gray-600 text-gray-200"
              }`}
            >
              <div className="font-medium truncate" title={file.path || file.name}>
                {file.name}
              </div>
              <div className="text-xs opacity-75 mt-1 space-y-0.5">
                <div>{formatBytes(file.size_bytes)} • {formatNumber(file.row_count)} rows</div>
                {file.path && <div className="text-[11px] text-gray-300 truncate">{file.path}</div>}
              </div>
            </button>
          ))
        )}
      </div>
    )}
  </div>
);

const ColumnSelector = ({ file, columns, timeColumn, onTimeColumnChange, onColumnAdd, loading, activeColumns = [] }) => {
  const [yFilter, setYFilter] = useState("");
  const fileKey = file?.id || file?.path || file?.name;
  const temporalColumns = columns.filter((c) => c.category === "temporal" || c.category === "numeric");
  const numericColumns = columns.filter((c) => c.category === "numeric");
  const filteredNumeric = yFilter
    ? numericColumns.filter((c) => c.name.toLowerCase().includes(yFilter.toLowerCase()))
    : numericColumns;

  useEffect(() => {
    setYFilter("");
  }, [file?.name]);

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
      ) : !file ? (
        <p className="text-gray-400 text-sm">Select a file to browse columns</p>
      ) : columns.length === 0 ? (
        <p className="text-gray-400 text-sm">Select a file first</p>
      ) : (
        <div className="space-y-4">
          <div className="flex items-center justify-between text-xs text-gray-400 bg-gray-700/40 px-3 py-2 rounded border border-gray-700">
            <span className="font-medium text-gray-200 truncate" title={file.name}>
              {file.name}
            </span>
            <span className="text-gray-400">row count: {formatNumber(file.row_count || 0)}</span>
          </div>
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
            <div className="flex items-center justify-between gap-2 mb-2">
              <label className="block text-sm font-medium text-gray-300">Y-Axis Variables</label>
              <input
                type="text"
                value={yFilter}
                onChange={(e) => setYFilter(e.target.value)}
                placeholder="Filter"
                className="w-40 bg-gray-700 border border-gray-600 rounded px-2 py-1 text-sm text-gray-200 focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              />
            </div>
            <p className="text-xs text-gray-500 mb-2">Drag to a plot. Drop on edges to create split views.</p>
            <div className="max-h-64 overflow-y-auto space-y-1">
              {filteredNumeric.length === 0 ? (
                <div className="text-xs text-gray-500 px-2 py-3">No matching numeric columns</div>
              ) : (
                filteredNumeric.map((col, idx) => (
                  <div
                    key={col.name}
                    draggable
                    onDragStart={(e) => {
                      const payload = JSON.stringify({ column: col.name, file: fileKey, fileName: file?.name });
                      e.dataTransfer.setData("text/plain", payload);
                      e.dataTransfer.setData("application/json", payload);
                      e.dataTransfer.effectAllowed = "copy";
                    }}
                    onDoubleClick={() => onColumnAdd?.({ column: col.name, file: fileKey, fileName: file?.name })}
                    className="flex items-center gap-3 p-2 rounded cursor-grab active:cursor-grabbing transition-colors bg-gray-700/60 hover:bg-gray-700 border border-transparent hover:border-gray-600"
                  >
                    <span className="w-3 h-3 rounded-full" style={{ backgroundColor: COLORS[idx % COLORS.length] }} />
                    <div className="flex-1 min-w-0">
                      <div className="truncate text-sm text-gray-100">{col.name}</div>
                      <div className="text-xs text-gray-500">Drag to a plot or double-click to add</div>
                    </div>
                    {activeColumns.includes(col.name) && (
                      <span className="text-[10px] text-blue-200 bg-blue-500/10 px-2 py-1 rounded-full border border-blue-500/30">
                        Active
                      </span>
                    )}
                    <span className="text-xs text-gray-500">{col.type}</span>
                  </div>
                ))
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

const QuerySettings = ({ settings, onChange }) => {
  const tooltipEnabled = settings.showTooltip !== false;

  return (
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
            min="1000"
            max="20000"
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

        <div className="flex items-center justify-between bg-gray-700/40 border border-gray-700 rounded-lg px-3 py-2">
          <div>
            <label className="block text-sm font-medium text-gray-300">Cursor / Tooltip</label>
            <p className="text-xs text-gray-500">Show hover line and tooltip on every plot.</p>
          </div>
          <button
            type="button"
            role="switch"
            aria-checked={tooltipEnabled}
            onClick={() => onChange({ ...settings, showTooltip: !tooltipEnabled })}
            className={`relative inline-flex h-6 w-11 items-center rounded-full transition-colors ${
              tooltipEnabled ? "bg-blue-500" : "bg-gray-600"
            }`}
          >
            <span
              className={`inline-block h-5 w-5 transform rounded-full bg-white shadow transition-transform ${
                tooltipEnabled ? "translate-x-5" : "translate-x-1"
              }`}
            ></span>
          </button>
        </div>
      </div>
    </div>
  );
};

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

function getDropZone(e, container) {
  if (!container) return "center";
  const rect = container.getBoundingClientRect();
  const x = e.clientX - rect.left;
  const y = e.clientY - rect.top;
  const w = rect.width;
  const h = rect.height;

  const leftEdge = w * 0.25;
  const rightEdge = w * 0.75;
  const topEdge = h * 0.25;
  const bottomEdge = h * 0.75;

  if (x < leftEdge) return "left";
  if (x > rightEdge) return "right";
  if (y < topEdge) return "top";
  if (y > bottomEdge) return "bottom";
  return "center";
}

const DropOverlay = ({ zone }) => {
  const base = "absolute bg-blue-500/10 border border-blue-400/50";
  return (
    <div className="absolute inset-0 pointer-events-none">
      <div className="absolute inset-0 rounded-lg border-2 border-dashed border-blue-500/50" />
      {zone === "center" && <div className={`${base} inset-[20%] rounded-lg`} />}
      {zone === "left" && <div className={`${base} inset-y-[15%] left-[6%] right-[55%] rounded-l-lg`} />}
      {zone === "right" && <div className={`${base} inset-y-[15%] right-[6%] left-[55%] rounded-r-lg`} />}
      {zone === "top" && <div className={`${base} inset-x-[10%] top-[6%] bottom-[55%] rounded-t-lg`} />}
      {zone === "bottom" && <div className={`${base} inset-x-[10%] bottom-[6%] top-[55%] rounded-b-lg`} />}
      <div className="absolute bottom-3 right-3 bg-gray-900/70 text-xs text-blue-100 px-3 py-1 rounded-full shadow-lg border border-blue-500/40">
        Drop to {zone === "center" ? "add to plot" : `split ${zone}`}
      </div>
    </div>
  );
};

const PlotPanel = ({
  title,
  plotId,
  series,
  viewRange,
  fullTimeRange,
  onZoom,
  resetToken,
  loading,
  onDropVariable,
  onRemoveSeries,
  onRemovePlot,
  canRemovePlot,
  getColor,
  showTooltip,
}) => {
  const dropRef = useRef(null);
  const [dropZone, setDropZone] = useState(null);
  const [dragging, setDragging] = useState(false);

  const handleDragOver = (e) => {
    e.preventDefault();
    e.dataTransfer.dropEffect = "copy";
    const zone = getDropZone(e, dropRef.current);
    setDragging(true);
    setDropZone(zone);
  };

  const handleDragLeave = (e) => {
    if (e.currentTarget.contains(e.relatedTarget)) return;
    setDragging(false);
    setDropZone(null);
  };

  const handleDrop = (e) => {
    e.preventDefault();
    const raw = e.dataTransfer.getData("application/json") || e.dataTransfer.getData("text/plain");
    let payload = null;
    try {
      payload = raw ? JSON.parse(raw) : null;
    } catch (err) {
      payload = raw ? { column: raw } : null;
    }
    const zone = getDropZone(e, dropRef.current) || dropZone || "center";
    setDragging(false);
    setDropZone(null);
    if (payload && onDropVariable) {
      onDropVariable(payload, zone);
    }
  };

  return (
    <div className="bg-gray-800 rounded-lg shadow-lg border border-gray-700/60 overflow-hidden">
      <div className="px-4 py-3 border-b border-gray-700 flex items-center justify-between">
        <div className="flex items-center gap-2 text-sm text-gray-300">
          <span className="px-2 py-1 rounded bg-gray-700/70 text-blue-200 font-semibold">{title}</span>
          <div className="flex flex-wrap items-center gap-2">
            {series.map((s) => (
              <span
                key={s.id}
                className="inline-flex items-center gap-2 px-2 py-1 rounded-full bg-gray-700 text-xs text-gray-100 border border-gray-600"
              >
                <span
                  className="inline-block w-2.5 h-2.5 rounded-full"
                  style={{ backgroundColor: getColor?.(s) }}
                  title={s.file}
                />
                {s.file} • {s.column}
                <button
                  onClick={() => onRemoveSeries?.(s.id)}
                  className="text-gray-400 hover:text-red-400 transition-colors"
                  title="Remove variable"
                >
                  ×
                </button>
              </span>
            ))}
            {series.length === 0 && <span className="text-xs text-gray-500">Drop variables to plot</span>}
          </div>
        </div>
        <div className="flex items-center gap-2">
          {canRemovePlot && (
            <button
              onClick={onRemovePlot}
              className="text-xs px-2 py-1 rounded bg-gray-700 hover:bg-gray-600 border border-gray-600 text-gray-200"
            >
              Close plot
            </button>
          )}
        </div>
      </div>

      <div
        ref={dropRef}
        onDragOver={handleDragOver}
        onDragLeave={handleDragLeave}
        onDrop={handleDrop}
        className="relative min-h-[360px]"
      >
        {series.length > 0 ? (
          <Chart
            seriesList={series}
            onZoom={onZoom}
            loading={loading}
            viewRange={viewRange}
            fullTimeRange={fullTimeRange}
            getColor={getColor}
            resetToken={resetToken}
            showTooltip={showTooltip}
          />
        ) : (
          <div className="h-[360px] flex flex-col items-center justify-center text-gray-500 gap-2">
            <svg className="w-12 h-12 opacity-60" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="1.5" d="M4 16l4-4 4 4 6-6" />
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="1.5" d="M20 16h-4m0 0v4m0-4v-4" />
            </svg>
            <div className="text-sm">Drag a variable here to start a chart</div>
          </div>
        )}

        {dragging && <DropOverlay zone={dropZone || "center"} />}
      </div>
    </div>
  );
};

const Chart = ({
  seriesList = [],
  onZoom,
  loading,
  viewRange,
  fullTimeRange,
  getColor,
  resetToken,
  showTooltip = true,
}) => {
  const chartRef = useRef(null);
  const chartInstance = useRef(null);
  const isZooming = useRef(false);
  const [interactionMode, setInteractionMode] = useState("box"); // "box" or "pan"
  const [shiftDown, setShiftDown] = useState(false);
  const shiftDownRef = useRef(false);
  const xDomainRef = useRef({ min: null, max: null });
  const yDomainRef = useRef({ min: null, max: null });
  const [showLoader, setShowLoader] = useState(false);
  const loaderTimerRef = useRef(null);
  const lastZoomRef = useRef({ start: null, end: null });

  const emitZoom = useCallback(
    (start, end) => {
      if (!Number.isFinite(start) || !Number.isFinite(end)) return;
      const nextStart = Math.min(start, end);
      const nextEnd = Math.max(start, end);
      if (nextStart === nextEnd) return;

      const tolerance = 1e-4; // avoid tiny floating changes firing twice
      const { start: prevStart, end: prevEnd } = lastZoomRef.current || {};
      if (prevStart != null && prevEnd != null) {
        if (Math.abs(prevStart - nextStart) < tolerance && Math.abs(prevEnd - nextEnd) < tolerance) {
          return;
        }
      }

      lastZoomRef.current = { start: nextStart, end: nextEnd };
      onZoom?.(nextStart, nextEnd);
    },
    [onZoom]
  );

  useEffect(() => {
    if (!viewRange) return;
    const start = viewRange.start ?? viewRange.min;
    const end = viewRange.end ?? viewRange.max;
    if (!Number.isFinite(start) || !Number.isFinite(end)) return;
    lastZoomRef.current = { start, end };
  }, [viewRange?.start, viewRange?.end, viewRange?.min, viewRange?.max]);

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
    if (!chartInstance.current) return;

    const domainStart = fullTimeRange?.min ?? fullTimeRange?.start ?? viewRange?.min ?? viewRange?.start;
    const domainEnd = fullTimeRange?.max ?? fullTimeRange?.end ?? viewRange?.max ?? viewRange?.end;
    const rangeStart = viewRange?.start ?? viewRange?.min ?? domainStart;
    const rangeEnd = viewRange?.end ?? viewRange?.max ?? domainEnd;
    xDomainRef.current = { min: domainStart, max: domainEnd };
    // Compute vertical data domain from series
    let yMin = Infinity;
    let yMax = -Infinity;

    seriesList.forEach((s) => {
      (s.data || []).forEach((pt) => {
        const v = Array.isArray(pt) ? pt[1] : null;
        if (v == null || !isFinite(v)) return;
        if (v < yMin) yMin = v;
        if (v > yMax) yMax = v;
      });
    });

    let yAxisMin = null;
    let yAxisMax = null;
    if (isFinite(yMin) && isFinite(yMax) && yMin !== Infinity && yMax !== -Infinity) {
      yAxisMin = yMin;
      yAxisMax = yMax;
      yDomainRef.current = { min: yMin, max: yMax };
    } else {
      yDomainRef.current = { min: null, max: null };
    }


    const series = seriesList.map((s, idx) => ({
      name: s.name || `${s.file} • ${s.column}`,
      type: "line",
      symbol: "none",
      sampling: "lttb",
      data: s.data || [],
      lineStyle: {
        width: 1.5,
        color: getColor?.(s, idx),
      },
      emphasis: {
        lineStyle: {
          width: 2,
          color: getColor?.(s, idx),
        },
      },
      itemStyle: {
        color: getColor?.(s, idx),
      },
    }));

    const tooltip = showTooltip
      ? {
          show: true,
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
        }
      : { show: false, trigger: "none", axisPointer: { show: false } };

    const option = {
      backgroundColor: "transparent",
      animation: false,
      tooltip,
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
        min: domainStart,
        max: domainEnd,
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
        min: isFinite(yAxisMin) ? yAxisMin : undefined,
        max: isFinite(yAxisMax) ? yAxisMax : undefined,
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
      brush: interactionMode === "box"
        ? {
            toolbox: [],
            xAxisIndex: 0,
            yAxisIndex: 0,
            brushMode: "single",
            brushType: "rect",
            transformable: false,
            throttleType: "debounce",
            throttleDelay: 100,
            brushStyle: {
              color: "rgba(59, 130, 246, 0.15)",
              borderWidth: 1,
              borderColor: "#3b82f6",
            },
          }
        : { toolbox: [] },
      dataZoom: [
        {
          id: "x-inside",
          type: "inside",
          xAxisIndex: 0,
          filterMode: "none",
          throttle: 100,
          startValue: rangeStart,
          endValue: rangeEnd,
          zoomOnMouseWheel: false,
          moveOnMouseWheel: false,
          moveOnMouseMove: interactionMode === "pan" && !shiftDown,
        },
        {
          id: "x-slider",
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
          startValue: rangeStart,
          endValue: rangeEnd,
        },
        {
          id: "y-inside",
          type: "inside",
          yAxisIndex: 0,
          filterMode: "none",
          throttle: 100,
          zoomOnMouseWheel: false,
          moveOnMouseWheel: false,
          moveOnMouseMove: interactionMode === "pan" && shiftDown,
        },
      ],
      series,
      color: COLORS,
      toolbox: {
        show: false,
      },
    };

    isZooming.current = true;
    chartInstance.current.setOption(option, { notMerge: false, replaceMerge: ["series"] });
    isZooming.current = false;

    chartInstance.current.off("datazoom");
    chartInstance.current.on(
      "datazoom",
      debounce(() => {
        if (isZooming.current) return;
        const model = chartInstance.current?.getModel();
        if (!model) return;
        const xAxis = model.getComponent("xAxis", 0);
        const extent = xAxis?.axis?.scale?.getExtent?.() || [];
        if (extent[0] !== undefined && extent[1] !== undefined && isFinite(extent[0]) && isFinite(extent[1])) {
          emitZoom(extent[0], extent[1]);
        }
      }, 300)
    );

    chartInstance.current.off("brushEnd");
    if (interactionMode === "box") {
      chartInstance.current.on("brushEnd", (params) => {
        const area = params.areas?.[0];
        const xRange = area?.coordRange?.[0];
        const yRange = area?.coordRange?.[1];
        const [xStart, xEnd] = xRange || [];
        const [yStart, yEnd] = yRange || [];
        if (!isFinite(xStart) || !isFinite(xEnd) || xStart === xEnd) return;
        const minX = Math.min(xStart, xEnd);
        const maxX = Math.max(xStart, xEnd);
        const minY = isFinite(yStart) && isFinite(yEnd) ? Math.min(yStart, yEnd) : null;
        const maxY = isFinite(yStart) && isFinite(yEnd) ? Math.max(yStart, yEnd) : null;
        chartInstance.current?.dispatchAction({
          type: "dataZoom",
          dataZoomId: "x-inside",
          startValue: minX,
          endValue: maxX,
        });
        chartInstance.current?.dispatchAction({
          type: "dataZoom",
          dataZoomId: "x-slider",
          startValue: minX,
          endValue: maxX,
        });
        if (minY != null && maxY != null && minY !== maxY) {
          chartInstance.current?.dispatchAction({
            type: "dataZoom",
            dataZoomId: "y-inside",
            startValue: minY,
            endValue: maxY,
          });
        }
        chartInstance.current?.dispatchAction({ type: "brush", areas: [] });
      });
    }
  }, [seriesList, viewRange, fullTimeRange, interactionMode, getColor, emitZoom, shiftDown, showTooltip]);

  useEffect(() => {
    const handleKeyDown = (e) => {
      if (e.shiftKey) setShiftDown(true);
    };
    const handleKeyUp = (e) => {
      if (!e.shiftKey) setShiftDown(false);
    };
    window.addEventListener("keydown", handleKeyDown);
    window.addEventListener("keyup", handleKeyUp);
    return () => {
      window.removeEventListener("keydown", handleKeyDown);
      window.removeEventListener("keyup", handleKeyUp);
    };
  }, []);

  useEffect(() => {
    shiftDownRef.current = shiftDown;
  }, [shiftDown]);

  useEffect(() => {
    if (!chartInstance.current) return;
    if (interactionMode === "box") {
      chartInstance.current.dispatchAction({
        type: "takeGlobalCursor",
        key: "brush",
        brushOption: {
          brushType: "rect",
          brushMode: "single",
        },
      });
    } else {
      chartInstance.current.dispatchAction({
        type: "takeGlobalCursor",
        key: "brush",
        brushOption: { brushType: false },
      });
      chartInstance.current.dispatchAction({ type: "brush", areas: [] });
    }
  }, [interactionMode]);

  useEffect(() => {
    if (!chartInstance.current) return;
    isZooming.current = true;
    chartInstance.current.dispatchAction({
      type: "dataZoom",
      dataZoomId: "y-inside",
      start: 0,
      end: 100,
    });
    isZooming.current = false;
  }, [resetToken]);

  useEffect(() => {
    if (!chartInstance.current) return;
    const zr = chartInstance.current.getZr();
    if (!zr || !zr.handler) return;

    const handleWheel = (e) => {
      if (!chartInstance.current) return;
      const delta = e.event?.deltaY ?? e.event?.wheelDelta ?? 0;
      if (delta === 0) return;
      e.event?.preventDefault?.();

      const base = 1.0015;
      const zoomFactor = Math.pow(base, delta);
      if (!isFinite(zoomFactor) || zoomFactor === 1) return;

      const shiftHeld = shiftDownRef.current || !!e.event?.shiftKey;

      const point = chartInstance.current.convertFromPixel(
        { xAxisIndex: 0, yAxisIndex: 0 },
        [e.offsetX, e.offsetY]
      );
      const anchorX = point?.[0];
      const anchorY = point?.[1];

      const model = chartInstance.current.getModel();
      if (!model) return;

      const applyZoom = (axis) => {
        const comp = model.getComponent(axis === "x" ? "xAxis" : "yAxis", 0);
        const extent = comp?.axis?.scale?.getExtent?.();
        if (!extent || extent.length < 2) return;

        let [axisMin, axisMax] = extent;
        if (!isFinite(axisMin) || !isFinite(axisMax) || axisMin === axisMax) return;

        // Data domain for this axis
        const domainRef = axis === "x" ? xDomainRef : yDomainRef;
        let domainMin = domainRef.current.min;
        let domainMax = domainRef.current.max;

        if (!isFinite(domainMin) || !isFinite(domainMax)) {
          domainMin = axisMin;
          domainMax = axisMax;
        }
        if (domainMin > domainMax) {
          const tmp = domainMin;
          domainMin = domainMax;
          domainMax = tmp;
        }

        const domainSpan = domainMax - domainMin;

        // Current visible range = intersection of axis extent and data domain
        let curMin = Math.max(axisMin, domainMin);
        let curMax = Math.min(axisMax, domainMax);

        if (!isFinite(curMin) || !isFinite(curMax) || curMin === curMax) {
          curMin = axisMin;
          curMax = axisMax;
        }

        const anchorCoord = axis === "x" ? anchorX : anchorY;
        const anchor = isFinite(anchorCoord) ? anchorCoord : (curMin + curMax) / 2;

        const span = curMax - curMin;
        let newSpan = span * zoomFactor;

        // Prevent zooming in too far and zooming out beyond domain
        const baseSpan = domainSpan > 0 ? domainSpan : span;
        const minSpan = baseSpan / 1e6 || 1e-9;
        newSpan = Math.max(minSpan, newSpan);
        if (domainSpan > 0) {
          newSpan = Math.min(newSpan, domainSpan);
        }

        let newMin = anchor - (anchor - curMin) * zoomFactor;
        let newMax = newMin + newSpan;

        // Clamp into data domain if we have one
        if (domainSpan > 0) {
          newMin = Math.max(domainMin, Math.min(newMin, domainMax - newSpan));
          newMax = newMin + newSpan;
        }

        if (axis === "x") {
          chartInstance.current.dispatchAction({
            type: "dataZoom",
            dataZoomId: "x-inside",
            startValue: newMin,
            endValue: newMax,
          });
          chartInstance.current.dispatchAction({
            type: "dataZoom",
            dataZoomId: "x-slider",
            startValue: newMin,
            endValue: newMax,
          });
        } else {
          chartInstance.current.dispatchAction({
            type: "dataZoom",
            dataZoomId: "y-inside",
            startValue: newMin,
            endValue: newMax,
          });
        }
      };

      if (shiftHeld) {
        applyZoom("y");
      } else {
        applyZoom("x");
      }
    };

    zr.on("mousewheel", handleWheel);
    return () => {
      if (zr.handler) {
        zr.off("mousewheel", handleWheel);
      }
    };
  }, []);

  useEffect(() => {
    if (loading) {
      if (loaderTimerRef.current) clearTimeout(loaderTimerRef.current);
      loaderTimerRef.current = setTimeout(() => setShowLoader(true), 120);
    } else {
      if (loaderTimerRef.current) {
        clearTimeout(loaderTimerRef.current);
        loaderTimerRef.current = null;
      }
      setShowLoader(false);
    }
    return () => {
      if (loaderTimerRef.current) {
        clearTimeout(loaderTimerRef.current);
        loaderTimerRef.current = null;
      }
    };
  }, [loading]);

  return (
    <div className="relative bg-gray-800 rounded-lg shadow-lg overflow-hidden">
      <div className="absolute top-3 left-3 z-20 flex items-center gap-2 bg-gray-900/70 px-2 py-1 rounded border border-gray-700">
        <button
          onClick={() => setInteractionMode("box")}
          className={`text-xs px-3 py-1 rounded border transition-colors flex items-center gap-1 ${
            interactionMode === "box"
              ? "bg-blue-600 text-white border-blue-500 shadow"
              : "bg-gray-800 text-gray-200 border-gray-600 hover:bg-gray-700"
          }`}
        >
          <span>▭</span>
          <span>Box</span>
        </button>
        <button
          onClick={() => setInteractionMode("pan")}
          className={`text-xs px-3 py-1 rounded border transition-colors flex items-center gap-1 ${
            interactionMode === "pan"
              ? "bg-blue-600 text-white border-blue-500 shadow"
              : "bg-gray-800 text-gray-200 border-gray-600 hover:bg-gray-700"
          }`}
        >
          <span>✋</span>
          <span>Pan</span>
        </button>
        {interactionMode === "box" && (
          <span className="text-[11px] text-blue-100 bg-blue-500/10 border border-blue-500/40 px-2 py-1 rounded">
            Drag to select an area
          </span>
        )}
      </div>
      {showLoader && (
        <div className="absolute inset-0 bg-gray-900/70 flex items-center justify-center z-10">
          <div className="flex items-center gap-3">
            <div className="spinner"></div>
            <span>Loading data...</span>
          </div>
        </div>
      )}
      <div ref={chartRef} className="w-full h-[360px]"></div>
    </div>
  );
};

window.Components = { Spinner, FileSelector, ColumnSelector, QuerySettings, StatsDisplay, Chart, PlotPanel };
