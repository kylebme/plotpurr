const { formatBytes, formatNumber, debounce, toUPlotData, COLORS } = window.Utils;
const { useEffect, useRef, useState, useMemo } = React;

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

const ColumnSelector = ({ file, columns, timeColumn, onTimeColumnChange, onColumnAdd, loading, activeColumns = [] }) => {
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
            <label className="block text-sm font-medium text-gray-300 mb-2">Y-Axis Variables</label>
            <p className="text-xs text-gray-500 mb-2">Drag to a plot. Drop on edges to create split views.</p>
            <div className="max-h-64 overflow-y-auto space-y-1">
              {numericColumns.map((col, idx) => (
                <div
                  key={col.name}
                  draggable
                  onDragStart={(e) => {
                    const payload = JSON.stringify({ column: col.name, file: file?.name });
                    e.dataTransfer.setData("text/plain", payload);
                    e.dataTransfer.setData("application/json", payload);
                    e.dataTransfer.effectAllowed = "copy";
                  }}
                  onDoubleClick={() => onColumnAdd?.({ column: col.name, file: file.name })}
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

const Chart = ({ seriesList = [], onZoom, loading, viewRange, fullTimeRange, getColor, resetToken }) => {
  const containerRef = useRef(null);
  const uplotRef = useRef(null);
  const tooltipRef = useRef(null);
  const [interactionMode, setInteractionMode] = useState("box");
  const [showLoader, setShowLoader] = useState(false);
  const loaderTimerRef = useRef(null);
  const lastZoomRef = useRef({ start: null, end: null });
  const xLockRef = useRef(null);
  const yLockRef = useRef(null);
  const isUpdatingScale = useRef(false);
  const shiftDownRef = useRef(false);
  const xDomainRef = useRef({ min: null, max: null });
  const isPanning = useRef(false);
  const panStart = useRef({ x: 0, scaleMin: 0, scaleMax: 0 });

  // Debounced zoom callback
  const debouncedZoom = useMemo(
    () =>
      debounce((start, end) => {
        if (!Number.isFinite(start) || !Number.isFinite(end)) return;
        const nextStart = Math.min(start, end);
        const nextEnd = Math.max(start, end);
        if (nextStart === nextEnd) return;

        const tolerance = 1e-4;
        const { start: prevStart, end: prevEnd } = lastZoomRef.current || {};
        if (prevStart != null && prevEnd != null) {
          if (Math.abs(prevStart - nextStart) < tolerance && Math.abs(prevEnd - nextEnd) < tolerance) {
            return;
          }
        }

        lastZoomRef.current = { start: nextStart, end: nextEnd };
        onZoom?.(nextStart, nextEnd);
      }, 300),
    [onZoom]
  );

  // Convert series data to uPlot format
  const uplotData = useMemo(() => toUPlotData(seriesList), [seriesList]);
  const seriesKey = useMemo(
    () => (seriesList || []).map((s) => s?.id ?? `${s?.file ?? ""}::${s?.column ?? ""}::${s?.timeColumn ?? ""}`).join("|"),
    [seriesList]
  );

  // Compute domain bounds
  const domainStart = fullTimeRange?.min ?? fullTimeRange?.start ?? viewRange?.min ?? viewRange?.start;
  const domainEnd = fullTimeRange?.max ?? fullTimeRange?.end ?? viewRange?.max ?? viewRange?.end;
  const rangeStart = viewRange?.start ?? viewRange?.min ?? domainStart;
  const rangeEnd = viewRange?.end ?? viewRange?.max ?? domainEnd;

  useEffect(() => {
    xDomainRef.current = { min: domainStart, max: domainEnd };
  }, [domainStart, domainEnd]);

  // Track shift key
  useEffect(() => {
    const handleKeyDown = (e) => {
      if (e.shiftKey) shiftDownRef.current = true;
    };
    const handleKeyUp = (e) => {
      if (!e.shiftKey) shiftDownRef.current = false;
    };
    window.addEventListener("keydown", handleKeyDown);
    window.addEventListener("keyup", handleKeyUp);
    return () => {
      window.removeEventListener("keydown", handleKeyDown);
      window.removeEventListener("keyup", handleKeyUp);
    };
  }, []);

  // Build series config for uPlot
  const seriesConfig = useMemo(() => {
    return [
      { label: "Time" },
      ...seriesList.map((s, idx) => ({
        label: s.name || `${s.file} • ${s.column}`,
        stroke: getColor?.(s, idx) || COLORS[idx % COLORS.length],
        width: 1.5,
        points: { show: false },
        spanGaps: false,
      })),
    ];
  }, [seriesKey, getColor]);

  // Create/destroy uPlot instance
  useEffect(() => {
    if (!containerRef.current) return;

    // Clean up existing instance
    if (uplotRef.current) {
      uplotRef.current.destroy();
      uplotRef.current = null;
    }

    // Create tooltip element
    if (!tooltipRef.current) {
      tooltipRef.current = document.createElement("div");
      tooltipRef.current.className = "uplot-tooltip";
      containerRef.current.appendChild(tooltipRef.current);
    }

    const width = containerRef.current.clientWidth || 800;
    const height = 360;

    const opts = {
      width,
      height,
      series: seriesConfig,
      scales: {
        x: {
          time: false, // We're using Unix timestamps directly
          range: (u, min, max) => {
            const lockedX = xLockRef.current;
            if (
              lockedX &&
              Number.isFinite(lockedX.min) &&
              Number.isFinite(lockedX.max) &&
              lockedX.min !== lockedX.max
            ) {
              return [lockedX.min, lockedX.max];
            }
            // Use current view range if available
            const rMin = rangeStart ?? min;
            const rMax = rangeEnd ?? max;
            return [rMin, rMax];
          },
        },
        y: {
          auto: true,
          range: (u, min, max) => {
            const lockedY = yLockRef.current;
            if (
              lockedY &&
              Number.isFinite(lockedY.min) &&
              Number.isFinite(lockedY.max) &&
              lockedY.min !== lockedY.max
            ) {
              return [lockedY.min, lockedY.max];
            }
            if (min === max) {
              return [min - 1, max + 1];
            }
            const padding = (max - min) * 0.05;
            return [min - padding, max + padding];
          },
        },
      },
      axes: [
        {
          stroke: "#9ca3af",
          grid: { stroke: "#1f2937", width: 1 },
          ticks: { stroke: "#374151", width: 1 },
          values: (u, vals) =>
            vals.map((v) => {
              const date = new Date(v * 1000);
              return date.toISOString().substring(11, 19);
            }),
          font: "12px system-ui, sans-serif",
          gap: 8,
        },
        {
          stroke: "#9ca3af",
          grid: { stroke: "#1f2937", width: 1 },
          ticks: { stroke: "#374151", width: 1 },
          values: (u, vals) => vals.map((v) => (v != null ? v.toPrecision(4) : "")),
          font: "12px system-ui, sans-serif",
          size: 60,
          gap: 8,
        },
      ],
      cursor: {
        show: true,
        points: { show: false },
        drag: {
          x: interactionMode === "box",
          y: interactionMode === "box",
          uni: 20,
        },
        sync: { key: null },
      },
      legend: {
        show: true,
        live: true,
      },
      padding: [60, 20, 20, 10],
      hooks: {
        setSelect: [
          (u) => {
            if (interactionMode !== "box") return;
            const { left, width, top, height } = u.select;

            if (width > 0) {
              const xMin = u.posToVal(left, "x");
              const xMax = u.posToVal(left + width, "x");

              const minX = Math.min(xMin, xMax);
              const maxX = Math.max(xMin, xMax);

              const y1 = u.posToVal(top, "y");
              const y2 = u.posToVal(top + height, "y");
              const minY = Math.min(y1, y2);
              const maxY = Math.max(y1, y2);

              const canZoomX = Number.isFinite(minX) && Number.isFinite(maxX) && minX !== maxX;
              const canZoomY = height > 0 && Number.isFinite(minY) && Number.isFinite(maxY) && minY !== maxY;

              if (canZoomX || canZoomY) {
                isUpdatingScale.current = true;
                const applyScales = () => {
                  if (canZoomX) {
                    xLockRef.current = { min: minX, max: maxX };
                    u.setScale("x", { min: minX, max: maxX });
                  }
                  if (canZoomY) {
                    yLockRef.current = { min: minY, max: maxY };
                    u.setScale("y", { min: minY, max: maxY });
                  }
                };

                if (typeof u.batch === "function") {
                  u.batch(applyScales);
                } else {
                  applyScales();
                }
                isUpdatingScale.current = false;
                u.redraw?.();

                if (canZoomX) debouncedZoom(minX, maxX);
              }

              // Clear selection
              u.setSelect({ left: 0, top: 0, width: 0, height: 0 }, false);
            }
          },
        ],
        setScale: [
          (u, key) => {
            if (key !== "x" || isUpdatingScale.current) return;
            const { min, max } = u.scales.x;
            if (Number.isFinite(min) && Number.isFinite(max)) {
              debouncedZoom(min, max);
            }
          },
        ],
        setCursor: [
          (u) => {
            const { idx, left } = u.cursor;
            const tooltip = tooltipRef.current;
            if (!tooltip) return;

            if (idx == null || left == null || left < 0) {
              tooltip.style.display = "none";
              return;
            }

            const timestamp = u.data[0][idx];
            if (timestamp == null) {
              tooltip.style.display = "none";
              return;
            }

            const time = new Date(timestamp * 1000).toISOString();
            let html = `<div style="font-weight: 600; margin-bottom: 8px">${time}</div>`;

            u.series.forEach((s, i) => {
              if (i === 0 || !s.show) return;
              const val = u.data[i]?.[idx];
              const color = s._stroke || s.stroke;
              html += `
                <div style="display: flex; justify-content: space-between; gap: 16px; margin: 4px 0">
                  <span>
                    <span style="display: inline-block; width: 8px; height: 8px;
                      background: ${color}; border-radius: 50%; margin-right: 6px"></span>
                    ${s.label}
                  </span>
                  <span style="font-family: monospace">${val != null ? val.toFixed(6) : "N/A"}</span>
                </div>
              `;
            });

            tooltip.innerHTML = html;
            tooltip.style.display = "block";

            // Position tooltip
            const rect = containerRef.current.getBoundingClientRect();
            const tooltipRect = tooltip.getBoundingClientRect();

            let x = left + 15;
            let y = 80;

            if (x + tooltipRect.width > rect.width - 20) {
              x = left - tooltipRect.width - 15;
            }
            if (x < 10) x = 10;

            tooltip.style.left = x + "px";
            tooltip.style.top = y + "px";
          },
        ],
        ready: [
          (u) => {
            const over = u.root.querySelector(".u-over");
            if (!over) return;

            // Wheel zoom handler
            over.addEventListener("wheel", (e) => {
              e.preventDefault();

              const delta = e.deltaY;
              if (delta === 0) return;

              const factor = 0.75;
              const zoomIn = delta < 0;
              const zoomFactor = zoomIn ? factor : 1 / factor;

              const shiftHeld = shiftDownRef.current || e.shiftKey;
              const axis = shiftHeld ? "y" : "x";

              const rect = over.getBoundingClientRect();
              const cursorX = e.clientX - rect.left;
              const cursorY = e.clientY - rect.top;

              const scale = u.scales[axis];
              const curMin = scale.min;
              const curMax = scale.max;
              if (!Number.isFinite(curMin) || !Number.isFinite(curMax)) return;

              const range = curMax - curMin;
              const newRange = range * zoomFactor;

              // Get anchor point in data space
              const anchor = axis === "x"
                ? u.posToVal(cursorX, "x")
                : u.posToVal(cursorY, "y");

              if (!Number.isFinite(anchor)) return;

              const anchorRatio = (anchor - curMin) / range;
              let newMin = anchor - newRange * anchorRatio;
              let newMax = newMin + newRange;

              // Clamp to domain for X axis
              if (axis === "x") {
                const domainMin = xDomainRef.current.min ?? curMin;
                const domainMax = xDomainRef.current.max ?? curMax;
                const maxRange = domainMax - domainMin;

                if (newRange > maxRange) {
                  newMin = domainMin;
                  newMax = domainMax;
                } else {
                  if (newMin < domainMin) {
                    newMin = domainMin;
                    newMax = newMin + newRange;
                  }
                  if (newMax > domainMax) {
                    newMax = domainMax;
                    newMin = newMax - newRange;
                  }
                }
              }

              if (axis === "x") {
                xLockRef.current = { min: newMin, max: newMax };
              }
              if (axis === "y") {
                yLockRef.current = { min: newMin, max: newMax };
              }

              isUpdatingScale.current = true;
              u.setScale(axis, { min: newMin, max: newMax });
              isUpdatingScale.current = false;

              if (axis === "x") {
                debouncedZoom(newMin, newMax);
              }
            });

            // Pan handlers
            over.addEventListener("mousedown", (e) => {
              if (interactionMode !== "pan" || e.button !== 0) return;

              isPanning.current = true;
              panStart.current = {
                x: e.clientX,
                y: e.clientY,
                xMin: u.scales.x.min,
                xMax: u.scales.x.max,
                yMin: u.scales.y.min,
                yMax: u.scales.y.max,
              };
              over.style.cursor = "grabbing";
            });

            const handleMouseMove = (e) => {
              if (!isPanning.current || !uplotRef.current) return;

              const u = uplotRef.current;
              const rect = over.getBoundingClientRect();

              const shiftHeld = shiftDownRef.current || e.shiftKey;

              if (!shiftHeld) {
                // Pan X axis
                const dx = e.clientX - panStart.current.x;
                const pxPerUnit = rect.width / (panStart.current.xMax - panStart.current.xMin);
                const deltaUnits = -dx / pxPerUnit;

                let newMin = panStart.current.xMin + deltaUnits;
                let newMax = panStart.current.xMax + deltaUnits;

                // Clamp to domain
                const domainMin = xDomainRef.current.min ?? newMin;
                const domainMax = xDomainRef.current.max ?? newMax;
                const range = newMax - newMin;

                if (newMin < domainMin) {
                  newMin = domainMin;
                  newMax = newMin + range;
                }
                if (newMax > domainMax) {
                  newMax = domainMax;
                  newMin = newMax - range;
                }

                xLockRef.current = { min: newMin, max: newMax };

                isUpdatingScale.current = true;
                u.setScale("x", { min: newMin, max: newMax });
                isUpdatingScale.current = false;
              } else {
                // Pan Y axis
                const dy = e.clientY - panStart.current.y;
                const pxPerUnit = rect.height / (panStart.current.yMax - panStart.current.yMin);
                const deltaUnits = dy / pxPerUnit;

                const newMin = panStart.current.yMin + deltaUnits;
                const newMax = panStart.current.yMax + deltaUnits;

                yLockRef.current = { min: newMin, max: newMax };

                isUpdatingScale.current = true;
                u.setScale("y", { min: newMin, max: newMax });
                isUpdatingScale.current = false;
              }
            };

            const handleMouseUp = () => {
              if (isPanning.current && uplotRef.current) {
                isPanning.current = false;
                over.style.cursor = "";

                const { min, max } = uplotRef.current.scales.x;
                if (Number.isFinite(min) && Number.isFinite(max)) {
                  debouncedZoom(min, max);
                }
              }
            };

            document.addEventListener("mousemove", handleMouseMove);
            document.addEventListener("mouseup", handleMouseUp);

            // Store cleanup functions
            u._cleanupPan = () => {
              document.removeEventListener("mousemove", handleMouseMove);
              document.removeEventListener("mouseup", handleMouseUp);
            };
          },
        ],
        destroy: [
          (u) => {
            u._cleanupPan?.();
          },
        ],
      },
    };

    uplotRef.current = new uPlot(opts, uplotData, containerRef.current);

    // Resize observer
    const resizeObserver = new ResizeObserver(() => {
      if (uplotRef.current && containerRef.current) {
        uplotRef.current.setSize({
          width: containerRef.current.clientWidth,
          height: 360,
        });
      }
    });
    resizeObserver.observe(containerRef.current);

    return () => {
      resizeObserver.disconnect();
      if (uplotRef.current) {
        uplotRef.current.destroy();
        uplotRef.current = null;
      }
    };
  }, [seriesKey, interactionMode]);

  // Update data when it changes
  useEffect(() => {
    if (!uplotRef.current) return;
    uplotRef.current.setData(uplotData);
  }, [uplotData]);

  // Sync external range changes
  useEffect(() => {
    if (!uplotRef.current) return;
    if (!Number.isFinite(rangeStart) || !Number.isFinite(rangeEnd)) return;

    const current = uplotRef.current.scales.x;
    const tolerance = 1e-4;

    if (
      Math.abs((current.min ?? 0) - rangeStart) > tolerance ||
      Math.abs((current.max ?? 0) - rangeEnd) > tolerance
    ) {
      xLockRef.current = { min: rangeStart, max: rangeEnd };
      isUpdatingScale.current = true;
      uplotRef.current.setScale("x", { min: rangeStart, max: rangeEnd });
      isUpdatingScale.current = false;
    }
  }, [rangeStart, rangeEnd]);

  // Reset Y axis when resetToken changes
  useEffect(() => {
    if (!uplotRef.current || resetToken === undefined) return;

    xLockRef.current = null;
    yLockRef.current = null;

    // Force Y axis to auto-range by clearing and setting data
    const data = uplotRef.current.data;
    if (data) {
      uplotRef.current.setData(data);
    }
  }, [resetToken]);

  // Loading state
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
      <div ref={containerRef} className="w-full h-[360px]"></div>
    </div>
  );
};

window.Components = { Spinner, FileSelector, ColumnSelector, QuerySettings, StatsDisplay, Chart, PlotPanel };
