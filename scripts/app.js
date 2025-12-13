const { useState, useEffect, useCallback, useRef, useMemo } = React;
const { FileSelector, ColumnSelector, QuerySettings, StatsDisplay, PlotPanel } = window.Components;
const { COLORS } = window.Utils;

const getFileKey = (file) => {
  if (!file) return "";
  if (typeof file === "string") return file;
  return file.id || file.path || file.name || "";
};

const App = () => {
  const [files, setFiles] = useState([]);
  const [selectedFile, setSelectedFile] = useState(null);
  const [columnsByFile, setColumnsByFile] = useState({});
  const [timeColumnsByFile, setTimeColumnsByFile] = useState({});
  const [timeRange, setTimeRange] = useState(null);
  const [currentRange, setCurrentRange] = useState(null);
  const [settings, setSettings] = useState({
    maxPoints: 10000,
    downsampleMethod: "minmax",
    showTooltip: true,
  });
  const [fileFormats, setFileFormats] = useState({});

  const plotCounterRef = useRef(1);
  const seriesCounterRef = useRef(1);
  const colorMapRef = useRef({});
  const initialPlotIdRef = useRef(`plot-${plotCounterRef.current++}`);
  const [plots, setPlots] = useState([{ id: initialPlotIdRef.current, series: [] }]);
  const [layout, setLayout] = useState({ id: "layout-root", type: "plot", plotId: initialPlotIdRef.current });
  const [plotData, setPlotData] = useState({});
  const [plotStats, setPlotStats] = useState({});
  const [plotLoading, setPlotLoading] = useState({});
  const [resetToken, setResetToken] = useState(0);

  const [loadingFiles, setLoadingFiles] = useState(true);
  const [loadingColumns, setLoadingColumns] = useState(false);
  const browseFn = window.electronAPI?.selectDataPaths || window.electronAPI?.selectParquetPaths;
  const canBrowse = !!browseFn;

  const selectedFileKey = getFileKey(selectedFile);
  const activeColumns = selectedFileKey ? columnsByFile[selectedFileKey] || [] : [];
  const activeTimeColumn = selectedFileKey ? timeColumnsByFile[selectedFileKey] || "" : "";

  const allSeries = useMemo(() => plots.flatMap((p) => p.series), [plots]);
  const activeColumnsForFile = useMemo(() => {
    if (!selectedFileKey) return [];
    const set = new Set();
    plots.forEach((p) => {
      p.series.forEach((s) => {
        if (s.file === selectedFileKey) {
          set.add(s.column);
        }
      });
    });
    return Array.from(set);
  }, [plots, selectedFileKey]);

  const resetPlotsState = useCallback(() => {
    const newPlotId = `plot-${plotCounterRef.current++}`;
    setPlots([{ id: newPlotId, series: [] }]);
    setLayout({ id: "layout-root", type: "plot", plotId: newPlotId });
    setPlotData({});
    setPlotStats({});
    setPlotLoading({});
    setFileFormats({});
    setTimeRange(null);
    setCurrentRange(null);
    setSelectedFile(null);
    setColumnsByFile({});
    setTimeColumnsByFile({});
    setResetToken((t) => t + 1);
  }, []);

  const loadFiles = useCallback(async () => {
    setLoadingFiles(true);
    try {
      const fetched = await api.getFiles();
      const normalized = (fetched || []).map((f) => ({
        ...f,
        id: f.path || f.name,
      }));
      setFiles(normalized);
      setFileFormats(
        normalized.reduce((acc, f) => {
          const key = getFileKey(f);
          if (key) acc[key] = f.format || null;
          return acc;
        }, {})
      );
    } catch (err) {
      console.error("Error loading files:", err);
    }
    setLoadingFiles(false);
  }, []);

  const handleBrowseForData = useCallback(async () => {
    const selector = window.electronAPI?.selectDataPaths || window.electronAPI?.selectParquetPaths;
    if (!selector) {
      console.warn("Browse requires the Electron shell.");
      return;
    }
    const paths = await selector();
    if (!paths || !paths.length) return;

    setLoadingFiles(true);
    resetPlotsState();
    try {
      await api.setPaths(paths);
      await loadFiles();
    } catch (err) {
      console.error("Error applying selected paths:", err);
    }
    setLoadingFiles(false);
  }, [loadFiles, resetPlotsState]);

  useEffect(() => {
    if (canBrowse) {
      setLoadingFiles(false);
      return;
    }
    loadFiles();
  }, [canBrowse, loadFiles]);

  const getFileFormat = useCallback(
    (file) => {
      const key = getFileKey(file);
      if (!key) return null;
      if (file?.format) return file.format;
      return fileFormats[key] || null;
    },
    [fileFormats]
  );

  const loadColumnsForFile = useCallback(
    async (file) => {
      if (!file) return;
      const fileKey = getFileKey(file);
      if (columnsByFile[fileKey]) {
        setLoadingColumns(false);
        return;
      }
      setLoadingColumns(true);
      try {
        const cols = await api.getColumns(fileKey, getFileFormat(file));
        setColumnsByFile((prev) => ({ ...prev, [fileKey]: cols }));
        setTimeColumnsByFile((prev) => {
          if (prev[fileKey]) return prev;
          const temporalCol = cols.find((c) => c.category === "temporal");
          const numericCol = cols.find((c) => c.category === "numeric");
          return { ...prev, [fileKey]: temporalCol?.name || numericCol?.name || "" };
        });
      } catch (err) {
        console.error("Error loading columns:", err);
      }
      setLoadingColumns(false);
    },
    [columnsByFile, getFileFormat]
  );

  useEffect(() => {
    if (!selectedFile) {
      setLoadingColumns(false);
      return;
    }
    loadColumnsForFile(selectedFile);
  }, [selectedFile, loadColumnsForFile]);

  useEffect(() => {
    if (allSeries.length === 0) {
      setTimeRange(null);
      setCurrentRange(null);
    }
  }, [allSeries.length]);

  const getSeriesColor = useCallback((series) => {
    const key = `${series.file}::${series.column}`;
    if (colorMapRef.current[key] === undefined) {
      const nextIndex = Object.keys(colorMapRef.current).length % COLORS.length;
      colorMapRef.current[key] = nextIndex;
    }
    const idx = colorMapRef.current[key];
    return COLORS[idx % COLORS.length];
  }, []);

  const updateRangeForSeries = useCallback(
    async (series) => {
      const cols = columnsByFile[series.file];
      if (!cols) return;
      const timeColMeta = cols.find((c) => c.name === series.timeColumn);
      if (!timeColMeta) return;
      try {
        const range = await api.getTimeRange(
          series.file,
          series.timeColumn,
          timeColMeta.type,
          [series.column],
          series.format || fileFormats[series.file]
        );
        if (range.min_epoch == null || range.max_epoch == null) return;
        const nextRange = { min: range.min_epoch, max: range.max_epoch };
        setTimeRange((prev) => {
          if (!prev) return nextRange;
          return { min: Math.min(prev.min, nextRange.min), max: Math.max(prev.max, nextRange.max) };
        });
        setCurrentRange((prev) => {
          if (!prev) return { start: nextRange.min, end: nextRange.max };
          return { start: Math.min(prev.start, nextRange.min), end: Math.max(prev.end, nextRange.max) };
        });
      } catch (err) {
        console.error("Error loading time range for series", series, err);
      }
    },
    [columnsByFile, fileFormats]
  );

  const fetchPlotData = useCallback(
    async (plot) => {
      if (!currentRange || !plot.series.length) {
        return;
      }

      setPlotLoading((prev) => ({ ...prev, [plot.id]: true }));
      const queryStart = performance.now();

      try {
        const results = await Promise.all(
          plot.series.map(async (series) => {
            const columnsMeta = columnsByFile[series.file];
            if (!columnsMeta) return null;
            const result = await api.queryData({
              file: series.file,
              time_column: series.timeColumn,
              value_columns: [series.column],
              start_time: currentRange.start,
              end_time: currentRange.end,
              max_points: settings.maxPoints,
              downsample_method: settings.downsampleMethod,
              columnsMeta,
              format: series.format || fileFormats[series.file],
            });
            const timeData = result.data?.[series.timeColumn] || [];
            const valueData = result.data?.[series.column] || [];
            const pairs = timeData.map((t, idx) => [t, valueData[idx]]);
            return {
              ...series,
              name: `${series.fileName || series.file} • ${series.column}`,
              data: pairs,
              totalPoints: result.total_points,
              returnedPoints: result.returned_points,
              downsampled: result.downsampled,
            };
          })
        );

        const filtered = results.filter(Boolean);
        const queryTime = Math.round(performance.now() - queryStart);

        setPlotData((prev) => ({ ...prev, [plot.id]: filtered }));
        setPlotStats((prev) => ({
          ...prev,
          [plot.id]: {
            totalPoints: filtered.reduce((sum, s) => sum + (s?.totalPoints || 0), 0),
            returnedPoints: filtered.reduce((sum, s) => sum + (s?.returnedPoints || 0), 0),
            downsampled: filtered.some((s) => s?.downsampled),
            queryTime,
          },
        }));
      } catch (err) {
        console.error("Error querying data for plot", plot.id, err);
      } finally {
        setPlotLoading((prev) => ({ ...prev, [plot.id]: false }));
      }
    },
    [
      currentRange?.start,
      currentRange?.end,
      settings.maxPoints,
      settings.downsampleMethod,
      columnsByFile,
      fileFormats,
    ]
  );

  useEffect(() => {
    if (!currentRange) return;

    plots.forEach((plot) => {
      if (plot.series.length === 0) {
        setPlotData((prev) => {
          const next = { ...prev };
          delete next[plot.id];
          return next;
        });
        setPlotStats((prev) => {
          const next = { ...prev };
          delete next[plot.id];
          return next;
        });
        return;
      }
      fetchPlotData(plot);
    });
  }, [plots, currentRange?.start, currentRange?.end, fetchPlotData]);

  const handleZoom = useCallback((start, end) => {
    if (!Number.isFinite(start) || !Number.isFinite(end)) return;
    const nextStart = Math.min(start, end);
    const nextEnd = Math.max(start, end);
    if (nextStart === nextEnd) return;

    setCurrentRange((prev) => {
      const tolerance = 1e-4;
      if (prev && Math.abs(prev.start - nextStart) < tolerance && Math.abs(prev.end - nextEnd) < tolerance) {
        return prev;
      }
      return { start: nextStart, end: nextEnd };
    });
  }, []);

  const addSeriesToPlot = useCallback(
    (plotId, payload) => {
      if (!payload?.file || !payload?.column) return;
      const timeCol = timeColumnsByFile[payload.file];
      if (!timeCol) return;
      const newSeries = {
        id: `series-${seriesCounterRef.current++}`,
        file: payload.file,
        fileName: payload.fileName,
        column: payload.column,
        timeColumn: timeCol,
        format: fileFormats[payload.file] || null,
      };

      setPlots((prev) =>
        prev.map((plot) =>
          plot.id === plotId
            ? plot.series.some((s) => s.file === payload.file && s.column === payload.column && s.timeColumn === timeCol)
              ? plot
              : { ...plot, series: [...plot.series, newSeries] }
            : plot
        )
      );
      updateRangeForSeries(newSeries);
    },
    [timeColumnsByFile, updateRangeForSeries, fileFormats]
  );

  const splitLayoutWithPlot = useCallback((node, targetPlotId, zone, newPlotId) => {
    if (!node) return { node: null, applied: false };
    if (node.type === "plot") {
      if (node.plotId !== targetPlotId) return { node, applied: false };
      const direction = zone === "left" || zone === "right" ? "row" : "column";
      const newNode = { id: `layout-${newPlotId}`, type: "plot", plotId: newPlotId };
      const existingNode = { ...node };
      const first = zone === "left" || zone === "top" ? newNode : existingNode;
      const second = zone === "left" || zone === "top" ? existingNode : newNode;
      return {
        node: {
          id: `split-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
          type: "split",
          direction,
          first,
          second,
        },
        applied: true,
      };
    }
    const left = splitLayoutWithPlot(node.first, targetPlotId, zone, newPlotId);
    if (left.applied) {
      return { node: { ...node, first: left.node }, applied: true };
    }
    const right = splitLayoutWithPlot(node.second, targetPlotId, zone, newPlotId);
    return { node: { ...node, second: right.node }, applied: right.applied };
  }, []);

  const removePlotFromLayout = useCallback((node, targetPlotId) => {
    if (!node) return { node: null, removed: false };
    if (node.type === "plot") {
      if (node.plotId === targetPlotId) return { node: null, removed: true };
      return { node, removed: false };
    }

    const left = removePlotFromLayout(node.first, targetPlotId);
    const right = removePlotFromLayout(node.second, targetPlotId);

    if (!left.node && !right.node) return { node: null, removed: left.removed || right.removed };
    if (!left.node) return { node: right.node, removed: true };
    if (!right.node) return { node: left.node, removed: true };

    return { node: { ...node, first: left.node, second: right.node }, removed: left.removed || right.removed };
  }, []);

  const handleVariableDrop = useCallback(
    (plotId, payload, zone) => {
      if (!payload?.file || !payload?.column) return;
      const timeCol = timeColumnsByFile[payload.file];
      if (!timeCol) return;

      if (zone === "center") {
        addSeriesToPlot(plotId, payload);
        return;
      }

      const newPlotId = `plot-${plotCounterRef.current++}`;
      const newSeriesId = `series-${seriesCounterRef.current++}`;
      setPlots((prev) => [
        ...prev,
        {
          id: newPlotId,
          series: [
            {
              id: newSeriesId,
              file: payload.file,
              fileName: payload.fileName,
              column: payload.column,
              timeColumn: timeCol,
              format: fileFormats[payload.file] || null,
            },
          ],
        },
      ]);
      setLayout((prev) => {
        const result = splitLayoutWithPlot(prev, plotId, zone, newPlotId);
        return result.applied && result.node ? result.node : prev;
      });
      updateRangeForSeries({
        id: newSeriesId,
        file: payload.file,
        fileName: payload.fileName,
        column: payload.column,
        timeColumn: timeCol,
      });
    },
    [addSeriesToPlot, splitLayoutWithPlot, timeColumnsByFile, updateRangeForSeries, fileFormats]
  );

  const handleRemoveColumn = useCallback((plotId, seriesId) => {
    setPlots((prev) =>
      prev.map((plot) =>
        plot.id === plotId ? { ...plot, series: plot.series.filter((s) => s.id !== seriesId) } : plot
      )
    );
  }, []);

  const handleRemovePlot = useCallback(
    (plotId) => {
      if (plots.length <= 1) return;

      setPlots((prev) => prev.filter((p) => p.id !== plotId));
      setLayout((prev) => {
        const result = removePlotFromLayout(prev, plotId);
        return result.node || prev;
      });
      setPlotData((prev) => {
        const next = { ...prev };
        delete next[plotId];
        return next;
      });
      setPlotStats((prev) => {
        const next = { ...prev };
        delete next[plotId];
        return next;
      });
      setPlotLoading((prev) => {
        const next = { ...prev };
        delete next[plotId];
        return next;
      });
    },
    [plots.length, removePlotFromLayout]
  );

  const handleQuickAdd = useCallback(
    (payload) => {
      if (!payload?.file || !payload?.column) return;
      const target = plots[0];
      if (!target) return;
      handleVariableDrop(target.id, payload, "center");
    },
    [plots, handleVariableDrop]
  );

  const handleTimeColumnChange = useCallback(
    (newTimeColumn) => {
      if (!selectedFile) return;
      const fileKey = getFileKey(selectedFile);
      setTimeColumnsByFile((prev) => ({ ...prev, [fileKey]: newTimeColumn }));
      setPlots((prev) =>
        prev.map((plot) => ({
          ...plot,
          series: plot.series.map((s) => (s.file === fileKey ? { ...s, timeColumn: newTimeColumn } : s)),
        }))
      );
      plots.forEach((plot) => {
        plot.series
          .filter((s) => s.file === fileKey)
          .forEach((s) => updateRangeForSeries({ ...s, timeColumn: newTimeColumn }));
      });
    },
    [selectedFile, plots, updateRangeForSeries]
  );

  const combinedStats = useMemo(() => {
    const entries = Object.values(plotStats || {});
    if (!entries.length) return {};
    return {
      totalPoints: entries.reduce((sum, s) => sum + (s.totalPoints || 0), 0),
      returnedPoints: entries.reduce((sum, s) => sum + (s.returnedPoints || 0), 0),
      downsampled: entries.some((s) => s.downsampled),
      queryTime: entries.reduce((sum, s) => sum + (s.queryTime || 0), 0),
    };
  }, [plotStats]);

  const plotTitleMap = useMemo(() => {
    const map = {};
    plots.forEach((p, idx) => {
      map[p.id] = `Plot ${idx + 1}`;
    });
    return map;
  }, [plots]);

  const renderLayout = (node) => {
    if (!node) return null;
    if (node.type === "plot") {
      const plot = plots.find((p) => p.id === node.plotId);
      if (!plot) return null;
      const seriesData =
        plotData[plot.id] && plotData[plot.id].length
          ? plotData[plot.id]
          : plot.series.map((s) => ({
              ...s,
              name: `${s.fileName || s.file} • ${s.column}`,
              data: [],
            }));
      return (
        <div key={node.id} className="flex-1 min-w-0">
          <PlotPanel
            title={plotTitleMap[plot.id] || "Plot"}
            series={seriesData}
            viewRange={currentRange || timeRange}
            fullTimeRange={timeRange}
            onZoom={handleZoom}
	            resetToken={resetToken}
            loading={plotLoading[plot.id]}
            onDropVariable={(col, zone) => handleVariableDrop(plot.id, col, zone)}
            onRemoveSeries={(seriesId) => handleRemoveColumn(plot.id, seriesId)}
            onRemovePlot={() => handleRemovePlot(plot.id)}
            canRemovePlot={plots.length > 1}
            getColor={getSeriesColor}
            showTooltip={settings.showTooltip !== false}
          />
        </div>
      );
    }

    const isRow = node.direction === "row";
    return (
      <div key={node.id} className={`flex ${isRow ? "flex-row" : "flex-col"} gap-4 flex-1 min-h-[360px]`}>
        {renderLayout(node.first)}
        {renderLayout(node.second)}
      </div>
    );
  };

  const handleResetZoom = () => {
    if (timeRange) {
      setCurrentRange({
        start: timeRange.min,
        end: timeRange.max,
      });
      setResetToken((t) => t + 1);
    }
  };

  return (
    <div className="min-h-screen flex flex-col">
      <header className="bg-gray-800 shadow-lg border-b border-gray-700">
        <div className="max-w-screen-2xl mx-auto px-4 py-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <svg className="w-8 h-8 text-blue-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M7 12l3-3 3 3 4-4M8 21l4-4 4 4M3 4h18M4 4h16v12a1 1 0 01-1 1H5a1 1 0 01-1-1V4z"
                />
              </svg>
              <div>
                <h1 className="text-xl font-bold text-white">Data File Viewer</h1>
                <p className="text-xs text-gray-400">Powered by ChDB (ClickHouse)</p>
              </div>
            </div>

            <div className="flex items-center gap-4">
              <button
                onClick={handleResetZoom}
                disabled={!timeRange}
                className="px-4 py-2 bg-gray-700 hover:bg-gray-600 rounded-lg text-sm font-medium transition-colors disabled:opacity-50"
              >
                Reset Zoom
              </button>
            </div>
          </div>
        </div>
      </header>

      <main className="flex-1 max-w-screen-2xl mx-auto w-full px-4 py-6">
        <div className="grid grid-cols-12 gap-6">
          <aside className="col-span-3 space-y-6">
            <FileSelector
              files={files}
              selectedFile={selectedFile}
              onSelect={setSelectedFile}
              loading={loadingFiles}
              onBrowse={canBrowse ? handleBrowseForData : null}
            />

            <ColumnSelector
              file={selectedFile}
              columns={activeColumns}
              timeColumn={activeTimeColumn}
              onTimeColumnChange={handleTimeColumnChange}
              activeColumns={activeColumnsForFile}
              onColumnAdd={handleQuickAdd}
              loading={loadingColumns}
            />

            <QuerySettings settings={settings} onChange={setSettings} />

            <StatsDisplay stats={combinedStats} />
          </aside>

          <div className="col-span-9 space-y-4">
            {selectedFile ? (
              renderLayout(layout)
            ) : (
              <div className="bg-gray-800 rounded-lg shadow-lg h-[420px] flex items-center justify-center">
                <div className="text-center text-gray-400">
                  <svg className="w-16 h-16 mx-auto mb-4 opacity-50" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M11 3.055A9.001 9.001 0 1020.945 13H11V3.055z" />
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M20.488 9H15V3.512A9.025 9.025 0 0120.488 9z" />
                  </svg>
                  <p className="text-lg font-medium mb-2">No Data to Display</p>
                  <p className="text-sm">Select a data file, then drag a variable onto the plot area to start</p>
                </div>
              </div>
            )}

            {currentRange && (
              <div className="mt-4 bg-gray-800 rounded-lg p-4 shadow-lg">
                <div className="flex items-center justify-between text-sm">
                  <div>
                    <span className="text-gray-400">Current Range: </span>
                    <span className="font-mono">{new Date(currentRange.start * 1000).toISOString()}</span>
                    <span className="text-gray-500 mx-2">→</span>
                    <span className="font-mono">{new Date(currentRange.end * 1000).toISOString()}</span>
                  </div>
                  <div className="text-gray-400">
                    Duration: {((currentRange.end - currentRange.start) / 60).toFixed(2)} min
                  </div>
                </div>
              </div>
            )}
          </div>
        </div>
      </main>

      <footer className="bg-gray-800 border-t border-gray-700 py-3">
        <div className="max-w-screen-2xl mx-auto px-4">
          <div className="flex items-center justify-between text-sm text-gray-400">
            <span>Data File Viewer v1.0.0</span>
            <span>Extensible data visualization for large datasets</span>
          </div>
        </div>
      </footer>
    </div>
  );
};

const root = ReactDOM.createRoot(document.getElementById("root"));
root.render(<App />);
