const { useState, useEffect, useCallback, useRef, useMemo } = React;
const { FileSelector, ColumnSelector, QuerySettings, StatsDisplay, PlotPanel } = window.Components;

const App = () => {
  const [files, setFiles] = useState([]);
  const [selectedFile, setSelectedFile] = useState(null);
  const [columns, setColumns] = useState([]);
  const [timeColumn, setTimeColumn] = useState(null);
  const [timeRange, setTimeRange] = useState(null);
  const [currentRange, setCurrentRange] = useState(null);
  const [settings, setSettings] = useState({
    maxPoints: 4000,
    downsampleMethod: "lttb",
  });

  const plotCounterRef = useRef(1);
  const initialPlotIdRef = useRef(`plot-${plotCounterRef.current++}`);
  const [plots, setPlots] = useState([{ id: initialPlotIdRef.current, valueColumns: [] }]);
  const [layout, setLayout] = useState({ id: "layout-root", type: "plot", plotId: initialPlotIdRef.current });
  const [plotData, setPlotData] = useState({});
  const [plotStats, setPlotStats] = useState({});
  const [plotLoading, setPlotLoading] = useState({});

  const [loadingFiles, setLoadingFiles] = useState(true);
  const [loadingColumns, setLoadingColumns] = useState(false);

  const previousSelectionRef = useRef([]);
  const selectedColumnsRef = useRef([]);

  const allPlotColumns = useMemo(
    () => Array.from(new Set(plots.flatMap((p) => p.valueColumns))),
    [plots]
  );
  const hasColumns = allPlotColumns.length > 0;

  useEffect(() => {
    selectedColumnsRef.current = allPlotColumns;
  }, [allPlotColumns]);

  const resetPlots = useCallback(() => {
    const newPlotId = `plot-${plotCounterRef.current++}`;
    setPlots([{ id: newPlotId, valueColumns: [] }]);
    setLayout({ id: `layout-root-${newPlotId}`, type: "plot", plotId: newPlotId });
    setPlotData({});
    setPlotStats({});
    setPlotLoading({});
    return newPlotId;
  }, []);

  useEffect(() => {
    const loadFiles = async () => {
      setLoadingFiles(true);
      try {
        const files = await api.getFiles();
        setFiles(files);
      } catch (err) {
        console.error("Error loading files:", err);
      }
      setLoadingFiles(false);
    };
    loadFiles();
  }, []);

  useEffect(() => {
    if (!selectedFile) {
      setColumns([]);
      setTimeColumn(null);
      resetPlots();
      return;
    }

    const loadColumns = async () => {
      setLoadingColumns(true);
      try {
        const cols = await api.getColumns(selectedFile.name);
        setColumns(cols);
        resetPlots();

        const temporalCol = cols.find((c) => c.category === "temporal");
        const numericCol = cols.find((c) => c.category === "numeric");
        if (temporalCol) {
          setTimeColumn(temporalCol.name);
        } else if (numericCol) {
          setTimeColumn(numericCol.name);
        } else {
          setTimeColumn(null);
        }
      } catch (err) {
        console.error("Error loading columns:", err);
      }
      setLoadingColumns(false);
    };
    loadColumns();
  }, [selectedFile, resetPlots]);

  useEffect(() => {
    if (!selectedFile || !timeColumn) {
      setTimeRange(null);
      setCurrentRange(null);
      return;
    }

    const timeColMeta = columns.find((c) => c.name === timeColumn);
    if (!timeColMeta) {
      return;
    }

    const loadRange = async () => {
      try {
        const range = await api.getTimeRange(selectedFile.name, timeColumn, timeColMeta.type);
        if (selectedColumnsRef.current.length > 0) {
          return;
        }
        setTimeRange({
          min: range.min_epoch,
          max: range.max_epoch,
        });
        setCurrentRange({
          start: range.min_epoch,
          end: range.max_epoch,
        });
      } catch (err) {
        console.error("Error loading time range:", err);
      }
    };
    loadRange();
  }, [selectedFile, timeColumn, columns]);

  useEffect(() => {
    if (!selectedFile || !timeColumn) {
      previousSelectionRef.current = [];
      return;
    }

    const prevSelected = previousSelectionRef.current;
    previousSelectionRef.current = allPlotColumns;

    if (allPlotColumns.length === 0) {
      return;
    }

    const addedColumn = allPlotColumns.find((col) => !prevSelected.includes(col));
    if (!addedColumn) {
      return;
    }

    const timeColMeta = columns.find((c) => c.name === timeColumn);
    if (!timeColMeta) {
      return;
    }

    const updateRangeForColumn = async () => {
      try {
        const range = await api.getTimeRange(selectedFile.name, timeColumn, timeColMeta.type, [addedColumn]);
        if (range.min_epoch == null || range.max_epoch == null) {
          return;
        }

        const nextRange = {
          min: range.min_epoch,
          max: range.max_epoch,
        };

        setTimeRange(nextRange);

        if (prevSelected.length === 0) {
          setCurrentRange({
            start: nextRange.min,
            end: nextRange.max,
          });
        }
      } catch (err) {
        console.error("Error loading time range for column:", addedColumn, err);
      }
    };

    updateRangeForColumn();
  }, [allPlotColumns, selectedFile, timeColumn, columns]);

  const fetchPlotData = useCallback(
    async (plot) => {
      const hasTimeCol = columns.some((c) => c.name === timeColumn);
      if (!selectedFile || !timeColumn || !hasTimeCol || !currentRange || plot.valueColumns.length === 0) {
        return;
      }

      setPlotLoading((prev) => ({ ...prev, [plot.id]: true }));
      const queryStart = performance.now();

      try {
        const result = await api.queryData({
          file: selectedFile.name,
          time_column: timeColumn,
          value_columns: plot.valueColumns,
          start_time: currentRange.start,
          end_time: currentRange.end,
          max_points: settings.maxPoints,
          downsample_method: settings.downsampleMethod,
          columnsMeta: columns,
        });

        const queryTime = Math.round(performance.now() - queryStart);

        setPlotData((prev) => ({ ...prev, [plot.id]: result.data }));
        setPlotStats((prev) => ({
          ...prev,
          [plot.id]: {
            totalPoints: result.total_points,
            returnedPoints: result.returned_points,
            downsampled: result.downsampled,
            queryTime,
          },
        }));
      } catch (err) {
        console.error("Error querying data for plot", plot.id, err);
      } finally {
        setPlotLoading((prev) => ({ ...prev, [plot.id]: false }));
      }
    },
    [selectedFile, timeColumn, currentRange?.start, currentRange?.end, settings, columns]
  );

  useEffect(() => {
    if (!currentRange || !selectedFile || !timeColumn) return;

    plots.forEach((plot) => {
      if (plot.valueColumns.length === 0) {
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
  }, [plots, currentRange?.start, currentRange?.end, settings, selectedFile, timeColumn, fetchPlotData]);

  const handleZoom = useCallback((start, end) => {
    setCurrentRange({ start, end });
  }, []);

  const addColumnToPlot = useCallback((plotId, columnName) => {
    setPlots((prev) =>
      prev.map((plot) =>
        plot.id === plotId
          ? plot.valueColumns.includes(columnName)
            ? plot
            : { ...plot, valueColumns: [...plot.valueColumns, columnName] }
          : plot
      )
    );
  }, []);

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
    (plotId, columnName, zone) => {
      if (!selectedFile || !timeColumn) return;

      if (zone === "center") {
        addColumnToPlot(plotId, columnName);
        return;
      }

      const newPlotId = `plot-${plotCounterRef.current++}`;
      setPlots((prev) => [...prev, { id: newPlotId, valueColumns: [columnName] }]);
      setLayout((prev) => {
        const result = splitLayoutWithPlot(prev, plotId, zone, newPlotId);
        return result.applied && result.node ? result.node : prev;
      });
    },
    [addColumnToPlot, selectedFile, timeColumn, splitLayoutWithPlot]
  );

  const handleRemoveColumn = useCallback((plotId, columnName) => {
    setPlots((prev) =>
      prev.map((plot) =>
        plot.id === plotId ? { ...plot, valueColumns: plot.valueColumns.filter((c) => c !== columnName) } : plot
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
    (columnName) => {
      const target = plots[0];
      if (!target) return;
      handleVariableDrop(target.id, columnName, "center");
    },
    [plots, handleVariableDrop]
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
      return (
        <div key={node.id} className="flex-1 min-w-0">
          <PlotPanel
            title={plotTitleMap[plot.id] || "Plot"}
            plotId={plot.id}
            valueColumns={plot.valueColumns}
            data={plotData[plot.id] || {}}
            timeColumn={timeColumn}
            timeRange={currentRange}
            onZoom={handleZoom}
            loading={plotLoading[plot.id]}
            onDropVariable={(col, zone) => handleVariableDrop(plot.id, col, zone)}
            onRemoveColumn={(col) => handleRemoveColumn(plot.id, col)}
            onRemovePlot={() => handleRemovePlot(plot.id)}
            canRemovePlot={plots.length > 1}
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
                <h1 className="text-xl font-bold text-white">Parquet Viewer</h1>
                <p className="text-xs text-gray-400">Powered by DuckDB</p>
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
            <FileSelector files={files} selectedFile={selectedFile} onSelect={setSelectedFile} loading={loadingFiles} />

            <ColumnSelector
              columns={columns}
              timeColumn={timeColumn}
              onTimeColumnChange={setTimeColumn}
              activeColumns={allPlotColumns}
              onColumnAdd={handleQuickAdd}
              loading={loadingColumns}
            />

            <QuerySettings settings={settings} onChange={setSettings} />

            <StatsDisplay stats={combinedStats} />
          </aside>

          <div className="col-span-9 space-y-4">
            {selectedFile && timeColumn ? (
              renderLayout(layout)
            ) : (
              <div className="bg-gray-800 rounded-lg shadow-lg h-[420px] flex items-center justify-center">
                <div className="text-center text-gray-400">
                  <svg className="w-16 h-16 mx-auto mb-4 opacity-50" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M11 3.055A9.001 9.001 0 1020.945 13H11V3.055z" />
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M20.488 9H15V3.512A9.025 9.025 0 0120.488 9z" />
                  </svg>
                  <p className="text-lg font-medium mb-2">No Data to Display</p>
                  <p className="text-sm">
                    {!selectedFile
                      ? "Select a parquet file to begin"
                      : !timeColumn
                      ? "Select a time column for the X-axis"
                      : "Drag a variable onto the plot area to start"}
                  </p>
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
            <span>Parquet Viewer v1.0.0</span>
            <span>Extensible data visualization for large datasets</span>
          </div>
        </div>
      </footer>
    </div>
  );
};

const root = ReactDOM.createRoot(document.getElementById("root"));
root.render(<App />);
