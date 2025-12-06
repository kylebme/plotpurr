const { useState, useEffect, useCallback, useRef } = React;
const { formatNumber } = window.Utils;
const { Spinner, FileSelector, ColumnSelector, QuerySettings, StatsDisplay, Chart } = window.Components;

const App = () => {
  const [files, setFiles] = useState([]);
  const [selectedFile, setSelectedFile] = useState(null);
  const [columns, setColumns] = useState([]);
  const [timeColumn, setTimeColumn] = useState(null);
  const [selectedColumns, setSelectedColumns] = useState([]);
  const [timeRange, setTimeRange] = useState(null);
  const [currentRange, setCurrentRange] = useState(null);
  const [data, setData] = useState({});
  const [stats, setStats] = useState({});
  const [settings, setSettings] = useState({
    maxPoints: 2000,
    downsampleMethod: "lttb",
  });

  const [loadingFiles, setLoadingFiles] = useState(true);
  const [loadingColumns, setLoadingColumns] = useState(false);
  const [loadingData, setLoadingData] = useState(false);
  const previousSelectionRef = useRef([]);
  const selectedColumnsRef = useRef([]);

  useEffect(() => {
    selectedColumnsRef.current = selectedColumns;
  }, [selectedColumns]);

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
      setSelectedColumns([]);
      return;
    }

    const loadColumns = async () => {
      setLoadingColumns(true);
      try {
        const cols = await api.getColumns(selectedFile.name);
        setColumns(cols);

        const temporalCol = cols.find((c) => c.category === "temporal");
        if (temporalCol) {
          setTimeColumn(temporalCol.name);
        }
      } catch (err) {
        console.error("Error loading columns:", err);
      }
      setLoadingColumns(false);
    };
    loadColumns();
  }, [selectedFile]);

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
    previousSelectionRef.current = selectedColumns;

    if (selectedColumns.length === 0) {
      return;
    }

    const addedColumn = selectedColumns.find((col) => !prevSelected.includes(col));
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
  }, [selectedColumns, selectedFile, timeColumn, columns]);

  const queryData = useCallback(
    async (startTime, endTime) => {
      if (!selectedFile || !timeColumn || selectedColumns.length === 0) {
        return;
      }

      setLoadingData(true);
      const queryStart = performance.now();

      try {
        const result = await api.queryData({
          file: selectedFile.name,
          time_column: timeColumn,
          value_columns: selectedColumns,
          start_time: startTime,
          end_time: endTime,
          max_points: settings.maxPoints,
          downsample_method: settings.downsampleMethod,
          columnsMeta: columns,
        });

        const queryTime = Math.round(performance.now() - queryStart);

        setData(result.data);
        setStats({
          totalPoints: result.total_points,
          returnedPoints: result.returned_points,
          downsampled: result.downsampled,
          queryTime,
        });
      } catch (err) {
        console.error("Error querying data:", err);
      }

      setLoadingData(false);
    },
    [selectedFile, timeColumn, selectedColumns, settings, columns]
  );

  useEffect(() => {
    if (currentRange) {
      queryData(currentRange.start, currentRange.end);
    }
  }, [selectedColumns, currentRange?.start, currentRange?.end, settings]);

  const handleZoom = useCallback((start, end) => {
    setCurrentRange({ start, end });
  }, []);

  const handleColumnToggle = (columnName) => {
    setSelectedColumns((prev) => {
      if (prev.includes(columnName)) {
        return prev.filter((c) => c !== columnName);
      }
      return [...prev, columnName];
    });
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
              selectedColumns={selectedColumns}
              onTimeColumnChange={setTimeColumn}
              onColumnToggle={handleColumnToggle}
              loading={loadingColumns}
            />

            <QuerySettings settings={settings} onChange={setSettings} />

            <StatsDisplay stats={stats} />
          </aside>

          <div className="col-span-9">
            {selectedFile && timeColumn && selectedColumns.length > 0 ? (
              <Chart
                data={data}
                timeColumn={timeColumn}
                valueColumns={selectedColumns}
                onZoom={handleZoom}
                loading={loadingData}
                timeRange={currentRange}
              />
            ) : (
              <div className="bg-gray-800 rounded-lg shadow-lg h-[500px] flex items-center justify-center">
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
                      : "Select one or more variables to plot"}
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
