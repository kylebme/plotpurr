const { contextBridge, ipcRenderer } = require("electron");

contextBridge.exposeInMainWorld("electronAPI", {
  selectParquetPaths: () => ipcRenderer.invoke("select-parquet-paths"),
  selectDataPaths: () => ipcRenderer.invoke("select-data-paths"),
});
