const path = require("path");
const http = require("http");
const { spawn } = require("child_process");
const { app, BrowserWindow, dialog, ipcMain } = require("electron");

const SERVER_URL = "http://localhost:8765";
const PROJECT_ROOT = path.join(__dirname, "..");
const SERVER_SCRIPT = path.join(PROJECT_ROOT, "server.py");
const DATA_FILE_FILTERS = [
  {
    name: "Data Files",
    extensions: ["parquet", "csv", "tsv", "json", "jsonl", "ndjson", "arrow", "feather", "orc", "avro"],
  },
  { name: "All Files", extensions: ["*"] },
];

let pythonProcess = null;
let isQuitting = false;

const waitForServer = (retries = 25, delayMs = 400) =>
  new Promise((resolve, reject) => {
    const attempt = (remaining) => {
      const req = http.get(`${SERVER_URL}/api/files`, (res) => {
        res.resume();
        resolve();
      });

      req.on("error", (err) => {
        if (remaining <= 0) {
          reject(err);
          return;
        }
        setTimeout(() => attempt(remaining - 1), delayMs);
      });
    };
    attempt(retries);
  });

const startPythonServer = async () => {
  if (pythonProcess) return;

  pythonProcess = spawn("python3", [SERVER_SCRIPT], {
    cwd: PROJECT_ROOT,
    env: { ...process.env, NO_BROWSER: "1" },
    stdio: "inherit",
  });

  pythonProcess.on("exit", (code, signal) => {
    if (!isQuitting && code && code !== 0) {
      console.error(`Python server exited with code ${code}${signal ? `, signal ${signal}` : ""}`);
    }
  });

  await waitForServer();
};

const stopPythonServer = () => {
  if (pythonProcess && !pythonProcess.killed) {
    pythonProcess.kill();
  }
  pythonProcess = null;
};

const createWindow = () => {
  const win = new BrowserWindow({
    width: 1400,
    height: 900,
    webPreferences: {
      contextIsolation: true,
      nodeIntegration: false,
      preload: path.join(__dirname, "preload.js"),
    },
  });

  win.loadURL(SERVER_URL);
};

const startApp = async () => {
  try {
    await startPythonServer();
    createWindow();
  } catch (err) {
    dialog.showErrorBox("Failed to start server", err?.message || String(err));
    app.quit();
  }
};

app.whenReady().then(startApp);

const showFilePicker = async () => {
  const result = await dialog.showOpenDialog({
    properties: ["openFile", "openDirectory", "multiSelections", "dontAddToRecent"],
    filters: DATA_FILE_FILTERS,
  });
  if (result.canceled) return [];
  return result.filePaths || [];
};

ipcMain.handle("select-parquet-paths", showFilePicker);
ipcMain.handle("select-data-paths", showFilePicker);

app.on("before-quit", () => {
  isQuitting = true;
  stopPythonServer();
});

app.on("window-all-closed", () => {
  if (process.platform !== "darwin") {
    app.quit();
  }
});

app.on("activate", () => {
  if (BrowserWindow.getAllWindows().length === 0) {
    createWindow();
  }
});
