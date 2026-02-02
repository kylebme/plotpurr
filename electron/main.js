// Copyright (C) 2025 Kyle Bartholomew

// This file is part of PlotPurr.

// PlotPurr is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

// PlotPurr is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
// You should have received a copy of the GNU General Public License along with PlotPurr. If not, see <https://www.gnu.org/licenses/>. 

const fs = require("fs");
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

const getVenvPython = () => {
  if (process.platform === "win32") {
    return path.join(PROJECT_ROOT, ".venv", "Scripts", "python.exe");
  }

  const pythonPath = path.join(PROJECT_ROOT, ".venv", "bin", "python");
  if (fs.existsSync(pythonPath)) {
    return pythonPath;
  }

  return path.join(PROJECT_ROOT, ".venv", "bin", "python3");
};

const resolvePython = () => {
  const envPython = process.env.PLOTPURR_PYTHON;
  if (envPython) return envPython;

  const venvPython = getVenvPython();
  if (fs.existsSync(venvPython)) return venvPython;

  return process.platform === "win32" ? "python" : "python3";
};

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

  pythonProcess = spawn(resolvePython(), [SERVER_SCRIPT], {
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
    icon: path.join(PROJECT_ROOT, "public", "PlotPurr.png"),
    webPreferences: {
      contextIsolation: true,
      nodeIntegration: false,
      preload: path.join(__dirname, "preload.js"),
    },
  });

  const url = new URL(SERVER_URL);
  url.searchParams.set("v", Date.now().toString());
  win.webContents.session
    .clearCache()
    .catch((err) => console.warn("Failed to clear cache", err))
    .finally(() => {
      win.loadURL(url.toString());
    });
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

app.setName("PlotPurr");
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
