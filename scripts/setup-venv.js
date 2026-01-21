#!/usr/bin/env node
"use strict";

const { spawnSync } = require("child_process");
const fs = require("fs");
const path = require("path");

const projectRoot = path.join(__dirname, "..");
const venvDir = path.join(projectRoot, ".venv");
const requirementsPath = path.join(projectRoot, "requirements.txt");

const getVenvPython = () => {
  if (process.platform === "win32") {
    return path.join(venvDir, "Scripts", "python.exe");
  }

  const pythonPath = path.join(venvDir, "bin", "python");
  if (fs.existsSync(pythonPath)) {
    return pythonPath;
  }

  return path.join(venvDir, "bin", "python3");
};

const run = (cmd, args, options = {}) => {
  const result = spawnSync(cmd, args, { stdio: "inherit", ...options });
  if (result.error) {
    console.error(`Failed to run ${cmd}: ${result.error.message}`);
  }
  return result;
};

const canRun = (cmd) => {
  const result = spawnSync(cmd, ["-c", "import sys; print(sys.executable)"], {
    stdio: "ignore",
  });
  return result.status === 0;
};

const pickPython = () => {
  const candidates = [
    process.env.PLOTPURR_BOOTSTRAP_PYTHON,
    process.env.PLOTPURR_PYTHON,
    "python3",
    "python",
  ].filter(Boolean);

  for (const candidate of candidates) {
    if (canRun(candidate)) {
      return candidate;
    }
  }

  return null;
};

const main = () => {
  if (!fs.existsSync(requirementsPath)) {
    console.error(`Missing ${requirementsPath}`);
    process.exit(1);
  }

  const pythonCmd = pickPython();
  if (!pythonCmd) {
    console.error(
      "Python not found. Set PLOTPURR_BOOTSTRAP_PYTHON or PLOTPURR_PYTHON to a valid interpreter."
    );
    process.exit(1);
  }

  const venvPython = getVenvPython();
  if (!fs.existsSync(venvPython)) {
    console.log("Creating local virtual environment...");
    const createResult = run(pythonCmd, ["-m", "venv", venvDir]);
    if (createResult.status !== 0) {
      process.exit(createResult.status || 1);
    }
  } else {
    console.log("Using existing virtual environment.");
  }

  console.log("Installing Python dependencies...");
  const installResult = run(venvPython, ["-m", "pip", "install", "-r", requirementsPath]);
  if (installResult.status !== 0) {
    process.exit(installResult.status || 1);
  }
};

main();
