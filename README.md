# PlotPurr

<p align="center"><img src="PlotPurr.png" width=50%></p>

An experimental plotting playground designed for very large timeseries datasets.
PlotPurr downsamples the input data using Clickhouse and dynamically fetches new data as you zoom in.

## Demo

![PlotPurr Screencap](screencap.gif)

## Installation

1. Make sure you have a recent version of Node.js installed.
2. Make sure you have Python 3 + pip available (used to create a local venv in `.venv`).
3. Clone the repo.
4. Install dependencies:
   ```bash
   npm install
   ```
5. Start the app:
   ```bash
   npm start
   ```

If the venv setup fails (for example, your Python executable is not named `python3`), run:

```bash
PLOTPURR_BOOTSTRAP_PYTHON=/path/to/python npm run setup
```

To force PlotPurr to use a specific Python at runtime:

```bash
PLOTPURR_PYTHON=/path/to/python npm start
```

## Motivation

Existing plotting GUI tools (PlotJuggler, Rerun) are excellent, but they focus on datasets that can fit in RAM, since they load the entire dataset. Additionally, there is a large set of tooling for dealing with large datasets (Clickhouse, DuckDB, Polars, etc), but they are just the backend. They still require you to write code to get data and plot it. Sometimes you just need to plot something to skim through it before diving deeper in another tool. I wanted a tool that gives you the ease of use of PlotJuggler for large datasets in parquet format.

## Notes

This is a very early project, but it seems to be working ok. Disclaimer: it was mostly written by AI.
I have mostly tested it with parquet files, so other formats may not work as intended yet.
