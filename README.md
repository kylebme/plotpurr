# PlotPurr

<center><img src="PlotPurr.png" width=50%></center>

An experimental plotting playground designed for very large timeseries datasets.
PlotPurr downsamples the input data using Clickhouse and dynamically fetches new data as you zoom in.

## Demo

![PlotPurr Screencap](screencap.gif)

## Installation

1. Make sure you have a recent version of Node.js installed.
2. Clone the repo.
3. Install dependencies:
   ```bash
   npm install
   ```
4. Start the app:
   ```bash
   npm start
   ```

## Motivation

Existing plotting GUI tools (PlotJuggler, Rerun) are excellent, but they focus on datasets that can fit in RAM, since they load the entire dataset. Additionally, there is a large set of tooling for dealing with large datasets (Clickhouse, DuckDB, Polars, etc), but they are just the backend. They still require you to write code to get data and plot it. Sometimes you just need to plot something to skim through it before diving deeper in another tool. I wanted a tool that gives you the ease of use of PlotJuggler for large datasets in parquet format.
