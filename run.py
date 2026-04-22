import argparse
import json
import logging
import time
import sys
import os

import pandas as pd
import numpy as np
import yaml


def setup_logging(log_file):
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )


def load_config(config_path):
    if not os.path.exists(config_path):
        raise ValueError("Config file not found")

    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
    except Exception:
        raise ValueError("Invalid YAML format")

    required_keys = ["seed", "window", "version"]
    for key in required_keys:
        if key not in config:
            raise ValueError(f"Missing config key: {key}")

    if not isinstance(config["seed"], int):
        raise ValueError("Seed must be integer")

    if not isinstance(config["window"], int) or config["window"] <= 0:
        raise ValueError("Window must be positive integer")

    if not isinstance(config["version"], str):
        raise ValueError("Version must be string")

    return config


def load_data(input_path):
    if not os.path.exists(input_path):
        raise ValueError("Input CSV file not found")

    try:
        df = pd.read_csv(input_path)
    except Exception:
        raise ValueError("Invalid CSV format")

    if df.empty:
        raise ValueError("CSV file is empty")

    if "close" not in df.columns:
        raise ValueError("Missing required column: close")

    return df


def process_data(df, window):
    df["rolling_mean"] = df["close"].rolling(window=window).mean()
    df = df.dropna().copy()
    df["signal"] = (df["close"] > df["rolling_mean"]).astype(int)
    return df


def compute_metrics(df, version, seed, latency_ms):
    rows_processed = len(df)
    signal_rate = float(df["signal"].mean())

    return {
        "version": version,
        "rows_processed": rows_processed,
        "metric": "signal_rate",
        "value": round(signal_rate, 4),
        "latency_ms": int(latency_ms),
        "seed": seed,
        "status": "success"
    }


def write_metrics(output_path, metrics):
    with open(output_path, "w") as f:
        json.dump(metrics, f, indent=2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--log-file", required=True)

    args = parser.parse_args()

    setup_logging(args.log_file)
    start_time = time.time()

    logging.info("Job started")

    try:
        config = load_config(args.config)
        logging.info(f"Config loaded: {config}")

        np.random.seed(config["seed"])

        df = load_data(args.input)
        logging.info(f"Rows loaded: {len(df)}")

        df_processed = process_data(df, config["window"])
        logging.info("Rolling mean and signal computed")

        latency_ms = (time.time() - start_time) * 1000

        metrics = compute_metrics(
            df_processed,
            config["version"],
            config["seed"],
            latency_ms
        )

        write_metrics(args.output, metrics)

        logging.info(f"Metrics: {metrics}")
        logging.info("Job completed successfully")

        print(json.dumps(metrics, indent=2))
        sys.exit(0)

    except Exception as e:
        latency_ms = (time.time() - start_time) * 1000

        error_metrics = {
            "version": "v1",
            "status": "error",
            "error_message": str(e)
        }

        write_metrics(args.output, error_metrics)

        logging.error(f"Error occurred: {str(e)}")
        logging.info("Job failed")

        print(json.dumps(error_metrics, indent=2))
        sys.exit(1)


if __name__ == "__main__":
    main()