# MLOps Batch Pipeline Task

## Overview
This project implements a minimal production-style batch pipeline:

- YAML config-driven execution
- Deterministic runs via seed
- Rolling mean + signal generation
- Structured metrics output
- Full logging
- Dockerized for one-command execution

---

## CLI Usage

```bash
python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log