#!/usr/bin/env bash
set -e

# Usa el puerto que Render provee, y si corrés local sin PORT, cae en 10000.
PORT=${PORT:-10000}

exec uvicorn main:app --host 0.0.0.0 --port "$PORT"

