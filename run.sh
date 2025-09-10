#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"
python -m venv .venv || true
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
