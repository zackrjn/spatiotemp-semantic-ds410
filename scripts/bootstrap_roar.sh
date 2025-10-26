#!/usr/bin/env bash
set -euo pipefail

module purge || true
# Load conda on ROAR if needed; adjust for your environment
source ~/.bashrc || true
conda env update -f env/environment.yml --prune
conda run -n roar-commoncrawl python -m ipykernel install --user --name roar-commoncrawl --display-name "roar-commoncrawl"

git lfs install
echo "ROAR environment ready. Use kernel 'roar-commoncrawl' in JupyterLab."

