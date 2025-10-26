#!/usr/bin/env bash
set -euo pipefail

conda env update -f env/environment.yml --prune
conda run -n roar-commoncrawl python -m ipykernel install --user --name roar-commoncrawl --display-name "roar-commoncrawl"

git lfs install
echo "Local environment ready. Select kernel 'roar-commoncrawl' in Jupyter."

