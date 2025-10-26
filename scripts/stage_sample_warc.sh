#!/usr/bin/env bash
set -euo pipefail

dest="data/samples"
mkdir -p "$dest"

# A tiny WARC sample for unit tests (adjust as needed)
url="https://data.commoncrawl.org/crawl-data/CC-MAIN-2024-33/segments/1721699415222.23/warc/CC-MAIN-20240723155916-20240723185916-00000.warc.gz"
fname=$(basename "$url")

if [ ! -f "$dest/$fname" ]; then
	curl -L "$url" -o "$dest/$fname"
fi

echo "Sample WARC saved to $dest/$fname"

