Spatiotemporal & Semantic Analysis of Web Content via Common Crawl (PySpark)

Overview
- Scalable PySpark pipeline on PSU ROAR JupyterLab to ingest, filter, parse, and analyze Common Crawl (CC) data for spatiotemporal and semantic insights (topic drift, domain dynamics, link structure).

Initial Scope
- Crawl: CC-MAIN-2025-14
- Language: English (detected downstream)
- TLDs: .edu, .gov
- Sample: 1–5% of eligible URLs

Repository Conventions
- Single GitHub repo cloned locally and on ROAR.
- Use git-lfs for large artifacts (models, small sample WARCs). Do not commit raw WARC/WET/WAT; stage them on ROAR scratch/object storage and access via HTTPS/S3A.

Getting Started (high level)
1) Install git-lfs locally and on ROAR: `git lfs install`
2) Create the conda env: `conda env create -f env/environment.yml && conda activate roar-commoncrawl`
3) (ROAR) Launch JupyterLab kernel using this env; adjust Spark configs in `env/spark-defaults.conf` as needed.
4) Configure paths in `config/paths.roar.yaml` (ROAR) and `config/paths.local.yaml` (local).

Notes
- URL index Parquet: `s3a://commoncrawl/cc-index/table/cc-main/warc/`
- WARC records served from: `https://data.commoncrawl.org/`
- Curated outputs should be written as partitioned Parquet to ROAR scratch.

License
- For class/research use. Add a LICENSE file if publishing.


