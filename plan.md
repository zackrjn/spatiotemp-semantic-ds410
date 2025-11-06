# Spatiotemporal & Semantic Analysis via Common Crawl on ROAR — Implementation Plan

## Overview

Build a PySpark pipeline tailored to ROAR JupyterLab’s current ceiling (≤4 nodes / 24 total cores) to: (1) filter the Common Crawl URL index, (2) fetch/parse WARC records by byte-range, (3) extract main text, metadata, and outlinks, (4) generate NLP features and topics, (5) aggregate link metrics, and (6) visualize trends with performance measurements. Code runs both locally and on ROAR with a single GitHub repo and git-lfs for large artifacts; deliverables emphasize datasets, insights, and scaling evidence achievable within the available compute.

## Initial Subset (current focus)

- Crawl: Primary target `CC-MAIN-2025-14`; operational subset `CC-MAIN-2024-33` (used for validation because direct index access to the 2025 crawl returns 403 errors)
- Language: English (detected downstream)
- TLDs: `.edu`, `.gov`
- Sample: 0.2–1% of eligible URLs (≤200k parsed pages) sized to complete within the 4-node / 24-core limit

Rationale: cleaner, lower-spam, smaller and more coherent than top-volume TLDs; scoped sample keeps runtime manageable while still yielding meaningful insights and scaling comparisons. After baseline, expand to “top 5 TLDs by volume” using the same pipeline. When Common Crawl relaxes access constraints, re-point configs back to the 2025 crawl without changing code.

## Repository Structure

```
.
├── README.md
├── .gitignore
├── .gitattributes              # git-lfs patterns (models, small WARC samples, artifacts)
├── env/
│   ├── environment.yml         # conda env (pyspark, pyarrow, warcio, bs4, lxml, langdetect, jusText)
│   └── spark-defaults.conf     # optional defaults (executor mem, packages)
├── config/
│   ├── paths.roar.yaml         # ROAR paths (scratch roots, s3/http bases)
│   ├── paths.local.yaml        # local paths
│   └── crawl_2025_14.yaml      # crawl config (tlds, sample frac, filters)
├── notebooks/
│   ├── 00_env_check.ipynb
│   ├── 01_index_filter.ipynb
│   ├── 02_warc_parse_etl.ipynb
│   ├── 03_features_topics.ipynb
│   ├── 04_links_graph.ipynb
│   ├── 05_longitudinal.ipynb
│   └── 06_viz_perf.ipynb
├── src/
│   ├── commoncrawl/
│   │   ├── __init__.py
│   │   ├── index_filter.py     # read Parquet index; filter; persist offsets list
│   │   ├── warc_reader.py      # byte-range fetch; warcio parsing
│   │   ├── html_text.py        # lxml/bs4 + jusText; outlink extraction; normalization
│   │   ├── lang.py             # language ID (langdetect, optional fastText)
│   │   ├── features.py         # tokenize, stopword remove, TF/IDF; vocab pruning
│   │   ├── topics.py           # MLlib LDA wrapper + outputs
│   │   ├── links.py            # domain graph aggregation; degrees; PageRank (GraphFrames if available)
│   │   └── utils.py            # I/O, hashing, URL canon, error logging
│   └── jobs/
│       ├── filter_index.py
│       ├── parse_warc.py
│       ├── build_features.py
│       ├── run_topics.py
│       └── build_links.py
├── scripts/
│   ├── bootstrap_roar.sh       # env setup on ROAR; install graphframes if needed
│   ├── bootstrap_local.sh
│   └── stage_sample_warc.sh    # optional: download tiny WARC samples for unit tests
├── data/
│   ├── samples/                # small sample WARCs (LFS)
│   └── schemas/                # JSON schema snapshots for curated Parquet
└── tests/
    └── test_parsers.py
```

## Data & Storage

- Do not commit raw WARC/WET/WAT. Use ROAR scratch/object storage. Reference locations via `config/paths.*.yaml`.
- Read URL index Parquet anonymously from AWS Open Data (`s3a://commoncrawl/cc-index/table/cc-main/warc/`) OR pre-stage filtered parquet to ROAR scratch.
- If the AWS bucket denies listing for a crawl (observed for 2025-14), use the Common Crawl CDX API to fetch offsets for the matching URLs, convert to Parquet on scratch, and substitute the generated path (e.g., `config/paths.roar.localidx.yaml`).
- Fetch WARC records via HTTPS byte-range from `https://data.commoncrawl.org/` during parsing (streamed); optionally stage frequently used WARCs on scratch to reduce repeated downloads.

## Environment

- Conda: Python 3.10+, PySpark 3.3.x (module-provided), PyArrow, pandas, warcio, beautifulsoup4, lxml, jusText, langdetect, matplotlib, seaborn.
- Optional: fastText language ID (fallback), GraphFrames (if cluster allows `--packages graphframes:graphframes:0.8.3-spark3.5-s_2.12`).
- Spark defaults (tunable later): executor memory, cores, `spark.sql.shuffle.partitions`, `spark.serializer=KryoSerializer`.

## Pipeline Steps

1) Index filtering (Spark SQL/DataFrame)

- Input: CC index Parquet; filters: `crawl in ('CC-MAIN-2025-14','CC-MAIN-2024-33')`, `url_host_tld in ('edu','gov')`, `content_mime_detected like 'text/html%'`, successful `fetch_status`.
- Sampling tiers: 0.2%, 0.5%, 1.0% (≈10k, 50k, 200k URLs) materialized under `scratch/commoncrawl/offsets/{tier}/`; repartition by `warc_filename` (≥64 partitions) to keep downstream tasks parallel.
- Output schema: `url, domain, tld, timestamp, warc_filename, warc_record_offset, warc_record_length` (+ helpful metadata). Partition by `tld` for efficient reads.

2) WARC parsing ETL (mapPartitions)

- Run via SLURM in local mode (1→4 nodes) using HTTPS byte-range fetch; optionally cache active WARC files to scratch.
- Extract title, main text (jusText), outlinks (absolute), HTTP status, digest (SHA1), parse errors. Maintain telemetry on throughput and success rate per tier.
- Output: curated Parquet partitioned by `crawl`, `tld`, `lang`. Columns: `url, domain, tld, crawl, timestamp, status, lang, title, text, outlinks, digest, warc_filename, offset, length, parse_errors`.

3) NLP features & topics (MLlib)

- Tokenize (RegexTokenizer), StopWordsRemover (en), HashingTF or CountVectorizer with vocab pruning, IDF.
- Run LDA with k≈20–40, maxIter≤20 on each curated tier; capture runtime, convergence diagnostics, topic summaries, and coherence proxy (UMass).

4) Link aggregation

- Derive `src_domain -> dst_domain` edges from curated outlinks, aggregate weights, compute in/out-degree, and run PageRank (GraphFrames if available, otherwise iterative DataFrame approach) on the largest feasible tier.

5) Visualization & reporting

- Produce tier-specific sanity plots (language mix, status codes), topic term bars, link degree distributions, and performance charts comparing 1 vs 2 vs 4 nodes.

6) Performance & scaling

- Document wall-clock, throughput (URLs/sec), executor utilization, and shuffle volume per tier and node count. Note tuning levers (partitions, caching, broadcast joins) and limits imposed by the 24-core ceiling.

## Acceptance Criteria

- Reproducible repo with env, config, notebooks, and documented SLURM commands.
- Filtered offsets tiers (0.2%, 0.5%, 1.0%) for `.edu/.gov` with sanity stats and sufficient partitions.
- Curated Parquet (≤200k pages) with ≥80% parse success, language coverage summary, and schema snapshot.
- TF/IDF + LDA outputs (topics, doc mixes) and link metrics (degrees, PageRank) on the largest feasible tier.
- Performance study covering 1 vs 2 vs 4 nodes, highlighting throughput gains, bottlenecks, and best practices under the resource ceiling.

## Day-1 Tasks (after approval)

- Create repo skeleton with LFS patterns and env files; push to GitHub; clone on ROAR.
- Run `00_env_check.ipynb` to validate Spark + package availability, and confirm anonymous s3a access.
- Implement `index_filter.py` with minimal filters and write first offsets parquet (small sample).