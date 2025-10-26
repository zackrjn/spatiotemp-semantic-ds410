# Spatiotemporal & Semantic Analysis via Common Crawl on ROAR — Implementation Plan

## Overview

Build a scalable PySpark pipeline on ROAR JupyterLab to: (1) filter the Common Crawl URL index, (2) fetch/parse WARC records by byte-range, (3) extract main text, metadata, and outlinks, (4) generate NLP features and topics, (5) aggregate link metrics, (6) visualize trends and evaluate performance. Code runs both locally and on ROAR with a single GitHub repo and git-lfs for large artifacts.

## Initial Subset (chosen)

- Crawl: `CC-MAIN-2025-14`
- Language: English (detected downstream)
- TLDs: `.edu`, `.gov`
- Sample: 1–5% of eligible URLs

Rationale: cleaner, lower-spam, smaller and more coherent than top-volume TLDs; ideal for rapid correctness and performance baselines. After baseline, expand to “top 5 TLDs by volume” using the same pipeline.

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
- Fetch WARC records via HTTPS byte-range from `https://data.commoncrawl.org/` during parsing (streamed).

## Environment

- Conda: Python 3.10+, PySpark 3.5.x, PyArrow, pandas, warcio, beautifulsoup4, lxml, jusText, langdetect, matplotlib, seaborn.
- Optional: fastText language ID (fallback), GraphFrames (if cluster allows `--packages graphframes:graphframes:0.8.3-spark3.5-s_2.12`).
- Spark defaults (tunable later): executor memory, cores, `spark.sql.shuffle.partitions`, `spark.serializer=KryoSerializer`.

## Pipeline Steps

1) Index filtering (Spark SQL/DataFrame)

- Input: CC index Parquet; filters: `crawl=CC-MAIN-2025-14`, `url_host_tld in ('edu','gov')`, `content_mime_detected like 'text/html%'`, successful `fetch_status`.
- Output: offsets list parquet with columns: `url, domain, tld, timestamp, warc_filename, warc_record_offset, warc_record_length` (+ helpful metadata). Partition by `tld`.

2) WARC parsing ETL (mapPartitions)

- Group rows by `warc_filename` to batch HTTP Range requests. Stream with `warcio` to extract: title, main text (jusText), meta charset, outlinks (normalized absolute URLs), status, digest (SHA1), and failures.
- Output: curated dataset (Parquet), partitioned by `crawl`, `tld`, `lang` (post-detection). Columns: `url, domain, tld, crawl, timestamp, status, lang, title, text, outlinks, digest, warc_filename, offset, length, parse_errors`.

3) NLP features & topics (MLlib)

- Tokenize (RegexTokenizer), StopWordsRemover (en), HashingTF or CountVectorizer with vocab pruning, IDF.
- LDA (e.g., k=50, maxIter=20). Persist: topic-term table, doc-topic distributions, topic coherence proxy (UMass) for selection.

4) Link aggregation

- Extract `src_domain -> dst_domain` edges from outlinks. Aggregate weights, compute in/out-degree. PageRank via GraphFrames if available, else DataFrame-based power iteration for top domains.

5) Visualization & reporting

- Topic trends over time, domain distributions, link degree distributions, top communities (if computed). Performance charts across cluster sizes.

6) Performance & scaling

- Profiles: Dev (1 node), Medium (2 nodes), Full subset (4 nodes). Measure wall-clock, executor utilization, shuffle volume. Tune partitions, caching, broadcast joins.

## Acceptance Criteria

- Reproducible repo with env, config, and notebooks.
- Filtered offsets list for `.edu/.gov` (≥1–5% sample) with sanity stats.
- Curated Parquet with ≥80% successful parse rate and language coverage; schema documented.
- Baseline topics with interpretable terms; basic link metrics computed; visualizations produced.
- Performance report comparing 1 vs 2 vs 4 nodes with speedups and tuning notes.

## Day-1 Tasks (after approval)

- Create repo skeleton with LFS patterns and env files; push to GitHub; clone on ROAR.
- Run `00_env_check.ipynb` to validate Spark + package availability, and confirm anonymous s3a access.
- Implement `index_filter.py` with minimal filters and write first offsets parquet (small sample).