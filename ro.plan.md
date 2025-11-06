<!-- 376df47a-7896-40f6-bc00-c490f55dcf7a 9b5a32fd-1477-491e-845c-b92e4464ea96 -->
# Updated Execution Plan (<=4 nodes / 24 cores)

## Scope Reaffirmed

- Maintain focus on `CC-MAIN-2025-14`, `.edu/.gov`, English-only, but cap raw document targets at ~200k parsed pages (≈1% of crawl) to fit resource ceiling.
- Highlight incremental performance scaling (1 node → 2 nodes → 4 nodes) instead of larger clusters; include wall-clock + throughput metrics.

## Workstream Breakdown

1. **filter-index** – Re-run index filtering with tuned sampling fractions (0.2%, 0.5%, 1%) to generate partition-rich offsets stored under `scratch/commoncrawl/offsets/{size}/`.
2. **parse-warc** – In SLURM local mode, parse each offsets tier (e.g., 10k, 50k, 200k rows). Cache hot WARCs to scratch and record ingest throughput, parse success rate, language mix.
3. **features-topics** – Compute TF/IDF + LDA (k≈20–40) on each curated tier; report runtime, topic coherence, sample topic tables.
4. **links-metrics** – Build domain graph, out-degree/in-degree, PageRank (iterations ≤15) for the largest tier feasible; capture runtime and top-ranked domains.
5. **viz-perf-docs** – Summarize outputs in notebooks: sanity plots, topic snapshots, link graphs, plus scaling chart (1 vs 2 vs 4 nodes). Update `README.md` and `plan.md` with empirical limits and recommendations.

## Deliverables & Evidence

- Partitioned offsets & curated Parquet datasets (≤200k pages) with validation notebook snippets.
- Performance table covering all tiers and node counts, highlighting diminishing returns at 4 nodes.
- Topic and link analysis artifacts sized to the achievable dataset.
- Documentation updates noting constraints, methodology, and suggested future scaling.

### To-dos

- [x] Initialize repo with README, .gitignore, .gitattributes (git-lfs)
- [x] Create conda environment.yml and bootstrap scripts for local/ROAR
- [x] Validate Spark on ROAR and check GraphFrames availability
- [x] Add paths.roar.yaml, paths.local.yaml, crawl_2025_14.yaml

