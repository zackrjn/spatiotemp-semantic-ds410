#!/usr/bin/env python3
"""
Utility script to build partitioned Common Crawl offset tiers on ROAR.

Example:
  python scripts/generate_offsets.py \
    --crawl CC-MAIN-2024-33 \
    --input /scratch/sjr6223/commoncrawl/index/ccindex_api_2024_33_edu_gov.jsonl \
    --out_root /scratch/sjr6223/commoncrawl/offsets \
    --tiers 10k=10000 50k=50000 200k=200000
"""

import argparse
import os
from typing import Dict

from pyspark.sql import SparkSession, functions as F


def parse_tiers(raw: list[str]) -> Dict[str, int]:
    tiers: Dict[str, int] = {}
    for item in raw:
        if "=" not in item:
            raise argparse.ArgumentTypeError(f"Tier '{item}' must be formatted as name=count")
        name, count = item.split("=", 1)
        tiers[name] = int(count)
    if not tiers:
        raise argparse.ArgumentTypeError("At least one tier (name=count) is required.")
    return tiers


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate partitioned offsets tiers from Common Crawl CDX JSON.")
    parser.add_argument("--crawl", required=True, help="Crawl identifier (e.g., CC-MAIN-2024-33)")
    parser.add_argument("--input", required=True, help="Path to JSONL file from Common Crawl CDX API.")
    parser.add_argument("--out_root", required=True, help="Root directory for tiered Parquet outputs.")
    parser.add_argument("--tiers", nargs="+", type=str, required=True, help="List of tiers formatted as name=count.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for sampling order.")
    parser.add_argument("--partitions", type=int, default=64, help="Number of partitions per tier (repartition on warc_filename).")

    args = parser.parse_args()
    tiers = parse_tiers(args.tiers)

    spark = SparkSession.builder.appName("generate_offsets_tiers").getOrCreate()

    raw = spark.read.json(args.input)

    host = F.lower(F.regexp_extract(F.col("url"), r'^[a-z]+://([^/]+)', 1))
    tld = F.lower(F.regexp_extract(host, r'\.([a-z0-9-]{2,})$', 1))

    base_df = (
        raw.select("url", "filename", "offset", "length", "timestamp", "status")
        .where(F.col("offset").isNotNull() & F.col("length").isNotNull())
        .withColumn("domain", host)
        .withColumn("tld", tld)
        .withColumn("crawl", F.lit(args.crawl))
        .withColumnRenamed("filename", "warc_filename")
        .withColumnRenamed("offset", "warc_record_offset")
        .withColumnRenamed("length", "warc_record_length")
    )

    for tier_name, limit in tiers.items():
        out_path = os.path.join(args.out_root, f"{args.crawl}_edu_gov_{tier_name}")
        (
            base_df.orderBy(F.rand(args.seed))
            .limit(limit)
            .repartition(args.partitions, "warc_filename")
            .write.mode("overwrite")
            .partitionBy("tld")
            .parquet(out_path)
        )
        print(f"Wrote {out_path}")

    spark.stop()


if __name__ == "__main__":
    main()

