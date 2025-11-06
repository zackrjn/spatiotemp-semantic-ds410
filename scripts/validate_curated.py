#!/usr/bin/env python3
"""
Quick validation helper for curated Common Crawl outputs.

Usage:
  python scripts/validate_curated.py \
    --input /scratch/sjr6223/commoncrawl/curated/CC-MAIN-2024-33_edu_gov_10k
"""

import argparse

from pyspark.sql import SparkSession, functions as F


def main() -> None:
    parser = argparse.ArgumentParser(description="Print basic stats for curated Common Crawl parquet output.")
    parser.add_argument("--input", required=True, help="Path to curated Parquet directory.")
    parser.add_argument("--sample", type=int, default=5, help="Number of example rows to show.")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("validate_curated").getOrCreate()
    df = spark.read.parquet(args.input)

    total = df.count()
    print(f"Total rows: {total}")

    df.groupBy("status").count().orderBy(F.desc("count")).show(10, False)
    df.groupBy("lang").count().orderBy(F.desc("count")).show(10, False)

    df.select("url", "status", "lang", "title", "parse_errors").limit(args.sample).show(truncate=80)

    spark.stop()


if __name__ == "__main__":
    main()

