import argparse
from pyspark.sql import SparkSession
from commoncrawl.utils import resolve_path
from commoncrawl.links import build_domain_edges


def main() -> None:
	parser = argparse.ArgumentParser()
	parser.add_argument("--curated", required=True)
	parser.add_argument("--out", required=True)
	args = parser.parse_args()

	spark = SparkSession.builder.appName("cc-build-links").getOrCreate()
	curated = spark.read.parquet(resolve_path(args.curated))
	edges = build_domain_edges(curated)
	edges.write.mode("overwrite").parquet(resolve_path(args.out))
	spark.stop()


if __name__ == "__main__":
	main()


