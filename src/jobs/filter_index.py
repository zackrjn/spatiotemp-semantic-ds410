import argparse
from pyspark.sql import SparkSession

from commoncrawl.utils import load_yaml, resolve_path
from commoncrawl.index_filter import load_index, filter_index


def main() -> None:
	parser = argparse.ArgumentParser()
	parser.add_argument("--paths", required=True, help="paths YAML (roar/local)")
	parser.add_argument("--crawl", required=True, help="crawl config YAML")
	parser.add_argument("--out", required=True, help="output directory for offsets parquet")
	args = parser.parse_args()

	paths_cfg = load_yaml(args.paths)
	crawl_cfg = load_yaml(args.crawl)

	spark = (
		SparkSession.builder
		.appName("cc-index-filter")
		.config("spark.sql.shuffle.partitions", 400)
		.getOrCreate()
	)

	index_df = load_index(spark, paths_cfg["cc_index_parquet"])
	filtered = filter_index(index_df, crawl_cfg)

	out_dir = resolve_path(args.out)
	(
		filtered
		.repartition("tld")
		.write.mode("overwrite")
		.partitionBy("tld")
		.parquet(out_dir)
	)

	spark.stop()


if __name__ == "__main__":
	main()


