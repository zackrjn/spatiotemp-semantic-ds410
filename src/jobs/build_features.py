import argparse
from pyspark.sql import SparkSession
from commoncrawl.utils import resolve_path
from commoncrawl.features import build_text_features


def main() -> None:
	parser = argparse.ArgumentParser()
	parser.add_argument("--curated", required=True)
	parser.add_argument("--out", required=True)
	args = parser.parse_args()

	spark = SparkSession.builder.appName("cc-build-features").getOrCreate()
	curated = spark.read.parquet(resolve_path(args.curated))
	en = curated.where((curated.lang == "en") & (curated.text.isNotNull()))
	features_df, _ = build_text_features(en)
	features_df.write.mode("overwrite").parquet(resolve_path(args.out))
	spark.stop()


if __name__ == "__main__":
	main()


