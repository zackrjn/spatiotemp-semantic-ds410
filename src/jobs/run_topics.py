import argparse
from pyspark.sql import SparkSession
from commoncrawl.utils import resolve_path
from commoncrawl.topics import run_lda


def main() -> None:
	parser = argparse.ArgumentParser()
	parser.add_argument("--features", required=True)
	parser.add_argument("--out", required=True)
	parser.add_argument("--k", type=int, default=50)
	parser.add_argument("--max_iter", type=int, default=20)
	args = parser.parse_args()

	spark = SparkSession.builder.appName("cc-run-topics").getOrCreate()
	features_df = spark.read.parquet(resolve_path(args.features))
	doc_topics, model_topics = run_lda(features_df, k=args.k, max_iter=args.max_iter)

	doc_topics.write.mode("overwrite").parquet(resolve_path(f"{args.out}/doc_topics"))
	model_topics.write.mode("overwrite").parquet(resolve_path(f"{args.out}/model_topics"))

	spark.stop()


if __name__ == "__main__":
	main()


