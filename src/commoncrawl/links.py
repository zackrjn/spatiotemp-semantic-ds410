from pyspark.sql import DataFrame, functions as F
from urllib.parse import urlparse


def extract_domain(url: str) -> str:
	try:
		return urlparse(url).hostname or ""
	except Exception:
		return ""


def build_domain_edges(curated_df: DataFrame) -> DataFrame:
	to_domain = F.udf(lambda u: extract_domain(u))
	edges = (
		curated_df
		.select("domain", F.explode("outlinks").alias("dst_url"))
		.withColumn("dst_domain", to_domain(F.col("dst_url")))
		.where(F.col("dst_domain") != "")
		.groupBy("domain", "dst_domain")
		.agg(F.count(F.lit(1)).alias("weight"))
	)
	return edges


