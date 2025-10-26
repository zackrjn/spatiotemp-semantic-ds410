from typing import Dict

from pyspark.sql import DataFrame, SparkSession, functions as F


def load_index(spark: SparkSession, cc_index_path: str) -> DataFrame:
	return spark.read.parquet(cc_index_path)


def filter_index(df: DataFrame, cfg: Dict) -> DataFrame:
	crawl_id = cfg["crawl_id"]
	tlds = cfg.get("tlds", [])
	content_types = cfg.get("content_types", ["text/html"])
	filters = cfg.get("filters", {})

	res = df.where(F.col("crawl") == crawl_id)
	if tlds:
		res = res.where(F.col("url_host_tld").isin([t.lower() for t in tlds]))
	if content_types:
		mime_pred = F.lit(False)
		for ct in content_types:
			mime_pred = mime_pred | F.lower(F.col("content_mime_detected")).startswith(ct)
		res = res.where(mime_pred)
	if filters.get("fetch_status"):
		res = res.where(F.col("fetch_status").isin(filters["fetch_status"]))
	if filters.get("include_https_only"):
		res = res.where(F.col("url_surtkey").startswith("https"))

	# Essential columns to persist for byte-range fetch
	cols = [
		"url",
		"url_host_registered_domain",
		"url_host_tld",
		"crawl",
		"timestamp",
		"warc_filename",
		"warc_record_offset",
		"warc_record_length",
		"content_mime_detected",
	]
	res = res.select(*cols).withColumnRenamed("url_host_registered_domain", "domain").withColumnRenamed("url_host_tld", "tld")

	# Optional sampling and cap
	sample_fraction = cfg.get("sample_fraction")
	if sample_fraction and 0 < sample_fraction < 1:
		res = res.sample(withReplacement=False, fraction=float(sample_fraction), seed=42)
	max_records = cfg.get("max_records")
	if max_records:
		res = res.limit(int(max_records))
	return res


