import argparse
import json
from typing import Iterator, Dict, Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
	StructType, StructField, StringType, IntegerType, LongType, ArrayType
)

from commoncrawl.utils import load_yaml, resolve_path
from commoncrawl.warc_reader import http_range_fetch, parse_single_record, sha1_digest
from commoncrawl.html_text import extract_title_text_links
from commoncrawl.lang import detect_language


def process_partition(rows: Iterator[Dict[str, Any]], base_https: str) -> Iterator[Dict[str, Any]]:
	for r in rows:
		url = r["url"]
		domain = r["domain"]
		tld = r["tld"]
		crawl = r["crawl"]
		timestamp = r["timestamp"]
		warc_filename = r["warc_filename"]
		offset = int(r["warc_record_offset"]) if r["warc_record_offset"] is not None else None
		length = int(r["warc_record_length"]) if r["warc_record_length"] is not None else None
		parse_errors = None
		status_code = None
		lang = None
		title = ""
		text = ""
		outlinks = []
		digest = None
		try:
			if offset is None or length is None:
				raise ValueError("missing offset/length")
			chunk = http_range_fetch(base_https, warc_filename, offset, length)
			status_code, target_uri, content_type, payload = parse_single_record(chunk)
			if payload:
				title, text, outlinks = extract_title_text_links(payload, url, lang_stoplist="English")
				lang = detect_language(text) or "und"
				digest = sha1_digest(payload)
		except Exception as e:
			parse_errors = str(e)[:500]
		yield {
			"url": url,
			"domain": domain,
			"tld": tld,
			"crawl": crawl,
			"timestamp": timestamp,
			"status": int(status_code) if status_code is not None else None,
			"lang": lang,
			"title": title,
			"text": text,
			"outlinks": outlinks,
			"digest": digest,
			"warc_filename": warc_filename,
			"offset": offset,
			"length": length,
			"parse_errors": parse_errors,
		}


def main() -> None:
	parser = argparse.ArgumentParser()
	parser.add_argument("--paths", required=True)
	parser.add_argument("--offsets", required=True, help="input parquet from index filter")
	parser.add_argument("--out", required=True, help="curated parquet root")
	args = parser.parse_args()

	paths_cfg = load_yaml(args.paths)
	base_https = paths_cfg["cc_warc_base_https"]

	spark = SparkSession.builder.appName("cc-parse-warc").getOrCreate()
	input_df = spark.read.parquet(resolve_path(args.offsets))

	needed = input_df.select(
		"url",
		F.col("domain"),
		F.col("tld"),
		F.col("crawl"),
		F.col("timestamp"),
		F.col("warc_filename"),
		F.col("warc_record_offset"),
		F.col("warc_record_length"),
	)

	schema = StructType([
		StructField("url", StringType(), True),
		StructField("domain", StringType(), True),
		StructField("tld", StringType(), True),
		StructField("crawl", StringType(), True),
		StructField("timestamp", StringType(), True),
		StructField("status", IntegerType(), True),
		StructField("lang", StringType(), True),
		StructField("title", StringType(), True),
		StructField("text", StringType(), True),
		StructField("outlinks", ArrayType(StringType()), True),
		StructField("digest", StringType(), True),
		StructField("warc_filename", StringType(), True),
		StructField("offset", LongType(), True),
		StructField("length", LongType(), True),
		StructField("parse_errors", StringType(), True),
	])

	rdd = needed.rdd.mapPartitions(lambda it: process_partition((row.asDict(recursive=True) for row in it), base_https))
	curated_df = spark.createDataFrame(rdd, schema=schema)

	(
		curated_df
		.write.mode("overwrite")
		.partitionBy("crawl", "tld", "lang")
		.parquet(resolve_path(args.out))
	)

	spark.stop()


if __name__ == "__main__":
	main()


