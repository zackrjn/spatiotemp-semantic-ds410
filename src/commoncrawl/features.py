from typing import Tuple
from pyspark.sql import DataFrame
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml import Pipeline


def build_text_features(df: DataFrame, min_df: int = 5, vocab_size: int = 50000) -> Tuple[DataFrame, CountVectorizer]:
	"""Return (features_df, cv_model) with columns: url, domain, tld, crawl, features (sparse vector)."""
	tokenizer = RegexTokenizer(inputCol="text", outputCol="tokens", pattern="\\W+", toLowercase=True, minTokenLength=2)
	remover = StopWordsRemover(inputCol="tokens", outputCol="filtered", caseSensitive=False)
	cv = CountVectorizer(inputCol="filtered", outputCol="features", minDF=min_df, vocabSize=vocab_size)

	pipeline = Pipeline(stages=[tokenizer, remover, cv])
	model = pipeline.fit(df)
	transformed = model.transform(df)

	features_df = transformed.select("url", "domain", "tld", "crawl", "features")
	cv_model = model.stages[-1]
	return features_df, cv_model


