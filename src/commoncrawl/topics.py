from typing import Tuple
from pyspark.sql import DataFrame
from pyspark.ml.clustering import LDA


def run_lda(features_df: DataFrame, k: int = 50, max_iter: int = 20, seed: int = 42) -> Tuple[DataFrame, DataFrame]:
	"""Return (doc_topics, model_topics) DataFrames."""
	lda = LDA(featuresCol="features", k=k, maxIter=max_iter, seed=seed)
	model = lda.fit(features_df)
	doc_topics = model.transform(features_df)
	model_topics = model.describeTopics()
	return doc_topics, model_topics


