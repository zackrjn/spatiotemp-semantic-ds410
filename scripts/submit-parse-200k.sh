#!/bin/bash
#SBATCH --job-name=cc-parse-200k
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=96G
#SBATCH --time=08:00:00
#SBATCH --output=cc-parse-200k-%j.out
#SBATCH --error=cc-parse-200k-%j.err

module purge
module load anaconda
module load jdk
module load spark/3.3.0
source activate roar-commoncrawl

cd /storage/work/sjr6223/spatiotemp-semantic-ds410
export PYSPARK_PYTHON=$(which python)
export PYTHONPATH=$PWD/src

IN_OFFSETS=/scratch/sjr6223/commoncrawl/offsets/CC-MAIN-2024-33_edu_gov_200k
OUT_CURATED=/scratch/sjr6223/commoncrawl/curated/CC-MAIN-2024-33_edu_gov_200k

spark-submit \
  --master local[${SLURM_CPUS_PER_TASK}] \
  --conf spark.pyspark.python=$PYSPARK_PYTHON \
  --conf spark.pyspark.driver.python=$PYSPARK_PYTHON \
  --conf spark.executorEnv.PYTHONPATH=$PYTHONPATH \
  --conf spark.sql.shuffle.partitions=1024 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.network.timeout=600s \
  src/jobs/parse_warc.py \
  --paths config/paths.roar.yaml \
  --offsets $IN_OFFSETS \
  --out $OUT_CURATED

