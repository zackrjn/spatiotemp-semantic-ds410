#!/bin/bash
#SBATCH --job-name=cc-parse-10k
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --time=02:00:00
#SBATCH --output=cc-parse-10k-%j.out
#SBATCH --error=cc-parse-10k-%j.err

module purge
module load anaconda
module load jdk
module load spark/3.3.0
source activate roar-commoncrawl

cd /storage/work/sjr6223/spatiotemp-semantic-ds410
export PYSPARK_PYTHON=$(which python)
export PYTHONPATH=$PWD/src

IN_OFFSETS=/scratch/sjr6223/commoncrawl/offsets/CC-MAIN-2024-33_edu_gov_10k
OUT_CURATED=/scratch/sjr6223/commoncrawl/curated/CC-MAIN-2024-33_edu_gov_10k

spark-submit \
  --master local[${SLURM_CPUS_PER_TASK}] \
  --conf spark.pyspark.python=$PYSPARK_PYTHON \
  --conf spark.pyspark.driver.python=$PYSPARK_PYTHON \
  --conf spark.executorEnv.PYTHONPATH=$PYTHONPATH \
  --conf spark.sql.shuffle.partitions=256 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.network.timeout=600s \
  src/jobs/parse_warc.py \
  --paths config/paths.roar.yaml \
  --offsets $IN_OFFSETS \
  --out $OUT_CURATED
#!/bin/bash
#SBATCH --job-name=cc-parse-10k
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --time=02:00:00
#SBATCH --output=cc-parse-10k-%j.out
#SBATCH --error=cc-parse-10k-%j.err

module purge
module load anaconda
module load jdk
module load spark/3.3.0
source activate roar-commoncrawl

cd /storage/work/sjr6223/spatiotemp-semantic-ds410
export PYSPARK_PYTHON=
export PYTHONPATH=C:\Users\saksh\Desktop\spatiotemp-semantic-ds410/src

IN_OFFSETS=/scratch/sjr6223/commoncrawl/offsets/CC-MAIN-2024-33_edu_gov_10k
OUT_CURATED=/scratch/sjr6223/commoncrawl/curated/CC-MAIN-2024-33_edu_gov_10k

spark-submit \
  --master local[] \
  --conf spark.pyspark.python= \
  --conf spark.pyspark.driver.python= \
  --conf spark.executorEnv.PYTHONPATH= \
  --conf spark.sql.shuffle.partitions=256 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.network.timeout=600s \
  src/jobs/parse_warc.py \
  --paths config/paths.roar.yaml \
  --offsets  \
  --out 
