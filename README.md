## Local Big Data WordCount (Spark Streaming + Hadoop MapReduce)

### Prerequisites (local)

- Java 11+, Hadoop (HDFS + YARN), Spark.
- Ensure `hdfs`, `yarn`, and `spark-submit` are on your PATH.

Minimal Hadoop single-node config (high level):
- `core-site.xml`: `fs.defaultFS = hdfs://localhost:9000`
- `hdfs-site.xml`: `dfs.replication = 1`
- `mapred-site.xml`: `mapreduce.framework.name = yarn`

Start services (examples):
```bash
$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh
hdfs dfs -ls /
```

Create HDFS dirs (assignment requirement #1):
```bash
hdfs dfs -mkdir -p /user/$USER/input_dir /user/$USER/output_dir
```

### Dataset

GitHub raw URL (HTTPS, direct):
```
https://github.com/FilipePires98/LargeText-WordCount/blob/main/datasets/AChristmasCarol_CharlesDickens/AChristmasCarol_CharlesDickens_English.txt?raw=1
```

### Scripts

Note: Run `python3 scripts/extract_to_hdfs.py --config config/beam-pipeline.yaml` first to populate HDFS `input_dir` before running streaming or batch.

1) Streaming (Spark Structured Streaming): `scripts/stream_wordcount_spark.py`
- Ingests over HTTPS directly into HDFS (no local files)
- Reads `input_dir` as a streaming source, aggregates words, and writes a single `output.txt` in `output_dir`
- Use `--available-now` for a bounded run (process all present files and exit)

Run (recommended via spark-submit to provide PySpark) — now reads YAML by default:
```bash
spark-submit \
  --master 'local[*]' \
  scripts/stream_wordcount_spark.py \
  --config config/beam-pipeline.yaml \
  --available-now
```
If you have PySpark installed in your Python environment, running with python is also fine:
```bash
python3 scripts/stream_wordcount_spark.py \
  --hdfs-base hdfs://localhost:9000/user/$USER \
  --available-now
```
Optional streaming demo (split into 4 chunks with small delays):
```bash
spark-submit --master 'local[*]' scripts/stream_wordcount_spark.py \
  --hdfs-base hdfs://localhost:9000/user/$USER \
  --simulate-stream 4 --sleep 1 --available-now
```

2) Batch (Hadoop MapReduce via Streaming on YARN): `scripts/batch_wordcount_mapreduce.py`
- Ingests over HTTPS directly into HDFS
- Runs canonical Python mapper/reducer with Hadoop Streaming on YARN
- Consolidates to a single `output.txt` in `output_dir`

Run (reads YAML by default):
```bash
python3 scripts/batch_wordcount_mapreduce.py \
  --config config/beam-pipeline.yaml
```
If the script cannot find the streaming JAR, set:
```bash
export HADOOP_STREAMING_JAR=$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-<version>.jar
```

3) Extract to HDFS (declarative): `scripts/extract_to_hdfs.py`

Run:
```bash
python3 scripts/extract_to_hdfs.py --config config/beam-pipeline.yaml
```
YAML controls dataset URL and filename under `dataset`.

### Output (all scripts)

The word count is written as tab-separated `word\tcount` lines to:
```
hdfs://localhost:9000/user/$USER/output_dir/output.txt
```
First line is `total_words:<token or number>`. Encryption of total_words is controlled by YAML under `security.encrypt_total_words` (default true). The scripts print counts first and the decrypted/plain total at the end of the run for visibility.

### Notes on pipeline and målpinde

- Ingest → storage → processing → output all target HDFS, satisfying the "direct to DFS" requirement.
- Prefer ASCII-only characters in HDFS paths. If your local project path includes non-ASCII characters, consider using a simpler HDFS base such as `/user/$USER/Big_Data/Dag2`.
- Målpinde covered:
  - 1 & 13: End-to-end pipelines (streaming + batch) implemented on HDFS.
  - 3: Pipeline artifacts (inputs, outputs) versus operational job staging (YARN) versus analytical output (`output.txt`).
  - 6: Text format handled; extendable to CSV/JSON/Parquet.
  - 7: HTTPS used for secure transport; Spark/YARN noted. Kafka/MQTT are common alternatives for real streaming sources.
  - 9: Uses Hadoop (HDFS, YARN) and Spark locally.




### Apache Beam (declarative + git)

This project includes a declarative wordcount pipeline with Apache Beam driven by YAML.

- Config: `config/beam-pipeline.yaml` (edit behavior without touching code)
- Pipeline: `beam/beam_wordcount.py`
- Deps: `pip3 install -r requirements.txt`

Run Beam locally (keeps HDFS paths for input/output):
```bash
python3 beam/beam_wordcount.py --config config/beam-pipeline.yaml
```
Behavior you can change via YAML (examples):
- `hdfs_base`, `input_dir`, `output_dir`, `output.filename`
- `tokenize.lowercase`, `tokenize.allowed_charset`
- `pipeline.top_n` (limit output to top N words)
- `security.encrypt_total_words`, `security.fernet_key_path`
- `dataset.url`, `dataset.filename` (for the extractor)

Git (example):
```bash
git init
git add -A
git commit -m "Add Beam declarative pipeline"
git tag v-beam-declarative-1
```
# stream-batch-wordcount-hdfs
