# stream-batch-wordcount-hdfs

## Quickstart

- Install deps:
```bash
pip3 install -r requirements.txt
```

- Configure HDFS base and paths in YAML: `config/beam-pipeline.yaml`
  - `hdfs_base`: either a full URI (e.g. `hdfs://namenode:9000/user/$USER/Big_Data/Dag2`) or a path under your defaultFS (e.g. `/user/$USER/Big_Data/Dag2`).
  - `input_dir`, `output_dir`, `output.filename`, `security.encrypt_total_words`.

- Create HDFS dirs (optional, scripts will create as needed):
```bash
hdfs dfs -mkdir -p /user/$USER/input_dir /user/$USER/output_dir
```

1) Extract dataset directly to HDFS input_dir:
```bash
python3 scripts/extract_to_hdfs.py --config config/beam-pipeline.yaml
```

2) Run streaming (Spark Structured Streaming):
```bash
spark-submit --master 'local[*]' \
  scripts/stream_wordcount_spark.py \
  --config config/beam-pipeline.yaml \
  --available-now
```

3) Or run batch (MapReduce on YARN):
```bash
python3 scripts/batch_wordcount_mapreduce.py --config config/beam-pipeline.yaml
```

Output:
- Results are written to `<hdfs_base>/<output_dir>/output.txt`.
- First line is `total_words:<token or number>`; scripts print the decrypted total to terminal.

Optional (Beam, declarative):
```bash
python3 beam/beam_wordcount.py --config config/beam-pipeline.yaml
```