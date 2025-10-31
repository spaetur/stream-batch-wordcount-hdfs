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
python3 scripts/extract_to_hdfs.py
```

2) Run streaming (Spark Structured Streaming, defaults to one-shot availableNow):
```bash
spark-submit --master 'local[*]' scripts/stream_wordcount_spark.py
```

- To run continuously (watch for new files):
```bash
spark-submit --master 'local[*]' scripts/stream_wordcount_spark.py --continuous
```

Note: Clear the checkpoint if you want to reprocess the same data between one-shot runs:
```bash
hdfs dfs -rm -r <hdfs_base>/<output_dir>/_checkpoint_stream
```

3) Or run batch (MapReduce on YARN):
```bash
python3 scripts/batch_wordcount_mapreduce.py
```

Output:
- Results are written to `<hdfs_base>/<output_dir>/output.txt`.
- First line is `total_words:<token or number>`; scripts print the decrypted total to terminal.

Optional (Beam, declarative):
```bash
python3 beam/beam_wordcount.py
```