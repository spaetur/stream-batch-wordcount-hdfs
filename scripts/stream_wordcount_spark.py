#!/usr/bin/env python3
import argparse
import os
import sys
import shlex
import subprocess
from datetime import datetime
from pathlib import Path

# Ensure project root is on sys.path so we can import common_utils
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import common_utils as cu

def write_batch_to_single_output(batch_df, epoch_id: int, hdfs_output_dir: str, fernet_key: bytes, encrypt_total: bool) -> None:
    # Order, coalesce, and write a single part file, then move/rename to output.txt
    ordered = batch_df.orderBy(F.desc("count"), F.asc("word")).coalesce(1)
    tmp_dir = f"{hdfs_output_dir}/_tmp_out_{epoch_id}"
    
    # Write CSV to a temporary HDFS directory
    (ordered.write.mode("overwrite").option("header", "false").csv(tmp_dir))

    # Find the written part file and publish as output.txt
    # hdfs dfs -rm -f output.txt; hdfs dfs -cat tmp/part-* | hdfs dfs -put - output.txt; hdfs dfs -rm -r tmp
    cu.run_cmd(f"hdfs dfs -rm -f {shlex.quote(hdfs_output_dir)}/output.txt", check=False)
    cat_cmd = f"hdfs dfs -cat {shlex.quote(tmp_dir)}/part-*"
    put_cmd = f"hdfs dfs -put -f - {shlex.quote(hdfs_output_dir)}/output.txt"

    # Compute total words
    total = batch_df.agg(F.sum("count").alias("total"))
    total_val = total.collect()[0]["total"] or 0
    header = cu.total_header(str(total_val), encrypt_total, fernet_key)
    
    # Use a pipeline to avoid local disk; prepend total_words line
    cu.run_cmd(f"( echo '{header}'; {cat_cmd} ) | {put_cmd}")
    cu.run_cmd(f"hdfs dfs -rm -r -f {shlex.quote(tmp_dir)}", check=False)


def main() -> int:
    parser = argparse.ArgumentParser(description="Streaming wordcount on HDFS using Spark Structured Streaming")
    parser.add_argument("--continuous", action="store_true", help="Run continuously (processingTime trigger) instead of one-shot availableNow")
    args = parser.parse_args()

    cfg = cu.load_yaml_config("config/beam-pipeline.yaml")

    default_user = os.getenv("USER", "user")
    hdfs_base = cfg.get("hdfs_base", f"/user/{default_user}")
    input_dir = cfg.get("input_dir", "input_dir")
    output_dir = cfg.get("output_dir", "output_dir")
    
    tokenize = cfg.get("tokenize", {})
    allowed_charset = tokenize.get("allowed_charset", "a-z0-9")
    
    encrypt_total = bool(cfg.get("security", {}).get("encrypt_total_words", True))
    key_path_cfg = cfg.get("security", {}).get("fernet_key_path")

    hdfs_input_dir = f"{hdfs_base.rstrip('/')}/{input_dir.strip('/')}"
    hdfs_output_dir = f"{hdfs_base.rstrip('/')}/{output_dir.strip('/')}"

    # Prepare/load Fernet key stored in configured path (default to output_dir)
    key_path = key_path_cfg or f"{hdfs_output_dir}/_fernet.key"
    fernet_key = cu.get_or_create_fernet_key(hdfs_output_dir, key_path)

    # 1) Transform: Spark streaming wordcount reading HDFS
    spark = (
        SparkSession.builder.appName("StreamWordCount")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("WARN")

    lines = (
        spark.readStream.format("text")
        .option("path", hdfs_input_dir)
        .option("maxFilesPerTrigger", 1)
        .load()
    )

    # Use allowed_charset from YAML to normalize
    pattern = cu.allowed_charset_regex(allowed_charset)
    cleaned = F.regexp_replace(F.lower(F.col("value")), pattern, " ")
    words = lines.select(F.explode(F.split(cleaned, "\\s+")).alias("word")).where(F.col("word") != "")

    counts = words.groupBy("word").count()

    checkpoint = f"{hdfs_output_dir}/_checkpoint_stream"
    
    query = (
        counts.writeStream.outputMode("complete")
        .foreachBatch(lambda df, eid: write_batch_to_single_output(df, eid, hdfs_output_dir, fernet_key, encrypt_total))
        .option("checkpointLocation", checkpoint)
    )

    if args.continuous:
        query = query.trigger(processingTime="2 seconds")
    else:
        # One-shot: process all currently available files and then exit
        query = query.trigger(availableNow=True)

    active = query.start()
    active.awaitTermination()

    # 2) Load: print result from HDFS (decrypt total_words for terminal if encrypted)
    rc = cu.print_hdfs_output(f"{hdfs_output_dir}/output.txt", encrypt_total, fernet_key)
    spark.stop()
    return rc


if __name__ == "__main__":
    sys.exit(main())


