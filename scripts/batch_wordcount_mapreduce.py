#!/usr/bin/env python3
import argparse
import glob
import os
import shlex
import subprocess
import sys
from datetime import datetime
import common_utils as cu


def find_hadoop_examples_jar() -> str:
    env_jar = os.getenv("HADOOP_MAPRED_EXAMPLES_JAR")
    if env_jar and os.path.exists(env_jar):
        return env_jar
    hadoop_home = os.getenv("HADOOP_HOME")
    if hadoop_home:
        candidates = glob.glob(os.path.join(hadoop_home, "share", "hadoop", "mapreduce", "hadoop-mapreduce-examples-*.jar"))
        if candidates:
            candidates.sort()
            return candidates[-1]
    raise FileNotFoundError("Could not locate hadoop-mapreduce-examples jar. Set HADOOP_MAPRED_EXAMPLES_JAR or HADOOP_HOME.")


def run_mapreduce_wordcount(hdfs_input_dir: str, mr_out: str) -> None:
    examples_jar = find_hadoop_examples_jar()
    
    # Remove any previous output
    cu.run_cmd(f"hdfs dfs -rm -r -f {shlex.quote(mr_out)}", check=False)

    # Submit the built-in WordCount on YARN with small resources for single-node
    cmd = (
        " ".join(
            [
                "yarn jar",
                shlex.quote(examples_jar),
                "wordcount",
                "-D mapreduce.job.name=WordCountExamplesJar",
                "-D mapreduce.job.queuename=default",
                "-D mapreduce.job.maps=1",
                "-D mapreduce.job.reduces=1",
                "-D mapreduce.map.memory.mb=256",
                "-D mapreduce.reduce.memory.mb=256",
                "-D yarn.app.mapreduce.am.resource.mb=256",
                shlex.quote(hdfs_input_dir),
                shlex.quote(mr_out),
            ]
        )
    )
    out = cu.run_cmd(cmd)
    sys.stderr.write(out.stderr)


def main() -> int:
    parser = argparse.ArgumentParser(description="Batch wordcount on HDFS using Hadoop Streaming (YARN)")
    parser.add_argument("--config", default="config/beam-pipeline.yaml")
    default_user = os.getenv("USER", "user")
    
    parser.add_argument("--hdfs-base", default=f"hdfs://localhost:9000/user/{default_user}")
    parser.add_argument("--input-dir", default="input_dir")
    parser.add_argument("--output-dir", default="output_dir")
    args = parser.parse_args()

    cfg = cu.load_yaml_config(args.config)

    hdfs_base = cfg.get("hdfs_base", args.hdfs_base)
    input_dir = cfg.get("input_dir", args.input_dir)
    output_dir = cfg.get("output_dir", args.output_dir)
    output_filename = cfg.get("output", {}).get("filename", "output.txt")
    tokenize = cfg.get("tokenize", {})
    allowed_charset = tokenize.get("allowed_charset", "a-z0-9")
    encrypt_total = bool(cfg.get("security", {}).get("encrypt_total_words", True))
    key_path_cfg = cfg.get("security", {}).get("fernet_key_path")

    hdfs_input_dir = f"{hdfs_base.rstrip('/')}/{input_dir.strip('/')}"
    hdfs_output_dir = f"{hdfs_base.rstrip('/')}/{output_dir.strip('/')}"
    hdfs_output_path = f"{hdfs_output_dir}/{output_filename}"

    # 1) Transform (MapReduce on YARN via built-in WordCount)
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    mr_out = f"{hdfs_output_dir}/_mr_out_{ts}"
    try:
        run_mapreduce_wordcount(hdfs_input_dir, mr_out)
    except subprocess.CalledProcessError as e:
        sys.stderr.write("\nMapReduce WordCount failed. Stderr follows:\n")
        sys.stderr.write(e.stderr or "<no stderr>\n")
        sys.stderr.write("\nStdout follows:\n")
        sys.stderr.write(e.stdout or "<no stdout>\n")
        return e.returncode

    # Consolidate to single output (sorted by count desc, word asc)
    cu.run_cmd(f"hdfs dfs -rm -f {shlex.quote(hdfs_output_path)}", check=False)
    cat_cmd = f"hdfs dfs -cat {shlex.quote(mr_out)}/part-*"
    sort_cmd = "sort -k2,2nr -k1,1"
    put_cmd = f"hdfs dfs -put -f - {shlex.quote(hdfs_output_path)}"
    # Prepare/load Fernet key stored at configured path
    key_path = key_path_cfg or f"{hdfs_output_dir}/_fernet.key"
    fernet_key = cu.get_or_create_fernet_key(hdfs_output_dir, key_path)

    # Compute total words from MR output
    tot = cu.run_cmd(f"{cat_cmd} | awk 'BEGIN{{FS=\"\\t\"}} {{s+=$2}} END{{print s}}'")
    total_val = tot.stdout.strip() or "0"
    header = cu.total_header(total_val, encrypt_total, fernet_key)

    # Normalize words (lowercase, strip non-allowed) and aggregate counts before final sort, then prepend total header
    norm_agg = cu.awk_norm_agg(allowed_charset)
    cu.run_cmd(f"( echo '{header}'; {cat_cmd} | {norm_agg} | {sort_cmd} ) | {put_cmd}")
    cu.run_cmd(f"hdfs dfs -rm -r -f {shlex.quote(mr_out)}", check=False)

    # 2) Load: print result (decrypt total_words for terminal)
    return cu.print_hdfs_output(hdfs_output_path, encrypt_total, fernet_key)


if __name__ == "__main__":
    sys.exit(main())



