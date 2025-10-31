#!/usr/bin/env python3
import argparse
import os
import shlex
import subprocess
import yaml


def run(cmd: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, shell=True, check=check, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract a remote text file directly into HDFS input_dir")
    parser.add_argument("--config", default="config/beam-pipeline.yaml")
    default_user = os.getenv("USER", "user")
    parser.add_argument("--hdfs-base", default=f"hdfs://localhost:9000/user/{default_user}")
    parser.add_argument("--input-dir", default="input_dir")
    parser.add_argument("--dataset-url", required=False)
    parser.add_argument("--filename", default="input.txt", help="Target filename in input_dir (e.g., input.txt)")
    args = parser.parse_args()

    cfg = {}
    try:
        with open(args.config, "r", encoding="utf-8") as fh:
            cfg = yaml.safe_load(fh) or {}
    except FileNotFoundError:
        cfg = {}

    hdfs_base = cfg.get("hdfs_base", args.hdfs_base)
    input_dir = cfg.get("input_dir", args.input_dir)
    dataset = cfg.get("dataset", {})
    dataset_url = args.dataset_url or dataset.get("url") or (
        "https://github.com/FilipePires98/LargeText-WordCount/blob/main/datasets/"
        "AChristmasCarol_CharlesDickens/AChristmasCarol_CharlesDickens_English.txt?raw=1"
    )
    filename = args.filename if args.filename != "input.txt" else dataset.get("filename", args.filename)

    hdfs_input_dir = f"{hdfs_base.rstrip('/')}/{input_dir.strip('/')}"
    target = f"{hdfs_input_dir}/{filename}"

    # Ensure input dir exists
    run(f"hdfs dfs -mkdir -p {shlex.quote(hdfs_input_dir)}")

    # Stream HTTPS → HDFS
    cmd = f"curl -L {shlex.quote(dataset_url)} | hdfs dfs -put -f - {shlex.quote(target)}"
    out = run(cmd)
    if out.stderr:
        # print any warnings
        print(out.stderr, end="")
    print(target)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


