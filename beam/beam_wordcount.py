#!/usr/bin/env python3
import argparse
import os
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Tuple

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from cryptography.fernet import Fernet
import shutil
import common_utils as cu


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def stage_in_from_hdfs(hdfs_input_dir: str, tmp_dir: Path) -> Path:
    local_input = tmp_dir / "input.txt"

    # Concatenate all HDFS input files to a single local file
    # If directory is empty, create an empty local file
    rc = subprocess.run(
        f"hdfs dfs -test -e {shlex.quote(hdfs_input_dir)}",
        shell=True,
    ).returncode
    
    if rc != 0:
        # Path doesn't exist; produce empty file
        local_input.write_text("")
        return local_input

    # Use cat and ignore missing matches
    cat_cmd = f"hdfs dfs -cat {shlex.quote(hdfs_input_dir)}/*"
    
    with local_input.open("w", encoding="utf-8") as f:
        p = subprocess.run(cat_cmd, shell=True, stdout=f, stderr=subprocess.PIPE, text=True)
        
        if p.returncode != 0 and "No such file or directory" in (p.stderr or ""):
            # No files; keep empty
            pass
    
    return local_input


def stage_out_to_hdfs(local_output: Path, hdfs_output_path: str) -> None:
    cu.run_cmd(f"hdfs dfs -mkdir -p {shlex.quote(os.path.dirname(hdfs_output_path))}", check=False)
    cu.run_cmd(f"hdfs dfs -put -f {shlex.quote(str(local_output))} {shlex.quote(hdfs_output_path)}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Apache Beam declarative wordcount (HDFS -> local DirectRunner -> HDFS)")
    parser.add_argument("--config", default="config/beam-pipeline.yaml", help="Path to YAML config")
    
    args = parser.parse_args()

    cfg: Dict = cu.load_yaml_config(args.config)

    hdfs_base = cfg.get("hdfs_base", "/user/%s/Big_Data/Dag2" % os.getenv("USER", "user"))
    input_dir = cfg.get("input_dir", "input_dir").strip("/")
    output_dir = cfg.get("output_dir", "output_dir").strip("/")
    output_filename = cfg.get("output", {}).get("filename", "output.txt")

    tokenize = cfg.get("tokenize", {})
    lowercase = bool(tokenize.get("lowercase", True))
    allowed_charset = tokenize.get("allowed_charset", "a-z0-9")
    # normalization will be delegated to common_utils.normalize_text

    pipeline_cfg = cfg.get("pipeline", {})
    top_n = pipeline_cfg.get("top_n")  # may be None

    security_cfg = cfg.get("security", {})
    encrypt_total = bool(security_cfg.get("encrypt_total_words", True))
    key_path_cfg = security_cfg.get("fernet_key_path")

    hdfs_input_dir = f"{hdfs_base.rstrip('/')}/{input_dir}"
    hdfs_output_path = f"{hdfs_base.rstrip('/')}/{output_dir}/{output_filename}"

    tmp_dir = Path(".beam_tmp")
    ensure_dir(tmp_dir)

    local_input = stage_in_from_hdfs(hdfs_input_dir, tmp_dir)
    counts_path = tmp_dir / "counts.txt"
    total_path = tmp_dir / "total.txt"

    class NormalizeAndSplit(beam.DoFn):
        def process(self, line: str) -> List[str]:
            text = cu.normalize_text(line, lowercase, allowed_charset)
    
            for w in text.split():
                if w:
                    yield w

    def format_kv(kv: Tuple[str, int]) -> str:
        return f"{kv[0]}\t{kv[1]}"

    options = PipelineOptions()
    
    with beam.Pipeline(options=options) as p:
        lines = p | "ReadLocalInput" >> beam.io.ReadFromText(str(local_input))
        words = lines | "NormalizeSplit" >> beam.ParDo(NormalizeAndSplit())
        pairs = words | "ToKV" >> beam.Map(lambda w: (w, 1))
        counts = pairs | "CountPerWord" >> beam.CombinePerKey(sum)

        # Write counts unsorted; we'll sort locally post-pipeline for deterministic single-file output
        counts | "FormatCounts" >> beam.Map(format_kv) | "WriteCounts" >> beam.io.WriteToText(
            str(counts_path), shard_name_template="", num_shards=1
        )

        # Compute total words
        total = counts | "JustCounts" >> beam.Map(lambda kv: kv[1]) | "SumCounts" >> beam.CombineGlobally(sum)
        total | "WriteTotal" >> beam.io.WriteToText(str(total_path), shard_name_template="", num_shards=1)

    # Post-process: read counts, sort, optionally limit top_n, and prepend total_words header
    if total_path.exists():
        total_str = total_path.read_text(encoding="utf-8").strip()
    else:
        total_str = "0"

    kv_list: List[Tuple[str, int]] = []
    
    if counts_path.exists():
        for line in counts_path.read_text(encoding="utf-8").splitlines():
            if not line:
                continue
            parts = line.split("\t", 1)
    
            if len(parts) != 2:
                continue
            word, cnt_s = parts
    
            try:
                cnt = int(cnt_s)
            except ValueError:
                continue
    
            kv_list.append((word, cnt))

    # Sort by count desc, word asc
    kv_list.sort(key=lambda kv: (-kv[1], kv[0]))
    
    if isinstance(top_n, int) and top_n > 0:
        kv_list = kv_list[:top_n]

    local_output = tmp_dir / "output.txt"

    # Prepare/load Fernet key if encryption is enabled and compute header value
    if encrypt_total:
        default_key_path = f"{hdfs_base.rstrip('/')}/{output_dir}/_fernet.key"
        key_path = key_path_cfg or default_key_path
        fernet_key = cu.get_or_create_fernet_key(f"{hdfs_base.rstrip('/')}/{output_dir}", key_path)
    else:
        fernet_key = None
    header_val = cu.total_header(total_str, encrypt_total, fernet_key)

    with local_output.open("w", encoding="utf-8") as out_f:
        out_f.write(f"{header_val}\n")

        for w, c in kv_list:
            out_f.write(f"{w}\t{c}\n")

    stage_out_to_hdfs(local_output, hdfs_output_path)

    # Remove local staging directory entirely
    try:
        shutil.rmtree(tmp_dir, ignore_errors=True)
    except Exception:
        pass

    # Print decrypted/plain total at end for visibility
    print(f"total_words: {total_str}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

