<!-- 1f2cf180-f69c-4ee5-8e44-671a1eab411b 635f79aa-3fc2-4622-afda-0feefb03de19 -->
# Beam + Git plan (declarative pipeline on HDFS)

## Scope

- Keep the current project and HDFS paths unchanged.
- Add a new Apache Beam pipeline driven by YAML (declarative) and commit everything with git.
- Run locally with DirectRunner; stage input/output to/from HDFS under the hood so user-facing paths stay in HDFS.

## What we’ll add

- `beam/beam_wordcount.py`: Beam pipeline that reads a YAML config and executes wordcount accordingly.
- `config/beam-pipeline.yaml`: Declarative config (paths + normalization + output options).
- `.gitignore`: Python, venv, local Beam outputs.
- `requirements.txt` (or README note): `apache-beam`, `pyyaml`.
- README section: how to run, how to change behavior by editing YAML, and how to commit/tag with git.

## Declarative YAML (example keys)

- `hdfs_base`: e.g. `/user/simun/Big_Data/Dag2`
- `input_dir`: `input_dir`
- `output_dir`: `output_dir`
- `tokenize.lowercase`: true
- `tokenize.allowed_charset`: `a-z0-9` (strip others)
- `pipeline.top_n`: null or number (optional)
- `output.filename`: `output.txt`
- `encryption.total_words.enabled`: false (can demo security toggle if desired)

## Pipeline behavior

- Stage-in: `hdfs dfs -cat {hdfs_base}/{input_dir}/* > .beam_tmp/input.txt` (kept internal; user still declares HDFS paths).
- Run Beam DirectRunner on the local staged file:
- Read lines → normalize (lowercase + strip non-allowed) → split → filter → Count.PerElement → optional Top.N → format `word\tcount`.
- Compute `total_words` as the sum of counts.
- Write single output shard to `.beam_tmp/output.txt`.
- Stage-out:
- Optionally prepend `total_words:<value or token>` to the file depending on `encryption.total_words.enabled`.
- `hdfs dfs -put -f .beam_tmp/output.txt {hdfs_base}/{output_dir}/output.txt`.
- Clean up `.beam_tmp`.

## Git usage

- Initialize git, add and commit existing project + new Beam files.
- Create a branch `feature/beam-declarative` for this addition; open a PR (optional) or merge.
- Tag the demo state: `v-beam-declarative-1` for reproducibility.

## Commands (once implemented)

- Install deps: `pip3 install apache-beam pyyaml`.
- Run: `python3 beam/beam_wordcount.py --config config/beam-pipeline.yaml`.
- Change behavior by editing YAML only; re-run to see new output.
- Git: `git add -A && git commit -m "Beam declarative pipeline" && git tag v-beam-declarative-1`.

## Notes / options

- If we later want direct HDFS IO from Beam (no staging), we can add `pyarrow` and switch to `hdfs://` in YAML; keep the staging code as a fallback controlled by a YAML flag (`use_hdfs_filesystem: false`).
- No changes to your existing Spark/MapReduce scripts; they stay as-is.

### To-dos

- [ ] Add Beam pipeline at beam/beam_wordcount.py with YAML-driven options
- [ ] Create config/beam-pipeline.yaml with HDFS paths and tokenize options
- [ ] Implement HDFS stage-in/out around DirectRunner to keep HDFS paths
- [ ] Add README section and deps; show git init/commit/tag workflow
- [ ] Run Beam pipeline locally and validate output lands in HDFS output_dir