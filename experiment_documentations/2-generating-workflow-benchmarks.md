# Generating wfbench workflows

This document describes the CLI parameters accepted by `translator/wf_gen.py`.

## Script purpose

`wf_gen.py` converts WfFormat JSON instances into WfBench benchmark workflows under `./benchmarks/`.

It can:

- generate from a single workflow file
- generate for all workflow JSON files in `./workflows`
- optionally generate CPU-only and IO-only variants
- optionally generate a small test benchmark

## Usage

```bash
python wf_gen.py (--workflow <path> | --all) [options]
```

`--workflow` and `--all` are mutually exclusive, and one of them is required.

## Parameters

### Required selection (one required)

- `--workflow <path>`
	- Path to one workflow JSON file to process.
	- Example: `--workflow ./workflows/montage-chameleon-2mass-005d-001.json`

- `--all`
	- Process every `.json` file in `./workflows`.

### Optional flags and values

- `--test`
	- Also generates a test benchmark in `./benchmarks/test`.
	- Uses `./workflows/montage-chameleon-2mass-005d-001.json` as input in the current implementation.
	- A simple workflow that's 100% cpu, a total data size of 10MB.

- `--scale <float>`
	- Default: `1.0`
	- Scales CPU work and file sizes during benchmark generation.
	- Example: `--scale 0.5`

- `--debug`
	- Enables debug-level logging.

- `--cpu-only`
	- For each generated benchmark, also create a CPU-only variant.
	- Output folder/name suffix: `_cpu`.
	- Sets input/output file sizes to `0` and keeps CPU work.

- `--io-only`
	- For each generated benchmark, also create an IO-only variant.
	- Output folder/name suffix: `_io`.
	- Uses minimal CPU work (effectively emphasizing IO behavior).

- `--ref <float>`
	- Optional reference calibration value: seconds per `100` CPU work units.
	- If omitted, the script benchmarks automatically (10 runs average):
		- Linux: uses `wfbench` pinned with `taskset -c 0`
		- Non-Linux: uses `cpu-benchmark`
    * NOTE: this is kinda useless now that we are running the benchmarks using PARSL on the "cluster" to get a baseline runtime for each tasks...

## Behavior notes

- Output naming pattern for normal generation:
	- `./benchmarks/<workflow_name>_<scale>`
	- plus `_cpu` or `_io` suffix when selected.

- If both `--cpu-only` and `--io-only` are passed, both variants are generated in separate calls.

- Workflow recipes are matched internally by workflow name. If a recipe is not found, that workflow is skipped with a log message.

- Machine metadata (CPU/system/memory) is attached to generated tasks.

## Examples

Generate one workflow with default scale:

```bash
python wf_gen.py --workflow ./workflows/montage-chameleon-2mass-005d-001.json
```

Generate one workflow and both CPU-only and IO-only variants:

```bash
python wf_gen.py --workflow ./workflows/montage-chameleon-2mass-005d-001.json --cpu-only --io-only
```

Generate all workflows at half scale:

```bash
python wf_gen.py --all --scale 0.5
```

Use a fixed calibration reference and enable debug logs:

```bash
python wf_gen.py --all --ref 0.32 --debug
```

