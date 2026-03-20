# Translating wfbench workflows to PARSL

This document describes the CLI parameters accepted by `translator/translator.py`.

## Script purpose

`translator.py` converts WfBench workflow benchmark JSON files into executable Parsl workflow applications.

The translator:
- generates a `parsl_workflow.py` file from the workflow JSON
- copies necessary binary files
- generates input files
- creates directory structure (logs/, output/, jsons/)
- produces Parsl bash apps with dependency management

## Usage

```bash
python translator.py (--workflow <path> | --all | --io-only | --cpu-only | --cpu-io) [options]
```

Exactly one workflow selection flag is required from the mutually exclusive group.

## Parameters

### Required selection (one required)

- `--workflow <path>`
  - Path to a single WfFormat JSON workflow benchmark file to translate.
  - Example: `--workflow ./benchmarks/montage_1.0/montage_1.0.json`

- `--all`
  - Translate all workflow JSON files found in `./benchmarks` (recursively).
  - Processes every `.json` file in subdirectories under `./benchmarks`.

- `--io-only`
  - Translate only workflows whose directory name ends with `_io`.
  - Processes workflows in folders like `./benchmarks/montage_1.0_io/`.

- `--cpu-only`
  - Translate only workflows whose directory name ends with `_cpu`.
  - Processes workflows in folders like `./benchmarks/montage_1.0_cpu/`.

- `--cpu-io`
  - Translate workflows that do NOT have `_io` or `_cpu` suffix.
  - Effectively targets the "normal" benchmark variants.

### Optional parameters

- `--outdir <path>`
  - Default: `./parsl_script` (relative to current working directory)
  - Output directory where translated Parsl workflows will be stored.
  - Each workflow creates a subdirectory named after the workflow.
  - Example: `--outdir /tmp/translated_workflows`

- `--clean`
  - Generate a "clean" Parsl translation without scheduling metadata.
  - Uses `parsl_template_clean.py` instead of `parsl_template.py`.
  - Omits `parsl_resource_specification` from task definitions.

## Output structure

For each workflow, the translator creates:

```
<outdir>/<workflow_name>/
├── parsl_workflow.py       # Generated Parsl workflow script
├── start_workers.sh        # Worker startup script
├── kill_docker.sh          # Docker cleanup script
├── jsons/
│   ├── workflow.json       # Copy of source workflow
│   └── template.json       # Configuration template
├── logs/                   # Task stdout/stderr logs
└── output/                 # Task output files
```

## Behavior notes

- If the output directory already exists, the script prompts for confirmation before overwriting.

- Task dependencies are automatically resolved based on the workflow DAG structure.

- The translator computes:
  - Task levels using topological sort
  - Bottom level (critical path) for each task
  - Resource specifications (data size, computation, dependencies)

- Input/output file mappings are preserved from the benchmark definition.

- The generated workflow includes a barrier and timing instrumentation.

## Examples

Translate a single workflow:

```bash
python translator.py --workflow ./benchmarks/montage_1.0/montage_1.0.json
```

Translate all workflows:

```bash
python translator.py --all
```

Translate only CPU-only variants to a specific directory:

```bash
python translator.py --cpu-only --outdir /scratch/parsl_workflows
```

Generate clean translations (without scheduling info) for all IO-only workflows:

```bash
python translator.py --io-only --clean
```

Translate normal (non-IO, non-CPU) workflows:

```bash
python translator.py --cpu-io
```

