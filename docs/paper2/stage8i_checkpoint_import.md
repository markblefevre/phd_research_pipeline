# Paper 2 checkpoint — import and Git procedure

This bundle is **commit-ready**, not a completed Git commit. It contains:

- `scripts/paper2/audit_stage8i_sample_attrition.py`: read-only row-level Stage 8I attrition verifier.
- `docs/paper2/stage8i_sample_attrition.md`: checkpoint findings, methodology, and remaining checks.
- `docs/paper2/stage8i_diagnostics/`: four small Stage 8I CSV outputs and `SHA256SUMS.txt`.

From your **repository root** on the Mac (not the NAS storage root):

```bash
# Unpack into scripts/paper2/ and docs/paper2/ under the repo root.
unzip -n ~/Downloads/paper2_stage8i_attrition_checkpoint.zip -d .

# Check file names and current tracked/untracked changes.
git status --short

# Run source and data checks before making the checkpoint commit.
python -m py_compile scripts/paper2/audit_stage8i_sample_attrition.py
python -u scripts/paper2/test_stage8i_statistical_diagnostics.py
python -u scripts/paper2/audit_stage8i_sample_attrition.py

# Review data-driven audit output before changing STATUS.md or PIPELINE.md.
cat data/interim/paper2/regressions/stage8i_sample_attrition_audit/missing_controls.csv
cat data/interim/paper2/regressions/stage8i_sample_attrition_audit/component_presence_patterns.csv
cat data/interim/paper2/regressions/stage8i_sample_attrition_audit/stage8d_to_8h_sample_migration.csv

# Stage the new small reproducibility artifacts.
git add scripts/paper2/audit_stage8i_sample_attrition.py \
  docs/paper2/stage8i_sample_attrition.md \
  docs/paper2/stage8i_diagnostics/

# Also stage any outstanding Stage 8H/I production scripts or updated STATUS/PIPELINE,
# *after reviewing* git status; do not indiscriminately git add the large data tree.

git diff --cached --check
git diff --cached --stat
# Only after verifying the staged file list:
git commit -m "paper2: document Stage 8I inference and audit sample attrition"
```

If the default audit output directory already exists, provide a different path with `--output-dir ..._rerun`. Do not overwrite the authoritative prior results.

**Important:** If any of the project files (e.g. Stage 8I test script or the local stage 8E score run) are missing, stop at the failed command and investigate rather than committing a misleading reproduction claim.
