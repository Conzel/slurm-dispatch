# submit

Submit jobs to the least-loaded SLURM cluster with one command.

## Setup

1. Install: `swift build -c release`, copy `.build/release/submit` to your `$PATH`
2. Configure: `submit --init`, then edit `~/.config/submit/config.json`
   - Set your cluster hostnames, SSH keys, and `remote_repo_dir` paths
   - Point `git` at your experiment repo (`remote_url` + `remote_branch`)
   - Point `s3` at your bucket for results and logs

## Usage

```sh
submit example.sh
```

## What happens

1. SSHs into all clusters, picks the one with fewest queued/running jobs
2. Clones the git repo into `remote_repo_dir` (or pulls if it exists)
3. Stages `.s3cfg` and `container.sif` into the repo dir if missing
4. SCPs your script + a generated wrapper into `logs/`
5. Runs `sbatch` — on exit/error, the wrapper uploads results and logs to S3

Results land at `results_upload_path/<results_folder>/`, logs at `logs_upload_path/<slurm_id>/`.
