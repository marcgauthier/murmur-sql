# Scheduled soak batteries

The full-duration scenarios are too long for `run.sh all` (which uses
short test defaults) and run on a weekly schedule instead. Each job
runs alone on the box, reports per-scenario `LIVE_RESULT` lines plus
failure artifacts, and never fixes or reruns.

- Sundays 02:17 local: `./tests-live/run.sh soak` — `files-soak`
  (10 min) + `soak-slo` (2 h + 5 min settle) +
  `long-running-five-node` (1 h + 5 min settle). About 3.5 h.
- Wednesdays 02:17 local: `./tests-live/run.sh endurance-chaos` at
  test defaults (600 s chaos + 300 s settle, about 20 min), then
  `./tests-live/run.sh stress` (5x rolling-restart, partial-mesh,
  crash-recovery, partition, snapshot-resync, about 30 min).

Manual equivalent (from `/home/marc/MURMUR-SQL/murmur`, box otherwise
idle):

```sh
MURMUR_LIVE_CONTINUE_ON_FAILURE=1 GOCACHE=/tmp/gocache GOTOOLCHAIN=local ./tests-live/run.sh soak
MURMUR_LIVE_CONTINUE_ON_FAILURE=1 GOCACHE=/tmp/gocache GOTOOLCHAIN=local ./tests-live/run.sh endurance-chaos
MURMUR_LIVE_CONTINUE_ON_FAILURE=1 GOCACHE=/tmp/gocache GOTOOLCHAIN=local ./tests-live/run.sh stress
```

The jobs are permanent recurring schedules; list or remove them with
the session `cron_list` / `cron_delete` tools.
