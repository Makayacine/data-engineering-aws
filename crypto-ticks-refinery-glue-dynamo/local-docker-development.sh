#!/usr/bin/env bash
# Run the refinery inside the Glue 4.0 image, on the committed two-hour sample.
#
#   ./local-docker-development.sh [selfcheck|ingest|path1|path2|path3|dynamo|all]
#
# The reference lab runs ONE hardcoded SCRIPT_FILE_NAME. That cannot express this project for
# two reasons. These five scripts are a PIPELINE, not a menu -- the three paths consume the
# entryway's bars and the loader consumes all three -- so the default is `all`. And they are
# not all Spark: glue-dynamo.py is a Glue PYTHON SHELL job, run with `python3` and never
# spark-submitted, the same split the deployed job definitions make.
#
# No ~/.aws mount and no AWS_PROFILE, unlike the lab. Every stage below reads the committed
# sample and writes into the workspace, so credentials inside the container would buy nothing;
# the mount's only effect on a host without ~/.aws is to create one, owned by root.
set -euo pipefail

IMAGE=amazon/aws-glue-libs:glue_libs_4.0.0_image_01
WORKSPACE=/home/glue_user/workspace
OUT=_localrun/docker
RUN_ID=2025-01-sample

# The image's ENTRYPOINT is `bash -l`, so the container command is handed to a LOGIN SHELL,
# not exec'd. `docker run IMAGE python3 job.py` therefore asks bash to interpret an ELF binary
# as a shell script and exits 126 with "cannot execute binary file" -- while `spark-submit`
# survives only because it happens to be a shell script itself. `-c "$*"` is the one form that
# works for both, so both go through it. No argument below contains a space.
#
# DISABLE_SSL=true is not cargo-culted from the lab: without it the login profile generates a
# self-signed keystore on every container start. The Spark UI is deliberately NOT published --
# these are batch jobs that exit on their own, and -p 4040:4040 would abort the whole chain
# whenever a native `--local` run (or an orphaned Spark JVM) already holds the host port.
#
# MSYS_NO_PATHCONV: Git Bash rewrites the container-side /home/glue_user/... into a Windows
# path before docker ever sees it, and the mount then lands somewhere nothing reads.
glue() {
  MSYS_NO_PATHCONV=1 docker run --rm \
    -v "$(pwd):${WORKSPACE}" -w "${WORKSPACE}" \
    -e DISABLE_SSL=true \
    "${IMAGE}" -c "$*"
}

stage=${1:-all}
case "${stage}" in
  selfcheck)
    # Seconds each, and no Spark job runs -- but the three path modules, refinery_common and
    # the loader are IMPORTED under the image's real Spark 3.3.0 / Python 3.10, which is what
    # the strip test only simulates locally. Cheap enough to run before committing to the rest.
    for p in path1 path2 path3; do
      glue spark-submit "glue-jobs/glue-refinery-${p}.py" --self-check
    done
    glue python3 glue-jobs/glue-dynamo.py --self-check
    ;;
  ingest)
    # The calendar overrides are what make a two-hour sample meaningful: the bar grid is a
    # DECLARED calendar, so against the default month-wide one Step 3 truthfully reports
    # 1,606,390 of 1,607,040 bars missing.
    glue spark-submit glue-jobs/glue-ingest-bars.py --local \
      --input data/sample --bars-output "${OUT}/bars" --bar-interval 5s \
      --calendar-start 2025-01-01T00:00:00 --calendar-end 2025-01-01T02:00:00
    ;;
  path1|path2|path3)
    glue spark-submit "glue-jobs/glue-refinery-${stage}.py" --local \
      --input "${OUT}/bars" --output "${OUT}/${stage}"
    ;;
  dynamo)
    # --dry-run builds and boto3-encodes every item and opens no connection: no account, no
    # credentials, no region, and no table has to exist.
    glue python3 glue-jobs/glue-dynamo.py --dry-run --run-id "${RUN_ID}" \
      --path1 "${OUT}/path1" --path2 "${OUT}/path2" --path3 "${OUT}/path3"
    ;;
  all)
    for s in selfcheck ingest path1 path2 path3 dynamo; do bash "$0" "$s"; done
    ;;
  *)
    echo "usage: $0 [selfcheck|ingest|path1|path2|path3|dynamo|all]" >&2
    exit 2
    ;;
esac
