set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
spark-submit \
  --master spark://spark-master:7077 \
  --name load-customers \
  --deploy-mode client \
  --conf spark.executor.instances=1 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=1g \
  --conf spark.driver.memory=1g \
  --conf spark.dynamicAllocation.enabled=false \
  --packages org.postgresql:postgresql:42.7.3,org.apache.hadoop:hadoop-aws:3.3.4 \
  "$SCRIPT_DIR/customers.py" "$@"