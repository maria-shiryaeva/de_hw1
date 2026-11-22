set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.driver.host=jupyter \
  --packages org.postgresql:postgresql:42.7.3,org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.3.0 \
  "$SCRIPT_DIR/fct_orders.py" "$@"