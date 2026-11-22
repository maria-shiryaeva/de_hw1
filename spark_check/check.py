import argparse
import sys
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, functions as F

S3_ENDPOINT = "http://minio:9000"
S3_ACCESS_KEY = "minioadmin"
S3_SECRET_KEY = "minioadmin"

parser = argparse.ArgumentParser(description="Validate store_hse layers for load-date window")
parser.add_argument("--load-date", required=True, help="Дата запуска YYYY-MM-DD")
parser.add_argument("--run-id", required=False, default="manual", help="Идентификатор запуска")
args = parser.parse_args()

load_date_dt = datetime.strptime(args.load_date, "%Y-%m-%d").date()
window_start_dt = load_date_dt - timedelta(days=3)

spark = (
    SparkSession.builder
    .master("spark://spark-master:7077")
    .appName(f"check-store-hse-{args.load_date}")
    .config("spark.driver.host", "jupyter")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.3.0")
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

start_lit = F.lit(window_start_dt).cast("date")
end_lit = F.lit(load_date_dt).cast("date")

errors = []

try:
    stg_orders = (
        spark.read.format("parquet")
        .load("s3a://de-course/staging/stg_orders")
        .filter((F.col("order_date") >= start_lit) & (F.col("order_date") < end_lit))
    )
    if len(stg_orders.columns) != 7:
        errors.append(f"stg_orders: expected 7 cols, got {len(stg_orders.columns)}")
    ord_cnt = stg_orders.count()
    ord_uniq = stg_orders.select("order_id", "order_date").distinct().count()
    if ord_cnt != ord_uniq:
        errors.append(f"stg_orders: duplicate (order_id, order_date), rows={ord_cnt}, uniq={ord_uniq}")

    stg_payments = (
        spark.read.format("parquet")
        .load("s3a://de-course/staging/stg_payments")
        .filter(
            F.col("paid_dt").isNull()
            | ((F.col("paid_dt") >= start_lit) & (F.col("paid_dt") < end_lit))
        )
    )
    if len(stg_payments.columns) != 7:
        errors.append(f"stg_payments: expected 7 cols, got {len(stg_payments.columns)}")
    null_paid_non_null_dt = stg_payments.filter(F.col("paid_at").isNull() & F.col("paid_dt").isNotNull()).count()
    if null_paid_non_null_dt > 0:
        errors.append(f"stg_payments: found {null_paid_non_null_dt} rows with paid_at IS NULL and paid_dt IS NOT NULL")

    int_df = (
        spark.read.format("parquet")
        .load("s3a://de-course/intermediate/int_orders_with_payments")
        .filter((F.col("order_date") >= start_lit) & (F.col("order_date") < end_lit))
    )
    if len(int_df.columns) != 16:
        errors.append(f"int_orders_with_payments: expected 16 cols, got {len(int_df.columns)}")
    int_cnt = int_df.count()
    int_uniq = int_df.select("order_id", "order_date").distinct().count()
    if int_cnt != ord_cnt:
        errors.append(f"int_orders_with_payments: rows={int_cnt} != stg_orders window rows={ord_cnt}")
    if int_cnt != int_uniq:
        errors.append(f"int_orders_with_payments: duplicate (order_id, order_date), rows={int_cnt}, uniq={int_uniq}")

    bad_unpaid = int_df.filter(
        (F.col("successful_payments_count") == 0)
        & (
            (F.col("paid_amount") != 0)
            | (F.col("net_paid_amount") != 0)
            | F.col("first_paid_at").isNotNull()
            | (F.col("is_confirmed") != F.lit(False))
        )
    ).count()
    if bad_unpaid > 0:
        errors.append(f"int_orders_with_payments: {bad_unpaid} rows without successful payments violate expectations")

    fct_df = (
        spark.read.format("parquet")
        .load("s3a://de-course/marts/fct_orders")
        .filter((F.col("order_date") >= start_lit) & (F.col("order_date") < end_lit))
    )
    if len(fct_df.columns) != 12:
        errors.append(f"fct_orders: expected 12 cols, got {len(fct_df.columns)}")
    fct_cnt = fct_df.count()
    fct_uniq = fct_df.select("order_id", "order_date").distinct().count()
    expected_fct_cnt = int_df.filter(~F.col("is_test_order")).count()
    if fct_cnt != expected_fct_cnt:
        errors.append(f"fct_orders: rows={fct_cnt} != expected non-test rows={expected_fct_cnt}")
    if fct_cnt != fct_uniq:
        errors.append(f"fct_orders: duplicate (order_id, order_date), rows={fct_cnt}, uniq={fct_uniq}")

    if errors:
        print(f"[FAIL] run_id={args.run_id} load_date={args.load_date}: " + "; ".join(errors))
        sys.exit(1)
    else:
        print(
            f"[PASS] run_id={args.run_id} load_date={args.load_date} "
            f"window=[{window_start_dt}, {load_date_dt}) stg_orders={ord_cnt} int={int_cnt} fct={fct_cnt}"
        )
finally:
    spark.stop()