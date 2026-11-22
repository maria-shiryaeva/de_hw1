import argparse
from datetime import datetime, time, timedelta
from pyspark.sql import SparkSession, functions as F

INPUT_PATH = 's3a://de-course/raw/orders'
OUTPUT_PATH = 's3a://de-course/staging/stg_orders'
S3_ENDPOINT = 'http://minio:9000'
S3_ACCESS_KEY = 'minioadmin'
S3_SECRET_KEY = 'minioadmin'
parser = argparse.ArgumentParser(description='Staging load for staging/stg_orders')
parser.add_argument(
    '--load-date',
    required=False,
    default='2026-03-05',
    help='Дата запуска в формате YYYY-MM-DD',
)
args = parser.parse_args()
load_date_dt = datetime.strptime(args.load_date, '%Y-%m-%d').date()
window_start_dt = load_date_dt - timedelta(days=3)
spark = (
    SparkSession.builder
    .appName('load-stg-orders')
    .config('spark.hadoop.fs.s3a.endpoint', S3_ENDPOINT)
    .config('spark.hadoop.fs.s3a.access.key', S3_ACCESS_KEY)
    .config('spark.hadoop.fs.s3a.secret.key', S3_SECRET_KEY)
    .config('spark.hadoop.fs.s3a.path.style.access', 'true')
    .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false')
    .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
    .config('spark.sql.sources.partitionOverwriteMode', 'dynamic')
    .getOrCreate()
)
spark.sparkContext.setLogLevel('WARN')
start_ts = F.lit(datetime.combine(window_start_dt, time.min)).cast('timestamp')
end_ts = F.lit(datetime.combine(load_date_dt, time.min)).cast('timestamp')
df_raw = spark.read.format('parquet').load(INPUT_PATH)
df_filtered = df_raw.filter(
    (F.col('order_created_at').cast('timestamp') >= start_ts)
    & (F.col('order_created_at').cast('timestamp') < end_ts)
)
df_stg_orders = df_filtered.select(
    F.col('order_id'),
    F.col('customer_id'),
    F.col('order_created_at').cast('timestamp').alias('ordered_at'),
    F.col('order_created_at').cast('date').alias('order_date'),
    F.col('status').alias('order_status'),
    F.col('total_amount').alias('order_amount'),
    (F.col('status') == 'test').alias('is_test_order'),
)
(
    df_stg_orders.write
    .format('parquet')
    .mode('overwrite')
    .partitionBy('order_date')
    .save(OUTPUT_PATH)
)

spark.stop()