import argparse
from datetime import datetime, time, timedelta
from pyspark.sql import SparkSession, functions as F

INPUT_PATH = 's3a://de-course/raw/payments'
OUTPUT_PATH = 's3a://de-course/staging/stg_payments'
S3_ENDPOINT = 'http://minio:9000'
S3_ACCESS_KEY = 'minioadmin'
S3_SECRET_KEY = 'minioadmin'
parser = argparse.ArgumentParser(description='Staging load for staging/stg_payments')
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
    .appName('load-stg-payments')
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
paid_ts = F.col('paid_at').cast('timestamp')
df_filtered = df_raw.filter(
    paid_ts.isNull() | ((paid_ts >= start_ts) & (paid_ts < end_ts))
)
df_stg_payments = df_filtered.select(
    F.col('payment_id'),
    F.col('order_id'),
    paid_ts.alias('paid_at'),
    F.col('payment_status'),
    F.col('amount'),
    ((F.col('payment_status') == 'success') & (F.col('amount') < 0)).alias('is_refund'),
    F.to_date(paid_ts).alias('paid_dt'),
)
(
    df_stg_payments.write
    .format('parquet')
    .mode('overwrite')
    .partitionBy('paid_dt')
    .save(OUTPUT_PATH)
)
spark.stop()