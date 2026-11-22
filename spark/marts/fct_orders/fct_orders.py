import argparse
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, functions as F

INPUT_PATH = 's3a://de-course/intermediate/int_orders_with_payments'
OUTPUT_PATH = 's3a://de-course/marts/fct_orders'
S3_ENDPOINT = 'http://minio:9000'
S3_ACCESS_KEY = 'minioadmin'
S3_SECRET_KEY = 'minioadmin'
parser = argparse.ArgumentParser(description='Mart load for marts/fct_orders')
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
    .appName('load-fct-orders')
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
start_date_lit = F.lit(window_start_dt).cast('date')
end_date_lit = F.lit(load_date_dt).cast('date')
df_fct_orders = (
    spark.read.format('parquet').load(INPUT_PATH)
    .filter((F.col('order_date') >= start_date_lit) & (F.col('order_date') < end_date_lit))
    .filter(~F.col('is_test_order'))
    .select(
        F.col('order_id'),
        F.col('customer_id'),
        F.col('order_date'),
        F.col('ordered_at'),
        F.col('order_status'),
        F.col('order_amount'),
        F.col('paid_amount'),
        F.col('refunded_amount'),
        F.col('net_paid_amount'),
        F.col('successful_payments_count'),
        F.col('first_paid_at'),
        F.col('is_confirmed'),
    )
)
(
    df_fct_orders.write
    .format('parquet')
    .mode('overwrite')
    .partitionBy('order_date')
    .save(OUTPUT_PATH)
)

spark.stop()