import argparse
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, functions as F

ORDERS_PATH = 's3a://de-course/staging/stg_orders'
PAYMENTS_PATH = 's3a://de-course/staging/stg_payments'
OUTPUT_PATH = 's3a://de-course/intermediate/int_orders_with_payments'
S3_ENDPOINT = 'http://minio:9000'
S3_ACCESS_KEY = 'minioadmin'
S3_SECRET_KEY = 'minioadmin'
parser = argparse.ArgumentParser(description='Intermediate load for intermediate/int_orders_with_payments')
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
    .appName('load-int-orders-with-payments')
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
orders_df = (
    spark.read.format('parquet').load(ORDERS_PATH)
    .filter((F.col('order_date') >= start_date_lit) & (F.col('order_date') < end_date_lit))
    .alias('o')
)
payments_df = (
    spark.read.format('parquet').load(PAYMENTS_PATH)
    .filter(
        F.col('paid_dt').isNull()
        | ((F.col('paid_dt') >= start_date_lit) & (F.col('paid_dt') < end_date_lit))
    )
)
amount_type = payments_df.schema['amount'].dataType
zero_amount = F.lit(0).cast(amount_type)
zero_count = F.lit(0).cast('long')
valid_success = (F.col('payment_status') == 'success') & (~F.col('is_refund'))
order_payments_df = (
    payments_df.groupBy('order_id')
    .agg(
        F.count(F.when(valid_success, 1)).alias('successful_payments_count'),
        F.coalesce(
            F.sum(F.when(valid_success, F.col('amount'))),
            zero_amount,
        ).alias('paid_amount'),
        F.coalesce(
            -F.sum(F.when(F.col('is_refund'), F.col('amount'))),
            zero_amount,
        ).alias('refunded_amount'),
        F.min(F.when(valid_success, F.col('paid_at'))).alias('first_paid_at'),
        F.count(F.when(F.col('payment_status') == 'success', 1)).alias('success_operations_count'),
        F.count(F.when(F.col('payment_status') == 'failed', 1)).alias('failed_operations_count'),
        F.count(F.when(F.col('payment_status') == 'pending', 1)).alias('pending_operations_count'),
    )
    .alias('p')
)
paid_col = F.coalesce(F.col('p.paid_amount'), zero_amount)
refunded_col = F.coalesce(F.col('p.refunded_amount'), zero_amount)
success_cnt_col = F.coalesce(F.col('p.successful_payments_count'), zero_count)
int_df = (
    orders_df.join(order_payments_df, on='order_id', how='left')
    .select(
        F.col('o.order_id').alias('order_id'),
        F.col('o.customer_id').alias('customer_id'),
        F.col('o.ordered_at').alias('ordered_at'),
        F.col('o.order_date').alias('order_date'),
        F.col('o.order_status').alias('order_status'),
        F.col('o.order_amount').alias('order_amount'),
        F.col('o.is_test_order').alias('is_test_order'),
        success_cnt_col.alias('successful_payments_count'),
        paid_col.alias('paid_amount'),
        refunded_col.alias('refunded_amount'),
        (paid_col - refunded_col).alias('net_paid_amount'),
        F.col('p.first_paid_at').alias('first_paid_at'),
        ((F.col('o.order_status') == 'paid') & (success_cnt_col > 0)).alias('is_confirmed'),
        F.coalesce(F.col('p.success_operations_count'), zero_count).alias('success_operations_count'),
        F.coalesce(F.col('p.failed_operations_count'), zero_count).alias('failed_operations_count'),
        F.coalesce(F.col('p.pending_operations_count'), zero_count).alias('pending_operations_count'),
    )
)

(
    int_df.write
    .format('parquet')
    .mode('overwrite')
    .partitionBy('order_date')
    .save(OUTPUT_PATH)
)

spark.stop()