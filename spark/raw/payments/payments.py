import argparse
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, functions as F

PG_URL = 'jdbc:postgresql://postgres:5432/shop'
PG_PROPERTIES = {
    'user': 'student',
    'password': 'studentpass',
    'driver': 'org.postgresql.Driver',
}
S3_ENDPOINT = 'http://minio:9000'
S3_ACCESS_KEY = 'minioadmin'
S3_SECRET_KEY = 'minioadmin'
OUTPUT_PATH = 's3a://de-course/raw/payments'
parser = argparse.ArgumentParser(description='Incremental load for raw/payments')
parser.add_argument(
    '--load-date',
    required=False,
    default='2026-03-05',
    help='Дата запуска в формате YYYY-MM-DD',
)
args = parser.parse_args()
load_date_dt = datetime.strptime(args.load_date, '%Y-%m-%d').date()
window_start_dt = load_date_dt - timedelta(days=3)

# 4. partitionOverwriteMode=dynamic нужен, чтобы при mode('overwrite') перезаписывались только партиции paid_dt
# из текущего батча, а остальные партиции не затирались.

spark = (
    SparkSession.builder
    .appName('load-payments')
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

def check_sink_has_data(spark_session: SparkSession, path: str) -> bool:
    '''Проверяет, существуют ли уже данные в целевом пути S3.'''
    jvm = spark_session._jvm
    hadoop_conf = spark_session._jsc.hadoopConfiguration()
    fs_path = jvm.org.apache.hadoop.fs.Path(path)
    fs = fs_path.getFileSystem(hadoop_conf)
    if not fs.exists(fs_path):
        return False
    statuses = fs.listStatus(fs_path)
    return len(statuses) > 0

sink_has_data = check_sink_has_data(spark, OUTPUT_PATH)
if not sink_has_data:
    where_clause = 'TRUE'
else:
    where_clause = (
        f"((paid_at >= TIMESTAMP '{window_start_dt.isoformat()} 00:00:00' "
        f"AND paid_at < TIMESTAMP '{load_date_dt.isoformat()} 00:00:00') "
        f"OR paid_at IS NULL)"
    )
query = f'''
    (SELECT payment_id,
        order_id,
        paid_at,
        payment_status,
        amount
        FROM public.payments
        WHERE {where_clause}) AS payments_batch
'''
df_payments = (
    spark.read.format('jdbc')
    .option('url', PG_URL)
    .option('dbtable', query)
    .options(**PG_PROPERTIES)
    .load()
)
df_result = df_payments.withColumn(
    'paid_dt',
    F.to_date(F.col('paid_at')),
)
(
    df_result.write
    .format('parquet')
    .mode('overwrite')
    .partitionBy('paid_dt')
    .save(OUTPUT_PATH)
)

spark.stop()