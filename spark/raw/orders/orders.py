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
OUTPUT_PATH = 's3a://de-course/raw/orders'
parser = argparse.ArgumentParser(description='Incremental load for raw/orders')
parser.add_argument(
    '--load-date',
    required=False,
    default='2026-03-05',
    help='Дата запуска в формате YYYY-MM-DD',
)
args = parser.parse_args()
load_date_dt = datetime.strptime(args.load_date, '%Y-%m-%d').date()
window_start_dt = load_date_dt - timedelta(days=3)

# 4. partitionOverwriteMode=dynamic нужен, чтобы при mode('overwrite') перезаписывались только партиции order_created_dt из текущего батча,
# а остальные партиции в приёмнике не затирались.

spark = (
    SparkSession.builder
    .appName('load-orders')
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
        f"order_created_at >= TIMESTAMP '{window_start_dt.isoformat()} 00:00:00' "
        f"AND order_created_at < TIMESTAMP '{load_date_dt.isoformat()} 00:00:00'"
    )
query = f'''
    (SELECT order_id,
        customer_id,
        order_created_at,
        status,
        total_amount
        FROM public.orders
        WHERE {where_clause}) AS orders_batch
'''
df_orders = (
    spark.read.format('jdbc')
    .option('url', PG_URL)
    .option('dbtable', query)
    .options(**PG_PROPERTIES)
    .load()
)

df_result = df_orders.withColumn(
    'order_created_dt',
    F.to_date(F.col('order_created_at')),
)
(
    df_result.write
    .format('parquet')
    .mode('overwrite')
    .partitionBy('order_created_dt')
    .save(OUTPUT_PATH)
)
spark.stop()