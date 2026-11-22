import argparse
from pyspark.sql import SparkSession

PG_URL = 'jdbc:postgresql://postgres:5432/shop'
PG_PROPERTIES = {
    'user': 'student',
    'password': 'studentpass',
    'driver': 'org.postgresql.Driver',
}
S3_ENDPOINT = 'http://minio:9000'
S3_ACCESS_KEY = 'minioadmin'
S3_SECRET_KEY = 'minioadmin'
OUTPUT_PATH = 's3a://de-course/raw/customers'
parser = argparse.ArgumentParser(description='Full snapshot load for raw/customers')
parser.add_argument('--load-date', required=False, help='Дата запуска YYYY-MM-DD')
parser.parse_args()
spark = (
    SparkSession.builder
    .appName('load-customers')
    .config('spark.hadoop.fs.s3a.endpoint', S3_ENDPOINT)
    .config('spark.hadoop.fs.s3a.access.key', S3_ACCESS_KEY)
    .config('spark.hadoop.fs.s3a.secret.key', S3_SECRET_KEY)
    .config('spark.hadoop.fs.s3a.path.style.access', 'true')
    .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false')
    .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
    .getOrCreate()
)
spark.sparkContext.setLogLevel('WARN')

# 2. WHERE не нужен, так как таблица маленькая и загружается полным снимком
# 4. Окно не применяется, так как мы всегда читаем актуальный полный снимок целиком, данные не догружаются по дате.

query = '''
    (SELECT customer_id,
        registered_at,
        city,
        acquisition_channel
        FROM public.customers
    ) AS customers_snapshot
'''
df_customers = (
    spark.read.format('jdbc')
    .option('url', PG_URL)
    .option('dbtable', query)
    .options(**PG_PROPERTIES)
    .load()
)

# 3. partitionBy не используется, так как справочник маленький, и делить его на партиции не нужно
# 5. проверка sink_has_data не нужна, так как mode('overwrite') целиком перезаписывает при каждом запуске, и дубликатов нет

(
    df_customers.write
    .format('parquet')
    .mode('overwrite')
    .save(OUTPUT_PATH)
)
spark.stop()