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
OUTPUT_PATH = 's3a://de-course/raw/order_items'
spark = (
    SparkSession.builder
    .appName('load-order-items')
    .config('spark.hadoop.fs.s3a.endpoint', S3_ENDPOINT)
    .config('spark.hadoop.fs.s3a.access.key', S3_ACCESS_KEY)
    .config('spark.hadoop.fs.s3a.secret.key', S3_SECRET_KEY)
    .config('spark.hadoop.fs.s3a.path.style.access', 'true')
    .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false')
    .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
    .getOrCreate()
)
spark.sparkContext.setLogLevel('WARN')

# инкрементальная загрузка по окну невозможна, так как нет колонок с датой или временем, и делать партиционирование по времени не получится

query = '''
    (SELECT order_id,
        product_id,
        quantity,
        unit_price,
        discount_amount
        FROM public.order_items) AS order_items_full
'''
df_order_items = (
    spark.read.format('jdbc')
    .option('url', PG_URL)
    .option('dbtable', query)
    .options(**PG_PROPERTIES)
    .load()
)
(
    df_order_items.write
    .format('parquet')
    .mode('overwrite')
    .save(OUTPUT_PATH)
)

spark.stop()