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
OUTPUT_PATH = 's3a://de-course/raw/products'
spark = (
    SparkSession.builder
    .appName('load-products')
    .config('spark.hadoop.fs.s3a.endpoint', S3_ENDPOINT)
    .config('spark.hadoop.fs.s3a.access.key', S3_ACCESS_KEY)
    .config('spark.hadoop.fs.s3a.secret.key', S3_SECRET_KEY)
    .config('spark.hadoop.fs.s3a.path.style.access', 'true')
    .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false')
    .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
    .getOrCreate()
)
spark.sparkContext.setLogLevel('WARN')
query = '''
    (SELECT product_id,
        category,
        brand,
        product_name
        FROM public.products) AS products_full
'''
df_products = (
    spark.read.format('jdbc')
    .option('url', PG_URL)
    .option('dbtable', query)
    .options(**PG_PROPERTIES)
    .load()
)
(
    df_products.write
    .format('parquet')
    .mode('overwrite')
    .save(OUTPUT_PATH)
)
spark.stop()