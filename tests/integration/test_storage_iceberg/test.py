import helpers.client
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

import pyspark
import logging
import os
import json
import pytest
import time
import glob
import shutil

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DateType,
    TimestampType,
    BooleanType,
    ArrayType,
)
from pyspark.sql.functions import current_timestamp
from datetime import datetime
from pyspark.sql.functions import monotonically_increasing_id, row_number
from pyspark.sql.window import Window
from pyspark.sql.readwriter import DataFrameWriter, DataFrameWriterV2

from helpers.s3_tools import prepare_s3_bucket, upload_directory, get_file_contents

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
CURRENT_TEST_DIR = os.path.dirname(os.path.abspath(__file__))

def get_spark(warehouse="/iceberg_data"):
    os.environ["AWS_ACCESS_KEY_ID"] = "minio"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "minio123"
    os.environ["AWS_REGION"] = "eu-east-1"
    builder = (
        pyspark.sql.SparkSession.builder.appName("spark_test")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.iceberg.spark.SparkSessionCatalog",
        )
        #.config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.spark_catalog.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
        .config("spark.sql.catalog.spark_catalog.uri", "http://127.0.0.1:8181")
        .config("spark.sql.catalog.spark_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://warehouse/wh/")
        .config("spark.sql.catalog.spark_catalog.s3.endpoint", "http://minio:9001")
        .config("spark.sql.defaultCatalog", "spark_catalog")
        .config("spark.eventLog.enabled", "false")
        #.config("spark.eventLog.dir", "/home/iceberg/spark-events")
        #.config("spark.history.fs.logDirectory", "/home/iceberg/spark-events")
        .config("spark.sql.catalogImplementation", "in-memory")
        #.config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
        #.config("spark.sql.catalog.spark_catalog.type", "hadoop")
        #.config("spark.sql.catalog.spark_catalog.warehouse", "s3a://warehouse/")
        #.config("spark.sql.catalog.spark_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        #.config("spark.sql.catalog.spark_catalog.s3_endpoint", "http://minio:9000/warehouse/")
        #.config("spark.hadoop.fs.s3a.access.key", "admin") \
        #.config("spark.hadoop.fs.s3a.secret.key", "password") \
        #.config("spark.hadoop.fs.s3a.endpoint", "minio:9000/warehouse/") \
        #.config("spark.hadoop.fs.s3a.path.style.access", "true") \
        #.config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000") \
        #.config("spark.hadoop.fs.s3a.connection.timeout", "10000") \
        #.master("local")
    )
    return builder.master("local").getOrCreate()

#def get_spark():
#    builder = (
#        pyspark.sql.SparkSession.builder.appName("spark_test")
#        .config(
#            "spark.sql.catalog.spark_catalog",
#            "org.apache.iceberg.spark.SparkSessionCatalog",
#        )
#        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
#        .config("spark.sql.catalog.spark_catalog.type", "hadoop")
#        .config("spark.sql.catalog.spark_catalog.warehouse", "/iceberg_data")
#        .master("local")
#    )
#    return builder.master("local").getOrCreate()


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__, with_spark=True)
        instance = cluster.add_instance(
            "node1",
            main_configs=["configs/config.d/named_collections.xml"],
            with_minio=True,
        )

        logging.info("Starting cluster...")
        cluster.start()

        print("kssenii 1")

        instance.exec_in_container(
            [
                "bash",
                "-c",
                "ls -la",
            ],
            user="root",
        )
        shutil.copyfile(
            os.path.join(CURRENT_TEST_DIR, "./configs/spark_defaults_s3.conf"),
            "/spark-3.3.2-bin-hadoop3/conf/spark_defaults.conf"
        )
        print("kssenii 2")

        prepare_s3_bucket(cluster)
        logging.info("S3 bucket created")

        print("kssenii 3")
        cluster.spark_session = get_spark()
        print("kssenii 4")

        yield cluster

    finally:
        #time.sleep(500)
        pass
        #cluster.shutdown()


def run_query(instance, query, stdin=None, settings=None):
    # type: (ClickHouseInstance, str, object, dict) -> str

    logging.info("Running query '{}'...".format(query))
    result = instance.query(query, stdin=stdin, settings=settings)
    logging.info("Query finished")

    return result


def write_iceberg_from_file(
    spark, path, table_name, mode="overwrite", format_version="1", partition_by=None
):
    if mode == "overwrite":
        if partition_by is None:
            spark.read.load(f"file://{path}").writeTo(table_name).tableProperty(
                "format-version", format_version
            ).using("iceberg").create()
        else:
            spark.read.load(f"file://{path}").writeTo(table_name).partitionedBy(
                partition_by
            ).tableProperty("format-version", format_version).using("iceberg").create()
    else:
        spark.read.load(f"file://{path}").writeTo(table_name).append()


def write_iceberg_from_df(
    spark, df, table_name, mode="overwrite", format_version="1", partition_by=None
):
    if mode == "overwrite":
        if partition_by is None:
            df.writeTo(table_name).tableProperty(
                "format-version", format_version
            ).using("iceberg").create()
        else:
            df.writeTo(table_name).tableProperty(
                "format-version", format_version
            ).partitionedBy(partition_by).using("iceberg").create()
    else:
        df.writeTo(table_name).append()


def generate_data(spark, start, end):
    a = spark.range(start, end, 1).toDF("a")
    b = spark.range(start + 1, end + 1, 1).toDF("b")
    b = b.withColumn("b", b["b"].cast(StringType()))

    a = a.withColumn(
        "row_index", row_number().over(Window.orderBy(monotonically_increasing_id()))
    )
    b = b.withColumn(
        "row_index", row_number().over(Window.orderBy(monotonically_increasing_id()))
    )

    df = a.join(b, on=["row_index"]).drop("row_index")
    return df


def create_iceberg_table(node, table_name):
    node.query(
        f"""
        DROP TABLE IF EXISTS {table_name};
        CREATE TABLE {table_name}
        ENGINE=Iceberg(s3, filename = 'iceberg_data/default/{table_name}/')"""
    )


def create_initial_data_file(
    cluster, node, query, table_name, compression_method="none"
):
    node.query(
        f"""
        INSERT INTO TABLE FUNCTION
            file('{table_name}.parquet')
        SETTINGS
            output_format_parquet_compression_method='{compression_method}',
            s3_truncate_on_insert=1 {query}
        FORMAT Parquet"""
    )
    user_files_path = os.path.join(
        SCRIPT_DIR, f"{cluster.instances_dir_name}/node1/database/user_files"
    )
    result_path = f"{user_files_path}/{table_name}.parquet"
    return result_path


@pytest.mark.parametrize("format_version", ["1", "2"])
def test_single_iceberg_file(started_cluster, format_version):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    TABLE_NAME = "test_single_iceberg_file_" + format_version

    inserted_data = "SELECT number, toString(number) FROM numbers(100)"
    parquet_data_path = create_initial_data_file(
        started_cluster, instance, inserted_data, TABLE_NAME
    )

    write_iceberg_from_file(
        spark, parquet_data_path, TABLE_NAME, format_version=format_version
    )

    files = upload_directory(
        minio_client, bucket, f"/iceberg_data/default/{TABLE_NAME}/", ""
    )

    create_iceberg_table(instance, TABLE_NAME)
    assert instance.query(f"SELECT * FROM {TABLE_NAME}") == instance.query(
        inserted_data
    )


@pytest.mark.parametrize("format_version", ["1", "2"])
def test_partition_by(started_cluster, format_version):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    TABLE_NAME = "test_partition_by_" + format_version

    write_iceberg_from_df(
        spark,
        generate_data(spark, 0, 10),
        TABLE_NAME,
        mode="overwrite",
        format_version=format_version,
        partition_by="a",
    )

    files = upload_directory(
        minio_client, bucket, f"/iceberg_data/default/{TABLE_NAME}/", ""
    )
    assert len(files) == 14  # 10 partitiions + 4 metadata files

    create_iceberg_table(instance, TABLE_NAME)
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 10


@pytest.mark.parametrize("format_version", ["1", "2"])
def test_multiple_iceberg_files(started_cluster, format_version):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    TABLE_NAME = "test_multiple_iceberg_files_" + format_version

    write_iceberg_from_df(
        spark,
        generate_data(spark, 0, 100),
        TABLE_NAME,
        mode="overwrite",
        format_version=format_version,
    )

    files = upload_directory(
        minio_client, bucket, f"/iceberg_data/default/{TABLE_NAME}", ""
    )
    # ['/iceberg_data/default/test_multiple_iceberg_files/data/00000-1-35302d56-f1ed-494e-a85b-fbf85c05ab39-00001.parquet',
    # '/iceberg_data/default/test_multiple_iceberg_files/metadata/version-hint.text',
    # '/iceberg_data/default/test_multiple_iceberg_files/metadata/3127466b-299d-48ca-a367-6b9b1df1e78c-m0.avro',
    # '/iceberg_data/default/test_multiple_iceberg_files/metadata/snap-5220855582621066285-1-3127466b-299d-48ca-a367-6b9b1df1e78c.avro',
    # '/iceberg_data/default/test_multiple_iceberg_files/metadata/v1.metadata.json']
    assert len(files) == 5

    create_iceberg_table(instance, TABLE_NAME)
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 100

    write_iceberg_from_df(
        spark,
        generate_data(spark, 100, 200),
        TABLE_NAME,
        mode="append",
        format_version=format_version,
    )
    files = upload_directory(
        minio_client, bucket, f"/iceberg_data/default/{TABLE_NAME}", ""
    )
    assert len(files) == 9

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 200
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY 1") == instance.query(
        "SELECT number, toString(number + 1) FROM numbers(200)"
    )


@pytest.mark.parametrize("format_version", ["1", "2"])
def test_types(started_cluster, format_version):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    TABLE_NAME = "test_types_" + format_version

    data = [
        (
            123,
            "string",
            datetime.strptime("2000-01-01", "%Y-%m-%d"),
            ["str1", "str2"],
            True,
        )
    ]
    schema = StructType(
        [
            StructField("a", IntegerType()),
            StructField("b", StringType()),
            StructField("c", DateType()),
            StructField("d", ArrayType(StringType())),
            StructField("e", BooleanType()),
        ]
    )
    df = spark.createDataFrame(data=data, schema=schema)
    df.printSchema()
    write_iceberg_from_df(
        spark, df, TABLE_NAME, mode="overwrite", format_version=format_version
    )

    upload_directory(minio_client, bucket, f"/iceberg_data/default/{TABLE_NAME}", "")

    create_iceberg_table(instance, TABLE_NAME)
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 1
    assert (
        instance.query(f"SELECT a, b, c, d, e FROM {TABLE_NAME}").strip()
        == "123\tstring\t2000-01-01\t['str1','str2']\ttrue"
    )

    table_function = f"iceberg(s3, filename='iceberg_data/default/{TABLE_NAME}/')"
    assert (
        instance.query(f"SELECT a, b, c, d, e FROM {table_function}").strip()
        == "123\tstring\t2000-01-01\t['str1','str2']\ttrue"
    )

    assert instance.query(f"DESCRIBE {table_function} FORMAT TSV") == TSV(
        [
            ["a", "Nullable(Int32)"],
            ["b", "Nullable(String)"],
            ["c", "Nullable(Date32)"],
            ["d", "Array(Nullable(String))"],
            ["e", "Nullable(Bool)"],
        ]
    )

def test_s3(started_cluster):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    TABLE_NAME = "test_s3"

    data = [
        (
            123,
            "string",
            datetime.strptime("2000-01-01", "%Y-%m-%d"),
            ["str1", "str2"],
            True,
        )
    ]
    schema = StructType(
        [
            StructField("a", IntegerType()),
            StructField("b", StringType()),
            StructField("c", DateType()),
            StructField("d", ArrayType(StringType())),
            StructField("e", BooleanType()),
        ]
    )
    df = spark.createDataFrame(data=data, schema=schema)
    df.printSchema()
    df.writeTo("kssenii").using("iceberg").create()
