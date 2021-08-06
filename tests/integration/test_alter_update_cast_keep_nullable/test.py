import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', main_configs=['configs/users.xml'], with_zookeeper=True)

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def test_cast_keep_nullable(started_cluster):
    node1.query("""
        DROP TABLE IF EXISTS t;
        CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY tuple();
        INSERT INTO t SELECT number FROM numbers(10);
        SELECT * FROM t;
        SET mutations_sync = 1;
        -- SET cast_keep_nullable = 1;
        ALTER TABLE t UPDATE x = x % 3 = 0 ? NULL : x WHERE x % 2 = 1;　
    """)
