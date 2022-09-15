import logging
import time
import os

import pytest
import threading
import random

from helpers.cluster import ClickHouseCluster
from helpers.utility import generate_values, replace_config, SafeThread

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

TABLE_NAME = "test"


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node",
            main_configs=[
                "configs/config.d/storage_conf.xml",
                "configs/config.d/system_tables.xml",
                "configs/config.d/remote_servers.xml",
            ],
            stay_alive=True,
            with_minio=True,
            with_zookeeper=True,
        )

        cluster.add_instance(
            "node1",
            main_configs=[
                "configs/config.d/storage_conf.xml",
                "configs/config.d/system_tables.xml",
                "configs/config.d/remote_servers.xml",
            ],
            macros={"replica": "1"},
            with_minio=True,
            stay_alive=True,
            with_zookeeper=True,
        )

        cluster.add_instance(
            "node2",
            main_configs=[
                "configs/config.d/storage_conf.xml",
                "configs/config.d/system_tables.xml",
                "configs/config.d/remote_servers.xml",
            ],
            macros={"replica": "2"},
            with_minio=True,
            stay_alive=True,
            with_zookeeper=True,
        )

        cluster.add_instance(
            "node3",
            main_configs=[
                "configs/config.d/storage_conf.xml",
                "configs/config.d/system_tables.xml",
                "configs/config.d/remote_servers.xml",
            ],
            macros={"replica": "3"},
            with_minio=True,
            stay_alive=True,
            with_zookeeper=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


thread_errors = []


@pytest.mark.parametrize("storage_policy", ["s3_readonly"])
def test_merge_tree_s3(cluster, storage_policy):
    node = cluster.instances["node"]
    disk = "s3_readonly"

    storage_config_path = os.path.join(
        SCRIPT_DIR,
        f"./{cluster.instances_dir_name}/node/configs/config.d/storage_conf.xml",
    )

    def set_readonly():
        replace_config(
            storage_config_path,
            "<read_only>0</read_only>",
            "<read_only>1</read_only>",
        )

    def unset_readonly():
        replace_config(
            storage_config_path,
            "<read_only>1</read_only>",
            "<read_only>0</read_only>",
        )

    def check_readonly(expected=None):
        result = node.query(
            f"SELECT is_readonly FROM system.disks WHERE name = '{disk}'"
        )
        if expected is not None:
            assert int(result) == expected
        return int(result)

    def assert_insert_fails():
        result = node.query_and_get_error(f"INSERT INTO {TABLE_NAME} SELECT 1, 'kek'")
        assert f"Disk `{disk}` is read-only. Operation not allowed" in result

    def assert_insert_ok():
        node.query(f"INSERT INTO {TABLE_NAME} SELECT 1, 'kek'")

    node.query(
        f""" DROP TABLE IF EXISTS {TABLE_NAME} NO DELAY;
        CREATE TABLE {TABLE_NAME} (key UInt32, value String) Engine=MergeTree() ORDER BY key SETTINGS storage_policy='{storage_policy}';
    """
    )

    now = node.query("SELECT now()").strip()

    check_readonly(0)
    assert_insert_ok()

    set_readonly()
    time.sleep(5)

    check_readonly(1)
    assert_insert_fails()

    node.restart_clickhouse()

    check_readonly(1)
    assert_insert_fails()

    unset_readonly()
    time.sleep(5)

    check_readonly(0)
    assert_insert_ok()

    set_readonly()
    node.restart_clickhouse()

    node.query(f"SYSTEM STOP MERGES {TABLE_NAME}")

    unset_readonly()
    time.sleep(5)

    for _ in range(40):
        assert_insert_ok()

    count = node.query(
        f"SELECT count() FROM system.parts WHERE table='{TABLE_NAME}' AND active = 1"
    ).strip()

    node.query(f"SYSTEM START MERGES {TABLE_NAME}")
    time.sleep(10)

    result = ""
    n = 0
    while True:
        result = node.query("SELECT count() FROM system.merges")
        if int(result) == 0:
            break
        n += 1
        if n > 10:
            break
        time.sleep(1)

    assert int(result) == 0

    assert int(count) > int(node.query(
        f"SELECT count() FROM system.parts WHERE table='{TABLE_NAME}' AND active = 1"
    ).strip())

    def change_config():
        global thread_errors
        try:
            if check_readonly() == 1:
                unset_readonly()
            else:
                set_readonly()
        except Exception as e:
            thread_errors.append(repr(e))

    def insert():
        global thread_errors
        try:
            result = node.query_and_get_answer_with_error(
                f"INSERT INTO {TABLE_NAME} SELECT 1, 'kek'"
            )
            assert (
                "" == result
                or f"Disk `{disk}` is read-only. Operation not allowed" in result
            )
        except Exception as e:
            thread_errors.append(repr(e))

    def work(thread_id):
        if thread_id % 10 == 0:
            change_config()
        else:
            for _ in range(3):
                insert()

    thread_errors = []
    threads = []
    threads_num = 20
    for i in range(threads_num):
        threads.append(threading.Thread(target=work, args=(i,)))

    for thread in threads:
        time.sleep(1)
        thread.start()

    for thread in threads:
        thread.join()

    if len(thread_errors) > 0:
        for e in thread_errors:
            raise ValueError(e)

    result = node.query(
        f"""
        SELECT count()
        FROM system.text_log
        WHERE level='Error'
        AND event_time > toDateTime('{now}')
        AND empty(query_id)
    """
    )
    assert int(result) == 0

    node.query(f"DROP TABLE IF EXISTS {TABLE_NAME} NO DELAY;")


@pytest.mark.parametrize("storage_policy", ["s3_readonly"])
def test_replicated_merge_tree_s3(cluster, storage_policy):
    disk = "s3_readonly"

    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    node3 = cluster.instances["node3"]

    nodes = [node1, node2, node3]

    def set_readonly():
        for node_id in range(1, len(nodes) + 1):
            storage_config_path = os.path.join(
                SCRIPT_DIR,
                f"./{cluster.instances_dir_name}/node{node_id}/configs/config.d/storage_conf.xml",
            )

            replace_config(
                storage_config_path,
                "<read_only>0</read_only>",
                "<read_only>1</read_only>",
            )

    def unset_readonly():
        for node_id in range(1, len(nodes) + 1):
            storage_config_path = os.path.join(
                SCRIPT_DIR,
                f"./{cluster.instances_dir_name}/node{node_id}/configs/config.d/storage_conf.xml",
            )

            replace_config(
                storage_config_path,
                "<read_only>1</read_only>",
                "<read_only>0</read_only>",
            )

    def check_readonly(expected):
        for node in nodes:
            result = node.query(
                f"SELECT is_readonly FROM system.disks WHERE name = '{disk}'"
            )
            assert int(result) == expected

    def check_readonly_node(node, expected=None):
        result = node.query(
            f"SELECT is_readonly FROM system.disks WHERE name = '{disk}'"
        )
        if exptected is not None:
            assert int(result) == expected
        return int(result)

    def assert_insert_fails():
        result = nodes[0].query_and_get_error(
            f"INSERT INTO {TABLE_NAME} SETTINGS insert_quorum=3 SELECT rand(), 'kek'"
        )
        assert f"Disk `{disk}` is read-only. Operation not allowed" in result

    def assert_insert_ok():
        nodes[0].query(
            f"INSERT INTO {TABLE_NAME} SETTINGS insert_quorum=3 SELECT rand(), 'kek'"
        )

    unset_readonly()
    node1.query(
        f"""
        DROP TABLE IF EXISTS {TABLE_NAME} NO DELAY;
        CREATE TABLE {TABLE_NAME} ON CLUSTER cluster (key UInt32, value String) Engine=ReplicatedMergeTree() ORDER BY key SETTINGS storage_policy='{storage_policy}';
    """
    )

    now = node1.query("SELECT now()").strip()

    check_readonly(0)
    assert_insert_ok()

    set_readonly()
    time.sleep(5)

    check_readonly(1)
    assert_insert_fails()

    for node in nodes:
        node.restart_clickhouse()

    check_readonly(1)
    assert_insert_fails()

    unset_readonly()
    time.sleep(5)

    check_readonly(0)
    assert_insert_ok()

    set_readonly()
    for node in nodes:
        node.restart_clickhouse()

    node1.query(f"SYSTEM STOP MERGES ON CLUSTER cluster {TABLE_NAME}")

    unset_readonly()
    time.sleep(5)

    for _ in range(40):
        assert_insert_ok()

    count = node1.query(
        f"SELECT count() FROM system.parts WHERE table='{TABLE_NAME}' AND active = 1"
    ).strip()

    node1.query(f"SYSTEM START MERGES ON CLUSTER cluster {TABLE_NAME}")
    time.sleep(10)

    for node in nodes:
        result = ""
        n = 0
        while True:
            result = node.query("SELECT count() FROM system.merges")
            if int(result) == 0:
                break
            n += 1
            if n > 10:
                break
            time.sleep(1)

        assert int(result) == 0

        assert int(count) > int(node.query(
            f"SELECT count() FROM system.parts WHERE table='{TABLE_NAME}' AND active = 1"
        ))

    result = node1.query(
        f"""
        SELECT count()
        FROM clusterAllReplicas('cluster', 'system', 'text_log')
        WHERE level='Error'
        AND event_time > toDateTime('{now}')
        AND empty(query_id)
    """
    )
    assert int(result) == 0

    node.query(f"DROP TABLE IF EXISTS {TABLE_NAME} ON CLUSTER cluster NO DELAY;")
