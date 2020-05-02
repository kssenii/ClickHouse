import os.path as p
import random
import threading
import time
import pytest

import pika
from sys import getdefaultencoding

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV
from helpers.client import QueryRuntimeException
from helpers.network import PartitionManager

import json
import subprocess

from google.protobuf.internal.encoder import _VarintBytes

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance',
                                config_dir='configs',
                                main_configs=['configs/rabbitmq.xml','configs/log_conf.xml'],
                                with_rabbitmq=True,
                                clickhouse_path_dir='clickhouse_path')
rabbitmq_id = ''


# Helpers

def check_rabbitmq_is_available():
    p = subprocess.Popen(('docker',
                          'exec',
                          '-i',
                          rabbitmq_id,
                          'rabbitmqctl', 
                          'await_startup'),
                         stdout=subprocess.PIPE)
    p.communicate()
    return p.returncode == 0


def wait_rabbitmq_is_available(max_retries=50):
    retries = 0
    while True:
        if check_rabbitmq_is_available():
            break
        else:
            retries += 1
            if retries > max_retries:
                raise "RabbitMQ is not available"
            print("Waiting for RabbitMQ to start up")
            time.sleep(1)


def rabbitmq_check_result(result, check=False, ref_file='test_rabbitmq_json.reference'):
    fpath = p.join(p.dirname(__file__), ref_file)
    with open(fpath) as reference:
        if check:
            assert TSV(result) == TSV(reference)
        else:
            return TSV(result) == TSV(reference)


# Fixtures

@pytest.fixture(scope="module")
def rabbitmq_cluster():
    try:
        global rabbitmq_id
        cluster.start()
        rabbitmq_id = instance.cluster.rabbitmq_docker_id
        print("rabbitmq_id is {}".format(rabbitmq_id))
        instance.query('CREATE DATABASE test')

        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def rabbitmq_setup_teardown():
    wait_rabbitmq_is_available()
    print("RabbitMQ is available - running test")
    yield  # run test
    instance.query('DROP TABLE IF EXISTS test.rabbitmq')


# Tests


@pytest.mark.timeout(180)
def test_rabbitmq_select_from_new_syntax_table(rabbitmq_cluster):
    instance.query('''
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_routing_key_list = 'new',
                     rabbitmq_exchange_name = 'direct_exchange',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_row_delimiter = '\\n';
        ''')

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.exchange_declare(exchange='direct_exchange', exchange_type='direct')

    messages = []
    for i in range(25):
        messages.append(json.dumps({'key': i, 'value': i}))

    for message in messages:
        channel.basic_publish(exchange='direct_exchange', routing_key='new', body=message)

    messages = []
    for i in range(25, 50):
        messages.append(json.dumps({'key': i, 'value': i}))
    for message in messages:
        channel.basic_publish(exchange='direct_exchange', routing_key='new', body=message)

    result = instance.query('SELECT * FROM test.rabbitmq', ignore_error=False)

    connection.close()
    rabbitmq_check_result(result, True)


@pytest.mark.timeout(180)
def test_rabbitmq_select_from_old_syntax_table(rabbitmq_cluster):
    instance.query('''
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ('rabbitmq1:5672', 'old', 'direct_exchange', 'JSONEachRow', '\\n');
        ''')

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.exchange_declare(exchange='direct_exchange', exchange_type='direct')

    messages = []
    for i in range(50):
        messages.append(json.dumps({'key': i, 'value': i}))

    for message in messages:
        channel.basic_publish(exchange='direct_exchange', routing_key='old', body=message)

    result = instance.query('SELECT * FROM test.rabbitmq', ignore_error=True)

    connection.close()
    rabbitmq_check_result(result, True)


@pytest.mark.timeout(180)
def test_rabbitmq_select_empty(rabbitmq_cluster):
    instance.query('''
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_routing_key_list = 'empty',
                     rabbitmq_format = 'TSV',
                     rabbitmq_row_delimiter = '\\n';
        ''')

    assert int(instance.query('SELECT count() FROM test.rabbitmq')) == 0


@pytest.mark.timeout(180)
def test_rabbitmq_json_without_delimiter(rabbitmq_cluster):
    instance.query('''
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_routing_key_list = 'json',
                     rabbitmq_exchange_name = 'direct_exchange',
                     rabbitmq_format = 'JSONEachRow'
        ''')

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.exchange_declare(exchange='direct_exchange', exchange_type='direct')

    messages = ''
    for i in range(25):
        messages += json.dumps({'key': i, 'value': i}) + '\n'

    all_messages = [messages]
    for message in all_messages:
        channel.basic_publish(exchange='direct_exchange', routing_key='json', body=message)

    messages = ''
    for i in range(25, 50):
        messages += json.dumps({'key': i, 'value': i}) + '\n'
    all_messages = [messages]
    for message in all_messages:
        channel.basic_publish(exchange='direct_exchange', routing_key='json', body=message)

    result = ''
    while True:
        result += instance.query('SELECT * FROM test.rabbitmq', ignore_error=True)
        if rabbitmq_check_result(result):
            break

    connection.close()
    rabbitmq_check_result(result, True)


@pytest.mark.timeout(180)
def test_rabbitmq_csv_with_delimiter(rabbitmq_cluster):
    instance.query('''
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_routing_key_list = 'csv',
                     rabbitmq_exchange_name = 'direct_exchange',
                     rabbitmq_format = 'CSV',
                     rabbitmq_row_delimiter = '\\n';
        ''')

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.exchange_declare(exchange='direct_exchange', exchange_type='direct')

    messages = []
    for i in range(50):
        messages.append('{i}, {i}'.format(i=i))

    for message in messages:
        channel.basic_publish(exchange='direct_exchange', routing_key='csv', body=message)

    result = ''
    while True:
        result += instance.query('SELECT * FROM test.rabbitmq', ignore_error=True)
        if rabbitmq_check_result(result):
            break


    connection.close()
    rabbitmq_check_result(result, True)


@pytest.mark.timeout(180)
def test_rabbitmq_tsv_with_delimiter(rabbitmq_cluster):
    instance.query('''
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_routing_key_list = 'tsv',
                     rabbitmq_exchange_name = 'direct_exchange',
                     rabbitmq_format = 'TSV',
                     rabbitmq_row_delimiter = '\\n';
        ''')

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.exchange_declare(exchange='direct_exchange', exchange_type='direct')

    messages = []
    for i in range(50):
        messages.append('{i}\t{i}'.format(i=i))

    for message in messages:
        channel.basic_publish(exchange='direct_exchange', routing_key='tsv', body=message)

    result = instance.query('SELECT * FROM test.rabbitmq', ignore_error=True)

    connection.close()
    rabbitmq_check_result(result, True)


@pytest.mark.timeout(180)
def test_rabbitmq_materialized_view(rabbitmq_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_routing_key_list = 'mv',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_row_delimiter = '\\n';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.rabbitmq;
    ''')

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    messages = []
    for i in range(50):
        messages.append(json.dumps({'key': i, 'value': i}))
    for message in messages:
        channel.basic_publish(exchange='direct_exchange', routing_key='mv', body=message)

    while True:
        result = instance.query('SELECT * FROM test.view')
        if (rabbitmq_check_result(result)):
            break;

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    ''')

    connection.close()
    rabbitmq_check_result(result, True)


@pytest.mark.timeout(180)
def test_rabbitmq_materialized_view_with_subquery(rabbitmq_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_routing_key_list = 'mvsq',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_row_delimiter = '\\n';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM (SELECT * FROM test.rabbitmq);
    ''')

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    messages = []
    for i in range(50):
        messages.append(json.dumps({'key': i, 'value': i}))
    for message in messages:
        channel.basic_publish(exchange='direct_exchange', routing_key='mvsq', body=message)

    while True:
        result = instance.query('SELECT * FROM test.view')
        if rabbitmq_check_result(result):
            break

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    ''')

    connection.close();
    rabbitmq_check_result(result, True)


@pytest.mark.timeout(180)
def test_rabbitmq_many_materialized_views(rabbitmq_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.view1;
        DROP TABLE IF EXISTS test.view2;
        DROP TABLE IF EXISTS test.consumer1;
        DROP TABLE IF EXISTS test.consumer2;
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_routing_key_list = 'mmv',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_row_delimiter = '\\n';
        CREATE TABLE test.view1 (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE TABLE test.view2 (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer1 TO test.view1 AS
            SELECT * FROM test.rabbitmq;
        CREATE MATERIALIZED VIEW test.consumer2 TO test.view2 AS
            SELECT * FROM test.rabbitmq;
    ''')

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    messages = []
    for i in range(50):
        messages.append(json.dumps({'key': i, 'value': i}))
    for message in messages:
        channel.basic_publish(exchange='direct_exchange', routing_key='mmv', body=message)

    while True:
        result1 = instance.query('SELECT * FROM test.view1')
        result2 = instance.query('SELECT * FROM test.view2')
        if rabbitmq_check_result(result1) and rabbitmq_check_result(result2):
            break

    instance.query('''
        DROP TABLE test.consumer1;
        DROP TABLE test.consumer2;
        DROP TABLE test.view1;
        DROP TABLE test.view2;
    ''')

    rabbitmq_check_result(result1, True)
    rabbitmq_check_result(result2, True)


@pytest.mark.timeout(300)
def test_rabbitmq_highload_message(rabbitmq_cluster):
    # Create batchs of messages of size ~100Kb
    rabbitmq_messages = 1000
    batch_messages = 1000
    messages = [json.dumps({'key': i, 'value': 'x' * 100}) * batch_messages for i in range(rabbitmq_messages)]

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        CREATE TABLE test.rabbitmq (key UInt64, value String)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_routing_key_list = 'big',
                     rabbitmq_format = 'JSONEachRow';
        CREATE TABLE test.view (key UInt64, value String)
            ENGINE = MergeTree
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.rabbitmq;
    ''')

    for message in messages:
        channel.basic_publish(exchange='direct_exchange', routing_key='big', body=message)

    while True:
        result = instance.query('SELECT count() FROM test.view')
        if int(result) == batch_messages * rabbitmq_messages:
            break

    connection.close()
    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    ''')

    assert int(result) == rabbitmq_messages*batch_messages, 'ClickHouse lost some messages: {}'.format(result)


@pytest.mark.timeout(180)
def test_rabbitmq_insert(rabbitmq_cluster):
    instance.query('''
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_routing_key_list = 'insert1',
                     rabbitmq_format = 'TSV',
                     rabbitmq_row_delimiter = '\\n';
    ''')

    credentials = pika.PlainCredentials('root', 'clickhouse')
    parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
    consumer_connection = pika.BlockingConnection(parameters)

    consumer = consumer_connection.channel()
    consumer.exchange_declare(exchange='direct_exchange', exchange_type='direct')
    result = consumer.queue_declare(queue='')
    queue_name = result.method.queue
    consumer.queue_bind(exchange='direct_exchange', queue=queue_name, routing_key='insert1')

    values = []
    for i in range(50):
        values.append("({i}, {i})".format(i=i))
    values = ','.join(values)

    while True:
        try:
            instance.query("INSERT INTO test.rabbitmq VALUES {}".format(values))
            break
        except QueryRuntimeException as e:
            if 'Local: Timed out.' in str(e):
                continue
            else:
                raise

    insert_messages = []
    def onReceived(channel, method, properties, body):
        i = 0
        insert_messages.append(body.decode())
        if (len(insert_messages) == 50):
            channel.stop_consuming()

    consumer.basic_qos(prefetch_count=50)
    consumer.basic_consume(queue=queue_name, on_message_callback=onReceived, auto_ack=True)
    consumer.start_consuming()
    consumer_connection.close()

    result = '\n'.join(insert_messages)
    rabbitmq_check_result(result, True)


@pytest.mark.timeout(500)
def test_rabbitmq_insert_select_via_materialized_view(rabbitmq_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_routing_key_list = 'insert2',
                     rabbitmq_format = 'TSV',
                     rabbitmq_row_delimiter = '\\n';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.rabbitmq;
    ''')

    messages_num = 2
    def insert():
        values = []
        for i in range(messages_num):
            values.append("({i}, {i})".format(i=i))
        values = ','.join(values)

        while True:
            try:
                instance.query("INSERT INTO test.rabbitmq VALUES {}".format(values))
                break
            except QueryRuntimeException as e:
                if 'Local: Timed out.' in str(e):
                    continue
                else:
                    raise

    threads = []
    threads_num = 4
    for _ in range(threads_num):
        threads.append(threading.Thread(target=insert))
    for thread in threads:
        #time.sleep(random.uniform(0, 1))
        time.sleep(2)
        thread.start()

    while True:
        result = instance.query('SELECT count() FROM test.view')
        time.sleep(1)
        if int(result) == messages_num * threads_num:
            break

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    ''')

    for thread in threads:
        thread.join()

    assert int(result) == messages_num * threads_num, 'ClickHouse lost some messages: {}'.format(result)


#@pytest.mark.timeout(180)
#def test_rabbitmq_virtual_columns(rabbitmq_cluster):
#    instance.query('''
#        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
#            ENGINE = RabbitMQ
#            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
#                     rabbitmq_routing_key_list = 'virt1',
#                     rabbitmq_format = 'JSONEachRow',
#                     rabbitmq_row_delimiter = '\\n';
#        ''')
#
#    credentials = pika.PlainCredentials('root', 'clickhouse')
#    parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
#    connection = pika.BlockingConnection(parameters)
#    channel = connection.channel()
#
#    messages = ''
#    for i in range(25):
#        messages += json.dumps({'key': i, 'value': i}) + '\n'
#    all_messages = [messages]
#    for message in all_messages:
#        channel.basic_publish(exchange='direct_exchange', routing_key='virt1', body=message)
#
#    messages = ''
#    for i in range(25, 50):
#        messages += json.dumps({'key': i, 'value': i}) + '\n'
#    all_messages = [messages]
#    for message in all_messages:
#        channel.basic_publish(exchange='direct_exchange', routing_key='virt1', body=message)
#
#    result = ''
#    while True:
#        result += instance.query(
#                 'SELECT _key, key,
#                _topic, value, _offset, _partition, _timestamp FROM test.kafka', 
#                ignore_error=True)
#        if rabbitmq_check_result(result, False, 'test_rabbitmq_virtual1.reference'):
#            break
#
#    rabbitmq_check_result(result, True, 'test_rabbitmq_virtual1.reference')




if __name__ == '__main__':
    cluster.start()
    raw_input("Cluster created, press any key to destroy...")
    cluster.shutdown()

