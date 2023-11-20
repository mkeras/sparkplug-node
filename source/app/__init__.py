from app import env
from app import sparkplug, mqtt_functions
import logging

logging.basicConfig(level=logging.DEBUG)
debug = logging.debug

from random import randint
def reader(prev_value) -> int:
    return randint(4354534, 4534564445)

def start():
    test_tag = sparkplug.SparkplugMetric(
        name='testing/Memory Tag Test',
        datatype=sparkplug.SparkplugDataTypes.Int64,
        read_function=reader,
        disable_alias=True
    )
    #test_tag.read()
    string_tag = sparkplug.SparkplugMemoryTag(
        name='testing/String Tag 1',
        datatype=sparkplug.SparkplugDataTypes.String,
        writable=True,
        disable_alias=True
    )

    brokers = [
        mqtt_functions.BrokerInfo(
            client_id=env.MQTT_CLIENT_ID,
            host=env.MQTT_HOST,
            port=env.MQTT_PORT,
            username=env.MQTT_USERNAME,
            password=env.MQTT_PASSWORD,
            primary=True,
            name='Primary Broker 1'
        )
    ]
    edge_node = sparkplug.SparkplugEdgeNode(
        group_id=env.SPARKPLUG_GROUP_ID,
        edge_node_id=env.SPARKPLUG_EDGE_NODE_ID,
        brokers=brokers,
        metrics=[test_tag, string_tag],
        scan_rate=5000
    )
    debug('Starting Edge Node!')
    edge_node.start_client()
    debug('Edge Node started!')
    edge_node.loop_forever()