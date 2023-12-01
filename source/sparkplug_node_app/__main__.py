from sparkplug_node_app import env, logging
from sparkplug_node_app import sparkplug, mqtt_functions


def on_set_client(node: sparkplug.SparkplugEdgeNode, mqtt_client: mqtt_functions.mqtt.Client):
    logging.debug('on_set_client CALLBACK')

def on_mqtt_connect(node: sparkplug.SparkplugEdgeNode, mqtt_client: mqtt_functions.mqtt.Client):
    logging.debug('on_mqtt_connect CALLBACK')

string_tag = sparkplug.SparkplugMemoryTag(
    name='demo/String Tag 1',
    datatype=sparkplug.SparkplugDataTypes.String,
    initial_value='This is a writable string tag',
    writable=True,
    disable_alias=True,
    persistence_file=env.MEMORY_TAGS_FILEPATH
)

int_tag = sparkplug.SparkplugMemoryTag(
    name='demo/Integer Tag 1',
    datatype=sparkplug.SparkplugDataTypes.Int64,
    initial_value=2341,
    writable=True,
    disable_alias=True,
    persistence_file=env.MEMORY_TAGS_FILEPATH
)

float_tag = sparkplug.SparkplugMemoryTag(
    name='demo/Float Tag 1',
    datatype=sparkplug.SparkplugDataTypes.Float,
    initial_value=3.1415,
    writable=True,
    disable_alias=True,
    persistence_file=env.MEMORY_TAGS_FILEPATH
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
    metrics=[string_tag, int_tag, float_tag],
    scan_rate=5000,
    on_set_client=on_set_client,
    on_mqtt_connect=on_mqtt_connect,
    config_filepath=env.CONFIG_FILEPATH,
    config_save_rate=20000
)

edge_node.loop_forever()