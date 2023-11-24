from os import environ
from random import randint

__true = ['True', 'true', '1']

DEBUG = environ.get('DEBUG', default=False) in __true

MQTT_HOST = environ.get('MQTT_HOST')
MQTT_PORT = int(environ.get('MQTT_PORT', default=8883))
MQTT_USERNAME = environ.get('MQTT_USERNAME')
MQTT_PASSWORD = environ.get('MQTT_PASSWORD')

MQTT_CLIENT_ID = environ.get('MQTT_CLIENT_ID', default=f'sparkplug-node-{randint(100000, 999999)}')

MQTT_USE_TLS = environ.get('MQTT_USE_TLS', default='True') in __true


SPARKPLUG_GROUP_ID = environ.get('SPARKPLUG_GROUP_ID')
SPARKPLUG_EDGE_NODE_ID = environ.get('SPARKPLUG_EDGE_NODE_ID')

DATA_DIRECTORY = environ.get('DATA_DIRECTORY', default=f'/etc/sparkplug/{SPARKPLUG_GROUP_ID}/{SPARKPLUG_EDGE_NODE_ID}/').replace(' ', '_')

CONFIG_FILEPATH = environ.get('CONFIG_FILEPATH', default=f'{DATA_DIRECTORY}config.json')

MEMORY_TAGS_FILEPATH = environ.get('MEMORY_TAGS_FILEPATH', default=f'{DATA_DIRECTORY}memory-tags.json')