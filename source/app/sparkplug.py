from app.protobuf_files import sparkplug_pb2
from app import config, mqtt_functions
from app.sparkplug_tags import SparkplugDataTypes, SparkplugMetric, SparkplugMemoryTag
from google.protobuf.json_format import MessageToJson, MessageToDict, Parse, ParseDict, ParseError
from google.protobuf.message import DecodeError, EncodeError
from typing import List, Callable, Optional
from enum import Enum
import logging
from app import helpers
from collections import deque
import time
import uuid
import os
import json


class SparkplugEdgeNodeTopics:
    def __init__(self, group_id: str, edge_node_id: str, host_application_id: str = None) -> None:
        if group_id == 'STATE':
            raise ValueError(f'Invalid group id of "{group_id}"')
        self._nbirth = f'spBv1.0/{group_id}/NBIRTH/{edge_node_id}'
        self._ndeath = f'spBv1.0/{group_id}/NDEATH/{edge_node_id}'
        self._ndata = f'spBv1.0/{group_id}/NDATA/{edge_node_id}'
        self._ncmd = f'spBv1.0/{group_id}/NCMD/{edge_node_id}'

        self._host_application = None if host_application_id is None else f'spBv1.0/STATE/{host_application_id}'

    @property
    def NBIRTH(self) -> str:
        return self._nbirth
    
    @property
    def NDEATH(self) -> str:
        return self._ndeath

    @property
    def NCMD(self) -> str:
        return self._ncmd

    @property
    def NDATA(self) -> str:
        return self._ndata

    @property
    def HOST_APPLICATION(self) -> str:
        return self._host_application

    @property
    def has_host_application(self) -> bool:
        return self._host_application is not None


class SparkplugEdgeNode:
    def __init__(self,
        group_id: str,
        edge_node_id: str,
        brokers: List[mqtt_functions.BrokerInfo],
        metrics: Optional[List[SparkplugMetric]] = None,
        host_application_id: Optional[str] = None,
        scan_rate: Optional[int] = None,
        config_save_rate: Optional[int] = None,
        on_set_client: Optional[Callable[['SparkplugEdgeNode', mqtt_functions.mqtt.Client], None]] = None,
        on_mqtt_connect: Optional[Callable[['SparkplugEdgeNode', mqtt_functions.mqtt.Client], None]] = None,
        on_mqtt_publish: Optional[Callable[['SparkplugEdgeNode', mqtt_functions.mqtt.Client], None]] = None,
        on_mqtt_message: Optional[Callable[['SparkplugEdgeNode', mqtt_functions.mqtt.Client], None]] = None,
        on_mqtt_disconnect: Optional[Callable[['SparkplugEdgeNode', mqtt_functions.mqtt.Client], None]] = None,
        config_filepath: str = None
        ) -> None:

        metrics = [] if metrics is None else metrics
        for metric in metrics:
            if metric.name in ['Node Control/Scan Rate', 'Node Control/Rebirth']:
                raise ValueError(f'Invalid metric name: "{metric.name}"!')

        self.__topics = SparkplugEdgeNodeTopics(group_id=group_id, edge_node_id=edge_node_id, host_application_id=host_application_id)
        self.__brokers = brokers
        self.__metrics = metrics
        self.__running = False

        scan_rate = 1000 if not scan_rate or scan_rate > 3_600_000 or scan_rate < 500 else scan_rate
        config_save_rate = 600_000 if not config_save_rate or config_save_rate > 36_000_000 or config_save_rate < 20_000 else config_save_rate

        self.__config_filepath = None
        if config_filepath:
            self.__init_config_file(config_filepath)
            self.__config_filepath = config_filepath
            logging.info(f'Config File set to: "{config_filepath}"')
            config_data = self.__read_config_file(config_filepath)
            if config_data.get('recreate_node_args'):
                if 'scan_rate' in config_data['recreate_node_args'].keys():
                    scan_rate = config_data['recreate_node_args']['scan_rate']
                if 'config_save_rate' in config_data['recreate_node_args'].keys():
                    config_save_rate = config_data['recreate_node_args']['config_save_rate']
                # TODO fully implement

        self.__scan_rate = SparkplugMemoryTag(
            name='Node Control/Scan Rate',
            datatype=SparkplugDataTypes.Int64,
            initial_value=scan_rate,
            writable=True,
            disable_alias=True
        )

        metrics.append(self.__scan_rate)


        self.__bdseq = helpers.Incrementor()
        self.__seq = helpers.Incrementor(maximum=255)

        self.__mid_deque = deque(maxlen=10)

        self.__config_save_rate = config_save_rate
        self.__last_config_save = 0
        

        '''Callbacks for exposing mqtt client to external functions. all functions are called with the arguments (node=self, mqtt_client=client)'''
        self.__callbacks = dict(
            on_set_client=on_set_client if callable(on_set_client) else None,
            on_mqtt_connect=on_mqtt_connect if callable(on_mqtt_connect) else None,
            on_mqtt_publish=on_mqtt_publish if callable(on_mqtt_publish) else None,
            on_mqtt_message=on_mqtt_message if callable(on_mqtt_message) else None,
            on_mqtt_disconnect=on_mqtt_disconnect if callable(on_mqtt_disconnect) else None
        )

        self.__last_read = 0

        if not brokers:
            raise ValueError('No brokers supplied to SparkPlugEdgeNode!')

        # set the primary broker TODO implement broker walk, etc
        self.__primary_broker_idx = 0
        for idx, broker in enumerate(brokers):
            if broker.primary:
                self.__primary_broker_idx = idx
                break
        
        self.__set_broker(self.__primary_broker_idx)

        
    @property
    def primary_broker(self) -> mqtt_functions.BrokerInfo:
        return self.__brokers[self.__primary_broker_idx]

    @property
    def current_broker(self) -> mqtt_functions.BrokerInfo:
        return self.__brokers[self.__current_broker_idx]

    def __set_client(self, client: mqtt_functions.mqtt.Client):
        '''
        Set the mqtt client and get it ready for connection
        '''
        self.__client = client
        self.__client.on_connect = self.__on_mqtt_connect
        self.__client.on_publish = self.__on_mqtt_publish
        self.__client.on_disconnect = self.__on_mqtt_disconnect
        self.__client.on_message = self.__on_mqtt_messge
        self.__client.message_callback_add(self.__topics.NCMD, self.__on_ncmd_message)
        if self.__callbacks['on_set_client']:
            self.__callbacks['on_set_client'](node=self, mqtt_client=client)
    
    def __set_broker(self, idx: int):
        if self.__running:
            self.__client.loop_stop()
        self.__current_broker_idx = idx
        self.__set_client(self.primary_broker.create_client())

    def start_client(self):
        if self.__running:
            self.__client.loop_stop()
        
        broker = self.current_broker

        self.__client.will_set(topic=self.__topics.NDEATH, payload=self.__get_ndeath_payload(), qos=1)

        self.__client.connect_async(
            host=broker.host,
            port=broker.port,
            clean_start=True
        )
        self.__client.loop_start()
        self.__running = True

    def stop_client(self):
        self.__client.loop_stop()
        self.__running = False

    def save_config(self) -> bool:
        '''
        Save config to file on ssd
        '''
        filepath = self.__config_filepath
        if not filepath:
            logging.debug('Ignoring config save, no filepath set')
            return False

        config = {
            'bdSeq': self.__bdseq.current_value,
            'recreate_node_args': {
                'scan_rate': self.__scan_rate.current_value,
                'config_save_rate': self.__config_save_rate
            }
        }

        with open(filepath, 'w') as file:
            json.dump(config, file, indent=4)

        for metric in self.metrics:
            if not isinstance(metric, SparkplugMemoryTag) or not metric.persistent:
                continue
            logging.debug(f'Saving tag "{metric.name}" to disk')
            metric.save_to_disk()

        self.__last_config_save = helpers.millis()
        return True

    @staticmethod
    def __init_config_file(config_filepath: str):
        if not config_filepath:
            logging.warning('Cannot initialize config file, no config filepath set!')
            return
        
        if os.path.isfile(config_filepath):
            logging.info(f'Skipping config file initialize, config file "{config_filepath}" already exists!')
            return
        
        directory_path = os.path.dirname(config_filepath)
        os.makedirs(directory_path, exist_ok=True)
        with open(config_filepath, 'w', newline='') as file:
            json.dump({}, file)


    @staticmethod
    def __read_config_file(config_filepath: str) -> dict:
        '''
        Verify that config file exists before calling, otherwise will throw error
        If file is not valid json will throw error
        '''
        if not config_filepath or not os.path.isfile(config_filepath):
            raise ValueError(f'Config file "{config_filepath}" does not exist!')

        with open(config_filepath, 'r') as file:
            return json.load(file)


    def read(self, rbe: bool = True) -> List[dict]:
        changed = []
        for metric in self.__metrics:
            metric.read()
            if not rbe:
                changed.append(metric.as_birth_metric())
                continue
            if metric.rbe_ignore:
                continue
            if not metric.value_changed:
                continue
            changed.append(metric.as_rbe_metric())
        self.__last_read = helpers.millis()
        return changed

    @property
    def metrics(self) -> List[SparkplugMetric]:
        return self.__metrics

    @property
    def last_read_delta(self) -> int:
        return helpers.millis() - self.__last_read
    
    @property
    def read_due(self) -> bool:
        if self.__scan_rate.current_value is None:
            return True
        return self.last_read_delta >= self.__scan_rate.current_value

    @property
    def last_config_save_delta(self) -> int:
        return helpers.millis() - self.__last_config_save

    @property
    def config_save_due(self) -> bool:
        if not self.__config_save_rate:
            return False
        return self.last_config_save_delta >= self.__config_save_rate
    
    def make_payload_from_metrics(self, metrics: List[dict]) -> bytes:
        payload_dict = {
            'timestamp': helpers.millis(),
            'seq': self.__seq.current_value,
            'metrics': metrics
        }
        return ParseDict(payload_dict, sparkplug_pb2.Payload()).SerializeToString()

    def loop_forever(self):
        if not self.__running:
            logging.info('Starting Edge Node MQTT loop!')
            self.start_client()
        logging.info('MQTT started! Starting RBE loop')
        while True:
            if not self.__client.is_connected:
                return
            if self.read_due:
                logging.debug('Tag Read Due!')
                self._rbe()
            if self.config_save_due:
                logging.debug('Config Save Due!')
                self.save_config()

    def _rbe(self):
        metrics_to_publish = self.read()
        if metrics_to_publish:
            logging.debug(f'{len(metrics_to_publish)} Values have changed, publish')
            self.__mqtt_publish(
                client=self.__client,
                topic=self.__topics.NDATA,
                payload=self.make_payload_from_metrics(metrics_to_publish)
            )

    '''
    Sparkplug functions
    '''
    def __get_ndeath_payload(self) -> bytes:
        millis = helpers.millis()
        
        return ParseDict({
            'timestamp': millis,
            'metrics': [
                {
                    'timestamp': millis,
                    'name': 'bdSeq',
                    'datatype': SparkplugDataTypes.UInt64.value,
                    'long_value': self.__bdseq.current_value
                }
            ]
        }, sparkplug_pb2.Payload()).SerializeToString()
        
    
    def __get_nbirth_payload(self, rebirth: bool = False) -> bool:
        logging.debug(f'MAKING BIRTH PAYLOAD, bdSeq: {self.__bdseq.previous_value if rebirth else self.__bdseq.current_value}')
        millis = helpers.millis()
        self.__seq.reset()  # Remove this line for sparkplug 3.0.0

        payload = {
            'timestamp': millis,
            'seq': self.__seq.current_value,
            'metrics': [
                {
                    'timestamp': millis,
                    'name': 'bdSeq',
                    'datatype': SparkplugDataTypes.UInt64.value,
                    'long_value': self.__bdseq.previous_value if rebirth else self.__bdseq.current_value
                },
                {
                    'timestamp': millis,
                    'name': 'Node Control/Rebirth',
                    'datatype': SparkplugDataTypes.Boolean.value,
                    'boolean_value': False
                }
            ]
        }
        # add metrics to payload
        payload['metrics'].extend(self.read(rbe=False))
        
        return ParseDict(payload, sparkplug_pb2.Payload()).SerializeToString()


    def __sparkplug_message_published(self):
        logging.info(f'SPARKPLUG MESSAGE PUBLISHED (seq: {self.__seq.current_value})')
        self.__seq.next_value()


    def __on_ncmd_message(self, client, userdata, message):
        if message.topic != self.__topics.NCMD:
            logging.debug('Ignoring NCMD with invalid topic!')
            return
        logging.debug('Received NCMD Message!')
        try:
            trigger_publish: bool = False
            trigger_rebirth: bool = False
            payload = sparkplug_pb2.Payload()
            payload.ParseFromString(message.payload)
            data = MessageToDict(payload)
            for metric in data['metrics']:
                if metric.get('name') is None and metric.get('alias') is None:
                    continue

                if metric['name'] == 'Node Control/Rebirth' and metric['booleanValue']:
                    logging.debug(f'REBIRTH NCMD SET')
                    trigger_rebirth = True
                elif metric['name'] == 'Node Control/Scan Rate' and metric['longValue']:
                    new_scan_rate = int(metric['longValue'])
                    if 499 < new_scan_rate < 3600001:
                        self.__scan_rate.write(new_scan_rate)
                        trigger_publish = True
                else:
                    # match metric by name
                    for metric_obj in self.__metrics:
                        if metric['name'] != metric_obj.name:
                            continue
                        if not metric_obj.writable:
                            logging.warning(f'Ignoring NCMD: cannot write to read only tag "{metric_obj.name}"')
                            break
                        if metric_obj.value_key_camel_case in metric.keys():
                            new_value = metric[metric_obj.value_key_camel_case]

                        value_key = metric_obj.value_key_camel_case if metric_obj.value_key_camel_case in metric.keys() else None
                        if not value_key:
                            if metric_obj.value_key in metric.keys():
                                value_key = metric_obj.value_key
                            else:
                                logging.error(f'NCMD Error: mismatched value key for metric "{metric_obj.name}". Expected value key "{metric_obj.value_key_camel_case}"')
                                break

                        new_value = metric[value_key]
                        metric_obj.write(new_value)
                        trigger_publish = True
                        logging.info(f'NCMD, wrote "{new_value}" to metric "{metric_obj.name}"')
                        break
            
            if trigger_rebirth:
                payload = self.__get_nbirth_payload(rebirth=True)
                if payload:
                    self.__mqtt_publish(client=client, topic=self.__topics.NBIRTH, payload=payload)
                    logging.info('Rebirth Published!')
                return
            if not trigger_publish:
                return
            self._rbe()
        except (DecodeError, KeyError, ValueError) as err:
            logging.error(f'NCMD failed: {err}')

    '''
    MQTT paho-mqtt client functions
    '''
    def __mqtt_publish(self, client: mqtt_functions.mqtt.Client, topic: str, payload: str or bytes, qos: int = 0, retain: bool = False):
        if not client.is_connected:
            pass # TODO STORE AND FORWARD
        result = client.publish(topic=topic, payload=payload, qos=qos, retain=retain)
        self.__mid_deque.append(result.mid)

    def __on_mqtt_connect(self, client, userdata, flags, rc, reasonCode = None, properties = None):
        return_code = mqtt_functions.ReturnCodes(rc)
        if rc != 0:
            logging.error(f'MQTT Connect failed: {return_code.description}')
            return
        logging.info(f'MQTT Connection Success: {return_code.description}')
        client.subscribe(self.__topics.NCMD)

        self.__mqtt_publish(client, self.__topics.NBIRTH, self.__get_nbirth_payload(rebirth=False))
        logging.debug(f'PUBLISHED NBIRTH')
        self.__bdseq.next_value()
        if self.__callbacks['on_mqtt_connect']:
            self.__callbacks['on_mqtt_connect'](node=self, mqtt_client=client)

    def __on_mqtt_publish(self, client, userdata, mid):
        logging.debug(f'MQTT MESSAGE PUBLISHED')
        if mid is not None and mid in self.__mid_deque:
            self.__sparkplug_message_published()
        if self.__callbacks['on_mqtt_publish']:
            self.__callbacks['on_mqtt_publish'](node=self, mqtt_client=client)

    def __on_mqtt_messge(self, client, userdata, msg):
        logging.debug('-----< MQTT MESSAGE RECEIVED >-----')

    def __on_mqtt_disconnect(self, client, userdata, rc):
        logging.error('MQTT DISCONNECTED')
        if self.__callbacks['on_mqtt_disconnect']:
            self.__callbacks['on_mqtt_disconnect'](node=self, mqtt_client=client)

    
    

class SparkplugDevice:
    def __init__(self, edge_node: SparkplugEdgeNode, device_id: str, metrics: List[SparkplugMetric] = None) -> None:
        raise NotImplementedError



