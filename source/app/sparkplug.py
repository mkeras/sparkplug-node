from app.protobuf_files import sparkplug_pb2
from app import config, mqtt_functions
from google.protobuf.json_format import MessageToJson, MessageToDict, Parse, ParseDict, ParseError
from google.protobuf.message import DecodeError, EncodeError
from typing import List
from enum import Enum
import logging
from app import helpers
from collections import deque
import time
import uuid


class SparkplugDataTypes(Enum):
    """ Indexes of Data Types """

    """ Unknown placeholder for future expansion. """
    Unknown = 0

    """ Basic Types """
    Int8 = 1
    Int16 = 2
    Int32 = 3
    Int64 = 4
    UInt8 = 5
    UInt16 = 6
    UInt32 = 7
    UInt64 = 8
    Float = 9
    Double = 10
    Boolean = 11
    String = 12
    DateTime = 13
    Text = 14

    """ Additional Metric Types """
    UUID = 15
    DataSet = 16
    Bytes = 17
    File = 18
    Template = 19

    """ Additional PropertyValue Types """
    PropertySet = 20
    PropertySetList = 21

    """ Array Types """
    Int8Array = 22
    Int16Array = 23
    Int32Array = 24
    Int64Array = 25
    UInt8Array = 26
    UInt16Array = 27
    UInt32Array = 28
    UInt64Array = 29
    FloatArray = 30
    DoubleArray = 31
    BooleanArray = 32
    StringArray = 33
    DateTimeArray = 34

    @property
    def is_number(self) -> bool:
        return 0 < self.value < 11

    @property
    def value_key(self) -> str:
        if self.value in [1, 2, 3, 5, 6, 7]:
            return 'int_value'
        elif self.value in [4, 8]:
            return 'long_value'
        elif self.value == 9:
            return 'float_value'
        elif self.value == 10:
            return 'double_value'
        elif self.value == 11:
            return 'boolean_value'
        elif self.value in [12, 13, 14, 15]:
            return 'string_value'
        elif self.value in [17, 18]:
            return 'bytes_value'
        raise NotImplementedError

    @property
    def value_key_camel_case(self) -> str:
        value_key_split = self.value_key.split('_')
        return value_key_split[0] + value_key_split[1][0].upper() + value_key_split[1][1:]

    @property
    def coerce_fn(self):
        if self.value in [1, 2, 3, 4, 5, 6, 7, 8]:
            return int
        elif self.value in [9, 10]:
            return float
        elif self.value == 11:
            return bool
        elif self.value in [12, 13, 14, 15]:
            return str
        elif self.value in [17, 18]:
            return bytes
        raise NotImplementedError


class SparkplugMetric:
    __instance_count = 0
    def __init__(
        self,
        name: str,
        datatype: SparkplugDataTypes,
        read_function,
        write_function = None,
        alias: int = None,
        disable_alias: bool = False,
        rbe_ignore: bool = False,
    ) -> None:
        """
        read function signature: read_function(prev_value)
        returns value, whatever its datatype is

        write function signature: write_function(value) -> bool
        The bool return value of write indicates success / failure
        """
        self.__instance_count += 1
        if alias is None:
            alias = self.__instance_count

        self.__read_fn = read_function
        self.__write_fn = write_function
        
        self.__alias = alias
        self.__disable_alias = disable_alias
        self.__name = name
        self.__datatype = datatype
        self.__value_key = datatype.value_key
        self.__value_key_camel_case = datatype.value_key_camel_case

        self.__read_millis = 0
        self.__current_value = None
        self.__prev_value = None

        self.__rbe_ignore = rbe_ignore

        self.__properties = self.make_metric_properties([{'key': 'readOnly', 'type': 11, 'value': not self.writable}])
        self.__coerce_fn = datatype.coerce_fn

    @property
    def name(self) -> str:
        return self.__name

    @property
    def alias(self) -> str:
        return self.__alias
    
    @property
    def writable(self) -> bool:
        return self.__write_fn is not None

    @property
    def sparkplug_datatype(self) -> SparkplugDataTypes:
        return self.__datatype

    @property
    def value_key(self) -> str:
        return self.__value_key

    @property
    def value_key_camel_case(self) -> str:
        return self.__value_key_camel_case
    
    @property
    def value_changed(self) -> bool:
        return self.__prev_value != self.__current_value
    
    @property
    def current_value(self):
        return self.__current_value
    
    @property
    def rbe_ignore(self) -> bool:
        return self.__rbe_ignore

    @staticmethod
    def make_metric_properties(metric_props: List[dict]) -> dict:
        """take simple list of dict with keys 'key', 'value', 'type' and format for use in property structure for metric"""
        props_formatted = {
            'keys': [],
            'values': []
        }
        for property in metric_props:
            props_formatted['keys'].append(property['key'])
            datatype = SparkplugDataTypes(property['type'])
            props_formatted['values'].append({'type': property['type'], datatype.value_key: property['value']})
        return props_formatted
    
    def read(self) -> bool:
        try:
            prev_value = self.__current_value
            self.__current_value = self.__read_fn(prev_value)
            self.__read_millis = helpers.millis()
            self.__prev_value = prev_value
        except Exception as err:
            return False
        return True

    def write(self, value) -> bool:
        try:
            if self.__write_fn is not None and self.__write_fn(self.__coerce_fn(value)):
                # self.read()
                return True
            return False
        except Exception:
            return False

    def as_birth_metric(self) -> dict:
        metric = {
            'timestamp': self.__read_millis,
            'name': self.__name,
            'datatype': self.__datatype.value,
            'properties': self.__properties
        }
        if not self.__disable_alias:
            metric['alias'] = self.__alias
        if self.__current_value is None:
            metric['is_null'] = True
        else:
            metric[self.__value_key] = self.__current_value
        return metric

    def as_rbe_metric(self) -> dict:
        metric = {
            'timestamp': self.__read_millis,
            self.__value_key: self.__current_value,
            'datatype': self.__datatype.value  # REMOVE THIS LINE FOR SPARKPLUG 3
        }
        if self.__disable_alias:
            metric['name'] = self.__name
        else:
            metric['alias'] = self.__alias

        if self.__current_value is None:
            metric['is_null'] = True
        else:
            metric[self.__value_key] = self.__current_value
        return metric


class SparkplugMemoryTag(SparkplugMetric):
    def __init__(
        self,
        name: str,
        datatype: SparkplugDataTypes,
        writable: bool = False,
        initial_value = None,
        alias: int = None,
        disable_alias: bool = False,
        rbe_ignore: bool = False,
        persistent: bool = False
    ) -> None:
        self.__mem_value = initial_value
        self.__persistent = persistent
        super().__init__(
            name=name,
            datatype=datatype,
            read_function=self.__mem_reader,
            write_function=self.__mem_writer if writable else None,
            alias=alias,
            disable_alias=disable_alias,
            rbe_ignore=rbe_ignore
        )
        if persistent:  # Get value from storage
            raise NotImplementedError
        self.read()

    def __mem_reader(self, prev_value):
        if not self.__persistent:
            return self.__mem_value

    def __mem_writer(self, value) -> bool:
        if not self.__persistent:
            self.__mem_value = value
            return True


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
        metrics: List[SparkplugMetric] = None,
        host_application_id: str = None,
        scan_rate: int = None
        ) -> None:

        metrics = [] if metrics is None else metrics
        for metric in metrics:
            if metric.name in ['Node Control/Scan Rate', 'Node Control/Rebirth']:
                raise ValueError(f'Invalid metric name: "{metric.name}"!')

        self.__topics = SparkplugEdgeNodeTopics(group_id=group_id, edge_node_id=edge_node_id, host_application_id=host_application_id)
        self.__brokers = brokers
        self.__metrics = metrics
        self.__running = False

        scan_rate = 1000 if not scan_rate or scan_rate > 3600000 or scan_rate < 500 else scan_rate

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

        self.__nbirth_payload = None
        self.__ndeath_payload = None

        self.__mid_deque = deque(maxlen=10)

        self.__on_client_callback = None

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
        self.__client.message_callback_add(self.__topics.NCMD, self.__on_ncmd_message)
        if self.__on_client_callback:
            self.__on_client_callback(self.__client)
    
    def __set_broker(self, idx: int):
        if self.__running:
            self.__client.loop_stop()
        self.__current_broker_idx = idx
        self.__set_client(self.primary_broker.create_client())

    def start_client(self):
        if self.__running:
            self.__client.loop_stop()

        self.make_bd_payloads()
        
        broker = self.current_broker
        self.__client.will_set(topic=self.__topics.NDEATH, payload=self.__ndeath_payload, qos=1)

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

    def save_config(self):
        '''
        Save config to file on ssd
        '''

    @classmethod
    def from_config_file(cls):
        '''
        Load edge node data from ssd
        '''

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
    def last_read_delta(self) -> int:
        return helpers.millis() - self.__last_read
    
    @property
    def read_due(self) -> bool:
        if self.__scan_rate.current_value is None:
            return True
        return self.last_read_delta >= self.__scan_rate.current_value
    
    def make_payload_from_metrics(self, metrics: List[dict]) -> bytes:
        payload_dict = {
            'timestamp': helpers.millis(),
            'seq': self.__seq.current_value,
            'metrics': metrics
        }
        return ParseDict(payload_dict, sparkplug_pb2.Payload()).SerializeToString()

    def loop_forever(self):
        while True:
            if self.read_due:
                logging.debug('Tag Read Due!')
                self._rbe()
                

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
    def make_bd_payloads(self, rebirth: bool = False) -> bool:
        millis = helpers.millis()
        self.__seq.reset()  # Remove this line for sparkplug 3.0.0
        self.__ndeath_payload = ParseDict({
            'timestamp': millis,
            'metrics': [
                {
                    'timestamp': millis,
                    'name': 'bdSeq',
                    'datatype': SparkplugDataTypes.UInt64.value,
                    'long_value': self.__bdseq.current_value - 1 if rebirth else self.__bdseq.current_value
                }
            ]
        }, sparkplug_pb2.Payload()).SerializeToString()

        payload = {
            'timestamp': millis,
            'seq': self.__seq.current_value,
            'metrics': [
                {
                    'timestamp': millis,
                    'name': 'bdSeq',
                    'datatype': SparkplugDataTypes.UInt64.value,
                    'long_value': self.__bdseq.current_value - 1 if rebirth else self.__bdseq.current_value
                },
                {
                    'timestamp': millis,
                    'name': 'Node Control/Rebirth',
                    'datatype': SparkplugDataTypes.Boolean.value,
                    'boolean_value': False
                }
            ]
        }
        # TODO ADD METRICS TO PAYLOAD --> payload['metrics'].extend(self.__read_metrics_callback(rbe=True))
        payload['metrics'].extend(self.read(rbe=False))
        
        try:
            self.__nbirth_payload = ParseDict(payload, sparkplug_pb2.Payload()).SerializeToString()
        except (EncodeError, ParseError) as err:
            logging.error(f'FAILED TO MAKE BIRTH PAYLOAD: {err}')
            return False

        return True

    def __sparkplug_message_published(self):
        logging.debug(f'SPARKPLUG MESSAGE PUBLISHED (seq: {self.__seq.current_value})')
        self.__seq.next_value()

    '''
    MQTT paho-mqtt client functions
    '''
    def __mqtt_publish(self, client: mqtt_functions.mqtt.Client, topic: str, payload: str or bytes, qos: int = 0, retain: bool = False):
        result = client.publish(topic=topic, payload=payload, qos=qos, retain=retain)
        self.__mid_deque.append(result.mid)

    def __on_mqtt_connect(self, client, userdata, flags, rc, reasonCode = None, properties = None):
        logging.debug(f'self.on_connect() result: {mqtt_functions.ReturnCodes(rc).description}')
        client.subscribe(self.__topics.NCMD)
        self.__mqtt_publish(client, self.__topics.NBIRTH, self.__nbirth_payload)
        logging.debug(f'published NBIRTH')
        self.__bdseq.next_value()

    def __on_mqtt_publish(self, client, userdata, mid):
        logging.debug(f'MQTT MESSAGE PUBLISHED')
        if mid is not None and mid in self.__mid_deque:
            self.__sparkplug_message_published()

    def __on_mqtt_disconnect(self, client, userdata, rc):
        logging.error('MQTT DISCONNECTED')

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
                            logging.warning(f'Cannot write NCMD to read only tag "{metric_obj.name}"')
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

                        logging.debug(f'NCMD, write metric: (key {metric_obj.value_key_camel_case})')
                        logging.debug(str(metric))
                        break
            
            if trigger_rebirth:
                if self.make_bd_payloads(rebirth=True):
                    self.__mqtt_publish(client=client, topic=self.__topics.NBIRTH, payload=self.__nbirth_payload)
                    logging.debug('Rebirth Published!')
                return
            if not trigger_publish:
                return
            self._rbe()
        except (DecodeError, KeyError, ValueError) as err:
            logging.error(f'NCMD failed: {err}')
    

class SparkplugDevice:
    def __init__(self, edge_node: SparkplugEdgeNode, device_id: str, metrics: List[SparkplugMetric] = None) -> None:
        raise NotImplementedError



