from sparkplug_node_app import helpers
from enum import Enum
import os
import logging
from typing import List, Callable, Optional
import json

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
        on_write = None,
        on_read = None
    ) -> None:
        """
        read function signature: read_function(prev_value)
        returns value, whatever its datatype is

        write function signature: write_function(value) -> bool
        The bool return value of write indicates success / failure

        on_read callback signature: on_read(metric_obj=self, current_value=value, success=success)
        on_write callback signature: on_write(metric_obj=self, value_written=value, success=success)
        """
        self.__instance_count += 1
        if alias is None:
            alias = self.__instance_count

        self.__read_fn = read_function
        self.__on_read = on_read if on_read and callable(on_read) else None
        self.__write_fn = write_function
        self.__on_write = on_write if on_write and callable(on_write) else None
        
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
    def previous_value(self):
        return self.__prev_value
    
    @property
    def current_value(self):
        return self.__current_value

    @property
    def disable_alias(self) -> bool:
        return self.__disable_alias
    
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
        success = True
        try:
            prev_value = self.__current_value
            self.__current_value = self.__read_fn(prev_value)
            self.__read_millis = helpers.millis()
            self.__prev_value = prev_value
        except Exception as err:
            success = False

        if self.__on_read:
            self.__on_read(metric_obj=self, current_value=self.__current_value, success=success)
        return success

    def write(self, value) -> bool:
        if not self.writable:
            return False
        success = True
        try:
            value = self.__coerce_fn(value)
            success = self.__write_fn(value)
        except Exception:
            success = False
        if self.__on_write:
            self.__on_write(metric_obj=self, value_written=value, success=success)
        return success

    @staticmethod
    def int_to_uint(value, bit_size=32) -> int:
        if not isinstance(value, int):
            return None
        # Calculate the max value for the given bit size
        max_uint = 2 ** bit_size
        # Convert the int to uint
        if value < 0:
            return max_uint + value
        return value
    
    def __set_value_for_payload(self, metric_dict: dict):
        if self.__current_value is None:
            metric_dict['is_null'] = True
            return

        value = self.__current_value
        if self.__value_key == 'long_value':
            value = self.int_to_uint(self.__current_value, bit_size=64)
        elif self.__value_key == 'int_value':
            value = self.int_to_uint(self.__current_value, bit_size=32)
        metric_dict[self.__value_key] = value
        

    def as_birth_metric(self) -> dict:
        metric = {
            'timestamp': self.__read_millis,
            'name': self.__name,
            'datatype': self.__datatype.value,
            'properties': self.__properties
        }
        if not self.__disable_alias:
            metric['alias'] = self.__alias
        
        self.__set_value_for_payload(metric)
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

        self.__set_value_for_payload(metric)
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
        persistence_file: Optional[str] = None,
        on_write: Optional[Callable] = None,
        on_read: Optional[Callable] = None,
        write_validator: Optional[Callable] = None
    ) -> None:
        """
        write_validator function signature write_validator(current_value, new_value) -> bool
        returns False if new_value is invalid, True if it is
        """
        self.__mem_value = initial_value
        self.__persistence_file = persistence_file
        self.__write_validator = write_validator if callable(write_validator) else None

        init_args = dict(
            name=name,
            datatype=datatype,
            read_function=self.__mem_reader,
            write_function=self.__mem_writer if writable else None,
            alias=alias,
            disable_alias=disable_alias,
            rbe_ignore=rbe_ignore,
            on_write=on_write,
            on_read=on_read
        )
        
        if persistence_file:  # Get value from storage
            self.__create_persistence_file()
            persistence_data = self.__read_persistence_file()
            if persistence_data is None:
                logging.warning(f'Persitence data could not be loaded "{persistence_file}"')
            elif name in persistence_data.keys():
                if 'current_value' in persistence_data[name].keys():
                    self.__mem_value = persistence_data[name]['current_value']
                for key in init_args.keys():
                    if key not in persistence_data[name].keys():
                        continue
                    init_args[key] = persistence_data[name][key]
        
        super().__init__(**init_args)

        self.read()

    def __create_persistence_file(self):
        if not os.path.isfile(self.__persistence_file):
            directory_path = os.path.dirname(self.__persistence_file)
            os.makedirs(directory_path, exist_ok=True)
            with open(self.__persistence_file, 'w', newline='') as file:
                json.dump({}, file)
            logging.info(f'Created SparkplugMemoryTag persistence file "{self.__persistence_file}"')

    def __read_persistence_file(self) -> Optional[dict]:
        if not self.persistent:
            return None
        try:
            with open(self.__persistence_file, 'r') as file:
                return json.load(file)
        except json.JSONDecodeError as err:
            logging.error(f'Could not load persistence file for Memory Tag "{self.name}" (invalid json)')
            return None

    def save_to_disk(self):
        if not self.persistent:
            logging.warning(f'Cannot save tag "{self.name}", no persistence file configured!')
            return

        existing_file_data = self.__read_persistence_file()
        if existing_file_data is None:
            self.__create_persistence_file()
            existing_file_data = {}
        
        existing_file_data[self.name] = self.get_config()
        with open(self.__persistence_file, 'w', newline='') as file:
            json.dump(existing_file_data, file, indent=4)

    def __mem_reader(self, prev_value):
        return self.__mem_value

    def __mem_writer(self, value) -> bool:
        if self.__write_validator is not None and not self.__write_validator(self.current_value, value):
            return False
        self.__mem_value = value
        return True

    @property
    def persistent(self) -> bool:
        return self.__persistence_file is not None

    def get_config(self) -> dict:
        return {
            'name': self.name,
            'alias': self.alias,
            'writable': self.writable,
            'datatype_value': self.sparkplug_datatype.value,
            'disable_alias': self.disable_alias,
            'rbe_ignore': self.rbe_ignore,
            'persistent': self.persistent,
            'current_value': self.current_value
        }