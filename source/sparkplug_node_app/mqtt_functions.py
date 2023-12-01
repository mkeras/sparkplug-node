from sparkplug_node_app import env
import paho.mqtt.client as mqtt

from dataclasses import dataclass
from functools import partial
from enum import Enum


class ReturnCodes(Enum):
    def __new__(cls, value, description):
        member = object.__new__(cls)
        member._value_ = value
        member.description = description
        return member

    CONNECTION_ACCEPTED = (0, "Connection Accepted")
    CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION = (1, "Connection Refused: Unacceptable Protocol Version")
    CONNECTION_REFUSED_IDENTIFIER_REJECTED = (2, "Connection Refused: Identifier Rejected")
    CONNECTION_REFUSED_SERVER_UNAVAILABLE = (3, "Connection Refused: Server Unavailable")
    CONNECTION_REFUSED_BAD_USERNAME_OR_PASSWORD = (4, "Connection Refused: Bad Username or Password")
    CONNECTION_REFUSED_NOT_AUTHORIZED = (5, "Connection Refused: Not Authorized")


@dataclass(frozen=True, kw_only=True)
class BrokerInfo:
    client_id: str
    host: str
    port: int
    primary: bool = False
    name: str = None
    username: str = None
    password: str = None
    use_tls: bool = True

    def create_client(self) -> mqtt.Client:
        return create_client(self)


def create_client(broker: BrokerInfo) -> mqtt.Client:
    client = mqtt.Client(client_id=broker.client_id, clean_session=True)
    if broker.username or broker.password: # TODO is this valid to have pw without username or vice versa?
        client.username_pw_set(username=broker.username, password=broker.password)

    if broker.use_tls:
        client.tls_set(cert_reqs=mqtt.ssl.CERT_REQUIRED)

    return client