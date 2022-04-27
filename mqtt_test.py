# Simple HiveMQ Client built with PyCharm
# excellent resource here:
# https://github.com/eclipse/paho.mqtt.python#id3
import paho.mqtt.client as paho  # pip install paho.mqtt
from datetime import datetime
import time
import argparse
import random
from json import JSONEncoder
import json
import signal

keepRunning = True


def signal_handler(signalval, frame):
    print(f'Signal {signalval} {frame}')
    global keepRunning
    keepRunning = False


def on_connect(client, userdata, flags, rc):
    print(f'CONNACK received with code {rc}.')  # is 5 with wrong password. I thought connect fail would have hit
    if rc == 0:
        print("Succesful Connection")
    else:
        print("Error Connecting")
    print(client)
    print(userdata)
    print(flags)


def on_connect_fail(client, userdata, flags, rc):
    print(f'connection failed with code {rc}.')  # this does not get hit when there's a bad password
    print(client)
    print(userdata)
    print(flags)


def on_disconnect(client, userdata, rc):
    print(f'On disconnect callback {client}, {userdata} {rc}')


def on_message(client, userdata, message):
    print(f'Received data from topic: {message.topic} payload: {message.payload} userdata: {userdata} client: {client}')


def on_publish(client, userdata, mid):
    print(f'Publish callback {client}, {userdata}, {mid}')


def on_subscribe(client, userdata, mid, granted_qos):
    print(f'Subscribe callback {client}, {userdata}, {mid}, {granted_qos}')


def on_unsubscribe(client, userdata, mid):
    print(f'Unsubscribe callback {client}, {userdata}, {mid}')


def connect_hivemq(publisher, subscriber, mqtt_url, mqtt_port, mqtt_user, mqtt_pw):
    print(f'Connecting to {mqtt_url} on port {mqtt_port}')
    if publisher is True and subscriber is True:
        name = "MQTT Broker Test Pub-Sub"
    elif publisher is True:
        name = "MQTT Broker Test Pub"  # separate pub and sub did not work with the same name
    else:
        name = "MQTT Broker Test Sub"
    client = paho.Client(name, userdata=name)  # putting the name in userdata just to see how it works
    if mqtt_port == 8883:
        client.tls_set()  # needed because port 8883 is SSL/TLS by convention (I guessed and it worked)
    client.username_pw_set(mqtt_user, mqtt_pw)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_connect_fail = on_connect_fail
    client.on_message = on_message
    client.on_publish = on_publish
    client.on_subscribe = on_subscribe
    client.on_unsubscribe = on_unsubscribe
    client.connect(mqtt_url, mqtt_port)
    client.loop(3.0)  # a little time for the callback to be hit, maybe
    return client


def publish_current_time(client, topics):
    current = datetime.now()
    start = datetime(1970, 1, 1)
    payload = (current - start).total_seconds()
    for i in range(0,len(topics)-1):
        client.publish(topics[i][0], payload)
        payload = payload / ((i + 1) * 1000)


def print_start_msg(args):
    print(f'Starting MQTT Test 1')  # Press Ctrl+F8 to toggle the breakpoint.
    print(f'{args}')


def run(client, publisher, subscriber, topics):
    if subscriber:
        client.subscribe(topics)
    while keepRunning:
        if publisher:
            publish_current_time(client, topics)
            time.sleep(random.randint(1, 5))  # simulate doing some work
        client.loop(10.0)  # this lop will return before timeout of there is data (at least testing suggests this)
    print('Exiting application')
    for i in range(0,len(topics)-1):
        client.unsubscribe(topics[i][0])
    client.disconnect(0)
    client.loop(3.0)


def read_config(path_to_config):
    with open(path_to_config, 'r') as configfile:
        data = configfile.read()
    obj = json.loads(data)
    return obj

class Topic:
    description = None
    path = None
    qos = None

    def __init__(self, description, path, qos):
        self.description = description
        self.path = path
        self.qos = qos


class Configuration:
    description = None
    subscriber = None
    publisher = None
    mqtt_url = None
    mqtt_port = 1883
    mqtt_user = None
    mqtt_pw = None
    topics = None

    def __init__(self, subscriber, publisher, mqtt_url, mqtt_port, mqtt_user, mqtt_pw, topics):
        self.subscriber = subscriber
        self.publisher = publisher
        self.mqtt_url = mqtt_url
        self.mqtt_port = mqtt_port
        self.mqtt_user = mqtt_user
        self.mqtt_pw = mqtt_pw
        self.topics = topics


class ConfigurationEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Configuration):
            return obj.__dict__
        else:
            return json.JSONEncoder.default(self, obj)


if __name__ == '__main__':
    topics = []
    topics.append( Topic("", "datasource/attr1", 0).__dict__ )
    topics.append( Topic("", "datasource/attr2", 0).__dict__ )
    topics.append( Topic("", "datasource/attr3", 0).__dict__ )

    example_config = Configuration(True, True, "mqtt.example.com", 8883, "someuser", "somepassword", topics)
    jsonString = ConfigurationEncoder().encode(example_config)
    parser = argparse.ArgumentParser(description='Create a publisher and/or a subscriber to a MQTT Broker',
                                     allow_abbrev=True, epilog='This app can run just as subscriber, just as a '
                                                               'publisher, or both. Configuration is passed in a json '
                                                               'file. Example JSON configuration file: '
                                                               f'{jsonString}')
    parser.add_argument('-c', '--config_file', type=argparse.FileType('r'), help='json file with configuration data')
    appArgs = parser.parse_args()
    print_start_msg(appArgs)
    config = read_config(appArgs.config_file.name)
    signal.signal(signal.SIGINT, signal_handler)
    mqttc = connect_hivemq(config['publisher'], config['subscriber'], config['mqtt_url'], config['mqtt_port'],
                           config['mqtt_user'],
                           config['mqtt_pw'])
    ts = config["topics"]
    topics = []
    for t in ts:
        topics.append((t["path"], t["qos"]))
    run(mqttc, config['publisher'], config['subscriber'], topics)
