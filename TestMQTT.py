"""
This Python file is for capturing all MQTT messages being transmitted over network.
It was used in testing the functional capability of the system.
"""

import paho.mqtt.client as paho
from datetime import datetime, timezone
import sys


def on_message(client, userdata, msg):
    # Get the device and measurement that is being reported
    print("Topic: ", msg.topic, "| Message: ", msg.payload.decode())


client = paho.Client()
client.on_message = on_message

if client.connect("localhost", 1883, 60) != 0:
    print("Could not connect to MQTT broker!")
    sys.exit(-1)

client.subscribe("#", qos=2)

try:
    print("Press CTRL+C to exit...")
    client.loop_forever()
except:
    print("Disconnecting from broker!")

client.disconnect()
