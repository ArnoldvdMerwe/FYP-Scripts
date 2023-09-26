import paho.mqtt.client as paho
from influxdb import InfluxDBClient
from datetime import datetime, timezone
import sys


# Get the topic level which shows which Shelly is sending the MQTT message
def get_second_topic_level(topic):
    first_slash_index = topic.find("/")
    second_slash_index = topic.find("/", first_slash_index + 1)
    second_level_topic = topic[first_slash_index + 1 : second_slash_index]
    return second_level_topic


# Get the topic level which shows what property is being sent.
def get_last_topic_level(topic):
    last_slash_index = topic.rfind("/")
    last_topic = topic[last_slash_index + 1 :]
    return last_topic


def on_message(client, userdata, msg):
    # Get the device and measurement that is being reported
    device = get_second_topic_level(msg.topic)
    measurement = get_last_topic_level(msg.topic)

    json_body = [
        {
            "measurement": measurement,
            "tags": {"device": device},
            "time": str(datetime.now(timezone.utc)),
            "fields": {"value": float(msg.payload.decode())},
        }
    ]
    influx_client.write_points(json_body, database="renewable_energy_trading_platform")


influx_client = InfluxDBClient("localhost", 8086)
client = paho.Client()
client.on_message = on_message

if client.connect("localhost", 1883, 60) != 0:
    print("Could not connect to MQTT broker!")
    sys.exit(-1)

client.subscribe("shellies/+/emeter/0/#", qos=0)

try:
    print("Press CTRL+C to exit...")
    client.loop_forever()
except:
    print("Disconnecting from broker!")

client.disconnect()
