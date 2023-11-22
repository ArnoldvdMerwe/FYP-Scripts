"""
Python file : Home_LoadLimit.py
This script runs on the home platform.
It creates a MQTT client that subscribes on the topic "homes/home-{num}/load".
Note that the home number must be set correctly before running it on the home platform.
The relays are switched through GPIO as instructed by the community platform.
"""

import RPi.GPIO as GPIO
import paho.mqtt.client as paho
import sys

# Setup
# Set home number (IMPORTANT)
home_num = 1
gpio_pins = {13: False, 15: False}
GPIO.setmode(GPIO.BOARD)
for pin in gpio_pins:
    GPIO.setup(pin, GPIO.OUT)


def on_message(client, userdata, msg):
    # Control relays to reduce power use of home as needed
    received_msg = msg.payload.decode()

    # If msg is activate then reduce load by switching relays
    if received_msg == "activate":
        set_pin = False
        for pin in gpio_pins:
            if not gpio_pins[pin] and not set_pin:
                gpio_pins[pin] = True
                GPIO.output(pin, GPIO.HIGH)
                set_pin = True
    # If msg is full_activate then fully switch off all loads
    elif received_msg == "full_activate":
        for pin in gpio_pins:
            if not gpio_pins[pin]:
                gpio_pins[pin] = True
                GPIO.output(pin, GPIO.HIGH)
    # If msg is deactivate then stop all load control
    elif received_msg == "deactivate":
        for pin in gpio_pins:
            gpio_pins[pin] = False
            GPIO.output(pin, GPIO.LOW)


client = paho.Client()
client.on_message = on_message

if client.connect("localhost", 1883, 60) != 0:
    print("Could not connect to MQTT broker!")
    sys.exit(-1)

client.subscribe(f"homes/home-{home_num}/load", qos=2)

try:
    print("Press CTRL+C to exit...")
    client.loop_forever()
except:
    print("Disconnecting from broker!")

client.disconnect()
GPIO.cleanup()
