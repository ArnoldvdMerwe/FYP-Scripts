import paho.mqtt.client as paho
import pandas as pd
import sys
import os

total_num_measurements = 60
measurement_num = 0
measurements = []

# Get reference power
ref_power = float(sys.argv[1])
print("//////////////////////////////////////////////////")
print("Reference power [W]:", ref_power)

# Check if file already exists
# If it does use it
if os.path.isfile("test_result.csv"):
    print("Reading CSV file")
    df = pd.read_csv("test_result.csv")
else:
    df = pd.DataFrame(
        data=None, columns=["Shelly Power Measurement [W]", "Reference Power [W]"]
    )


def on_message(client, userdata, msg):
    global measurement_num, df
    # Get measurement and store it
    received_msg = float(msg.payload.decode())
    print(received_msg)
    measurements.append(received_msg)
    measurement_num += 1

    # Check if received enough measurements
    if measurement_num >= total_num_measurements:
        print("Obtained all measurements, stopping...")
        client.disconnect()
        data = {
            "Shelly Power Measurement [W]": measurements,
            "Reference Power [W]": [ref_power] * total_num_measurements,
        }
        new_df = pd.DataFrame(data)
        df = pd.concat([df, new_df], ignore_index=True)
        df = df.sort_values("Reference Power [W]")

        df.to_csv("test_result.csv", index=False)
        sys.exit(0)


client = paho.Client()
client.on_message = on_message

if client.connect("localhost", 1883, 60) != 0:
    print("Could not connect to MQTT broker!")
    sys.exit(-1)

client.subscribe("shellies/test/emeter/0/power", qos=0)

try:
    print("Connected to broker.")
    print("Press CTRL+C to exit...")
    client.loop_forever()
except:
    print("Disconnecting from broker!")

client.disconnect()
