"""
Python file : Community_LoadControlMQTT.py
This file is responsible for managing the load control of the community.
It checks if load control is active and whether any homes are over the load limit.
If homes are over the load limit, MQTT is used to send an instruction to reduce load.
If load control is inactive, it switches the homes back on.
It repeatedly runs based on an update interval.
The update interval is retrieved from config.ini.
This should match the MQTT interval of the Shelly EMs.
"""

from influxdb import InfluxDBClient
import mariadb
import sys
import sched
import time
import paho.mqtt.client as paho
import configparser

# Read update interval from config file
config = configparser.ConfigParser()
config.read("config.ini")
update_interval: int = int(config["DEFAULT"]["MQTT_time_interval"])

# Connect to MariaDB platform
try:
    conn = mariadb.connect(
        user="arnold",
        password="passmethesalt",
        host="localhost",
        port=3306,
        database="renewable_energy_trading_platform",
    )
    conn.autocommit = False
    print("Connected to MariaDB platform!")
except mariadb.Error as e:
    print(f"Error connecting to MariaDB platform: {e}")
    sys.exit(1)

# Get cursor
cur = conn.cursor()

# Connect to InfluxDB platform
influx_conn = InfluxDBClient("localhost", 8086)
print("Connected to InfluxDB platform!")

# Create and connect MQTT client
client = paho.Client(client_id="Community_LoadControl")
if client.connect("localhost", 1883, 60) != 0:
    print("Could not connect to MQTT broker!")
    sys.exit(1)
client.loop_start()


def control_home_loads(scheduler):
    global update_interval
    update_interval = int(config["DEFAULT"]["MQTT_time_interval"])
    # Update database information
    conn.commit()
    # Get homes
    cur.execute("select home_number from home")
    try:
        home_numbers = cur.fetchall()
        # Transform list of tuples into single list
        home_numbers = [item for sublist in home_numbers for item in sublist]
    except mariadb.Error as e:
        print(f"Error fetching home numbers: {e}")
        sys.exit(1)

    # If homes do not have an account balance left over, then switch the homes off
    cur.execute("select home_number from home where account_balance = 0")
    balance_home_numbers = cur.fetchall()
    if len(balance_home_numbers) != 0:
        # Transform list of tuples into single list
        balance_home_numbers = [
            item for sublist in balance_home_numbers for item in sublist
        ]
        for home in balance_home_numbers:
            client.publish(f"homes/home-{home}/load", "full_activate")

    # If homes do not opt to receive power during loadshedding, then switch the home off
    cur.execute("select * from general where field = 'loadshedding'")
    row = cur.fetchone()
    opted_out_home_numbers = []
    if row[1] == "true":
        cur.execute(
            "select home_number from home where receive_power_loadshedding = 'false'"
        )
        opted_out_home_numbers = cur.fetchall()
        # Transform list of tuples into single list
        opted_out_home_numbers = [
            item for sublist in opted_out_home_numbers for item in sublist
        ]
        for home in opted_out_home_numbers:
            client.publish(f"homes/home-{home}/load", "full_activate")

    # Check if any homes are exceeding the load limit
    # Only do load control if it has been activated
    cur.execute("select * from general where field = 'load_control'")
    row = cur.fetchone()
    load_limited_homes = []
    if row[1] == "true":
        # Get current load limit for each home
        cur.execute("select * from general where field = 'load_limit_home'")
        load_limit = float(cur.fetchone()[1])
        # Get homes that have not been messaged yet
        load_homes = list(set(home_numbers) - set(opted_out_home_numbers))
        # Get current power usage for each home
        for home in load_homes:
            current_power_usage_query = influx_conn.query(
                f"select value from power where device = 'home-{home}' and time > now() - 1m order by time desc limit 1",
                database="renewable_energy_trading_platform",
            )
            current_power_usage = 0
            for row in current_power_usage_query.get_points():
                current_power_usage = row["value"]
            if current_power_usage > load_limit:
                load_limited_homes.append(home)
                client.publish(f"homes/home-{home}/load", "activate")
                # Allow time for load to reduce if fast update interval
                if update_interval < 5:
                    update_interval = 5
    else:
        unmessaged_home_numbers = list(
            (set(home_numbers) - set(opted_out_home_numbers))
            - set(balance_home_numbers)
        )
        for home in unmessaged_home_numbers:
            client.publish(f"homes/home-{home}/load", "deactivate")

    # Schedule next call
    scheduler.enter(update_interval, 1, control_home_loads, (scheduler,))


# Setup scheduler
s = sched.scheduler(time.time, time.sleep)
s.enter(update_interval, 1, control_home_loads, (s,))
s.run()
