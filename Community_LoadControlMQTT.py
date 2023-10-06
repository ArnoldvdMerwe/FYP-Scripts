from influxdb import InfluxDBClient
import mariadb
import sys
import sched
import time
import paho.mqtt.client as paho

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

# Get homes
cur.execute("select home_number from home")
try:
    home_numbers = cur.fetchall()
    # Transform list of tuples into single list
    home_numbers = [item for sublist in home_numbers for item in sublist]
except mariadb.Error as e:
    print(f"Error fetching home numbers: {e}")
    sys.exit(1)


def control_home_loads(scheduler):
    # Schedule next call
    scheduler.enter(20, 1, control_home_loads, (scheduler,))

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
            print("Supposed to publish")
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

    unmessaged_home_numbers = list(
        (set(home_numbers) - set(opted_out_home_numbers)) - set(load_limited_homes)
    )
    for home in unmessaged_home_numbers:
        client.publish(f"homes/home-{home}/load", "deactivate")


# Setup scheduler
s = sched.scheduler(time.time, time.sleep)
s.enter(2, 1, control_home_loads, (s,))
s.run()