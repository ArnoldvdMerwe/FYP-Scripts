from influxdb import InfluxDBClient
import mariadb
import sys
import sched
import time
from datetime import datetime

num_homes = 2

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

# Fetch initial energy usage
old_energy_usage_dict = {}
for i in range(num_homes):
    query = influx_conn.query(
        f"select value from total where device = 'home-{i+1}' order by time desc limit 1",
        database="renewable_energy_trading_platform",
    )

    # If measurements in database add it to dict
    for row in query.get_points():
        old_energy_usage_dict[f"home-{i+1}"] = row["value"]

    # Use 0 if there is no measurements in database
    if f"home-{i+1}" not in old_energy_usage_dict:
        old_energy_usage_dict[f"home-{i+1}"] = 0


def calculate_balances(scheduler):
    # Schedule next call
    scheduler.enter(15, 1, calculate_balances, (scheduler,))

    # Fetch the newest energy usage for each home
    current_energy_usage_dict = {}
    for i in range(num_homes):
        # Fetch current energy total
        current_energy_usage_query = influx_conn.query(
            f"select value from total where device = 'home-{i+1}' order by time desc limit 1",
            database="renewable_energy_trading_platform",
        )

        for row in current_energy_usage_query.get_points():
            current_energy_usage_dict[f"home-{i+1}"] = row["value"]

        # Use same as old energy usage if there are no measurements in database
        if f"home-{i+1}" not in current_energy_usage_dict:
            current_energy_usage_dict[f"home-{i+1}"] = old_energy_usage_dict[
                f"home-{i+1}"
            ]

    # Calculate difference from old energy usage and convert to kWh
    diff_energy_usage_dict = {}
    for i in range(num_homes):
        diff_energy_usage_dict[f"home-{i+1}"] = (
            current_energy_usage_dict[f"home-{i+1}"]
            - old_energy_usage_dict[f"home-{i+1}"]
        ) / 1000

    # Check if it is currently loadshedding or not
    loadshedding = False
    cur.execute("select * from general where field = 'loadshedding'")
    row = cur.fetchone()
    if row[1] == "true":
        loadshedding = True

    # Fetch current electrical rate
    cur.execute("select * from electrical_rate")
    current_time = datetime.now().time()
    # Get correct time slot
    current_rate = 0
    row = cur.fetchone()
    in_range = time_in_range(
        (datetime.min + row[0]).time(), (datetime.min + row[1]).time(), current_time
    )
    if in_range:
        if loadshedding:
            current_rate = row[3]
        else:
            current_rate = row[2]

    # Update account balances
    for i in range(num_homes):
        cur.execute(f"select account_balance from home where home_number = {i+1}")
        row = cur.fetchone()
        # Home is assigned
        if row is not None:
            new_balance = row[0] - (
                (current_rate / 100) * diff_energy_usage_dict[f"home-{i+1}"]
            )
            cur.execute(
                f"update home set account_balance = {new_balance} where home_number = {i+1}"
            )
            conn.commit()

    # Reassign new to old energy usages
    for i in range(num_homes):
        old_energy_usage_dict[f"home-{i+1}"] = current_energy_usage_dict[f"home-{i+1}"]


# Check if time is within a range
def time_in_range(start, end, x):
    """Return true if x is in the range [start, end]"""
    if start <= end:
        return start <= x <= end
    else:
        return start <= x or x <= end


# Setup scheduler
s = sched.scheduler(time.time, time.sleep)
s.enter(15, 1, calculate_balances, (s,))
s.run()
