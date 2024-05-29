import re
import telnetlib
import mysql.connector
import time
from datetime import datetime

command = "\x01" + "200"  # CTRL+A200

# Function to perform telnet and insert tank readings directly into the database
# Function to perform telnet and insert tank readings directly into the database
def perform_telnet_and_insert(ip, port, command, cursor):
    try:
        tn = telnetlib.Telnet(ip, port, timeout=30)
        print("Connected to", ip, "on port", port)

        time.sleep(3)

        tn.write(command.encode('ascii') + b'\r\n')

        response = tn.read_until(b'\n', timeout=30)  # Read until newline or timeout
        while response:
            decoded_line = response.decode('utf-8').strip()
            if 'TANK  PRODUCT' in decoded_line or 'MAY' in decoded_line or '200' in decoded_line:
                # Skip header, date, and initial response lines
                response = tn.read_until(b'\n', timeout=30)
                continue
            data = parse_row(decoded_line)
            if data:
                insert_into_database(data, cursor, site_number)
            response = tn.read_until(b'\n', timeout=30)
    except Exception as e:
        print("Error:", e)


# Function to parse a single row of data
def parse_row(row):
    match = re.match(r'\s*(\d+)\s+(.*?)\s+(\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)\s+(\d+)', row.strip())
    if match:
        tank, product, gallons, inches, water, deg_f, ullage = match.groups()
        site_number = "12345"

        reading_date = datetime.now().strftime('%Y-%m-%d')
        reading_time = datetime.now().strftime('%H:%M:%S')

        total_capacity = int(ullage) + int(gallons)

        return {
            'tank': int(tank),
            'product': product.strip(),
            'gallons': int(gallons),
            'inches': float(inches),
            'water': float(water),
            'deg_f': float(deg_f),
            'ullage': int(ullage),
            'site_number': str(site_number),
            'total_capacity': total_capacity,
            'reading_date': str(reading_date),
            'reading_time': str(reading_time),
        }
    else:
        print("Error parsing row:", row)
        return None


# Function to insert data into the database
# Function to insert data into the database
# Function to insert data into the database
def insert_into_database(data, cursor, site_number):
    try:
        # Insert data into tankreadings table, excluding the 'id' column
        query = """INSERT INTO tankreadings (tank, product, gallons, inches, water, deg_f, ullage, site_number, total_capacity, reading_date, reading_time)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        values = (data['tank'], data['product'], data['gallons'], data['inches'], data['water'], data['deg_f'], data['ullage'], site_number, data['total_capacity'], data['reading_date'], data['reading_time'])
        cursor.execute(query, values)
        db_connection.commit()  # Commit the transaction
        print("Inserted data into database:", data)
    except Exception as e:
        print("Error inserting data into database:", e)
        # Rollback the transaction in case of error
        db_connection.rollback()

# MySQL database parameters
endpoint = "database-1.chakckkok58r.us-east-1.rds.amazonaws.com"
mysql_port = "3306"
username = "admin"
password = "Swingswin1"
schema = "fuel-levels"
sites_table = "sites"

try:
    print("Connecting to MySQL database...")
    db_connection = mysql.connector.connect(
        host=endpoint,
        port=mysql_port,
        user=username,
        passwd=password,
        database=schema
    )
    cursor = db_connection.cursor()
    print("Connected to MySQL database successfully.")

    print("Querying sites...")
    cursor.execute("SELECT ipaddress, port, sitenumber FROM {}".format(sites_table))
    sites = cursor.fetchall()
    print("Sites queried successfully.")
    
    # Iterate through the sites and perform telnet and insert tank readings
    for site in sites:
        ip_address, site_port, site_number = site
        perform_telnet_and_insert(ip_address, site_port, command, cursor)
except mysql.connector.Error as err:
    print("Error connecting to/querying MySQL:", err)
finally:
    # Close MySQL connection
    if 'db_connection' in locals() or 'db_connection' in globals():
        cursor.close()
        db_connection.close()
