from flask import Flask, request
app = Flask(__name__) 

# Import sqlite3 for make_connection()
import sqlite3

# Import pandas
import pandas as pd

# Define a function to create connection for reusability purpose
def make_connection():
    connection = sqlite3.connect('austin_bikeshare.db')
    return connection

# Make a connection
conn = make_connection()

@app.route('/')
def home():
    return 'Hello World'

##################################################################################
@app.route('/stations/')
def route_all_stations():
    conn = make_connection()
    stations = get_all_stations(conn)
    return stations.to_json()

# get_all_stations() function

def get_all_stations(conn):
    query = f"""SELECT * FROM stations"""
    result = pd.read_sql_query(query, conn)
    return result
##################################################################################
@app.route('/trips/')
def route_all_trips():
    conn = make_connection()
    trips = get_all_trips(conn)
    return trips.to_json()

# get_all_trips() function

def get_all_trips(conn):
    query = f"""SELECT * FROM trips"""
    result = pd.read_sql_query(query, conn)
    return result
##################################################################################
@app.route('/stations/<station_id>')
def route_stations_id(station_id):
    conn = make_connection()
    stations = get_station_id(station_id, conn)
    return stations.to_json()

# get_station_id() function

def get_station_id(station_id, conn):
    query = f"""SELECT * FROM stations WHERE station_id = {station_id}"""
    result = pd.read_sql_query(query, conn)
    return result 
##################################################################################
@app.route('/trips/<trip_id>')
def route_trips_id(trip_id):
    conn = make_connection()
    trips = get_trip_id(trip_id, conn)
    return trips.to_json()

# get_trip_id() function

def get_trip_id(trip_id, conn):
    query = f"""SELECT * FROM trips WHERE trip_id = {trip_id}"""
    result = pd.read_sql_query(query, conn)
    return result 
##################################################################################

@app.route('/json', methods=['POST']) 
def json_example():

    req = request.get_json(force=True) # Parse the incoming json data as Dictionary

    name = req['name']
    age = req['age']
    address = req['address']

    return (f'''Hello {name}, your age is {age}, and your address in {address}
            ''')

##################################################################################
@app.route('/stations/add', methods=['POST']) 
def route_add_station():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)

    conn = make_connection()
    result = insert_into_stations(data, conn)
    return result

# insert_into_stations() function

def insert_into_stations(data, conn):
    query = f"""INSERT INTO stations values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'
##################################################################################
@app.route('/trips/add', methods=['POST']) 
def trips_add_station():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)

    conn = make_connection()
    result = insert_into_trips(data, conn)
    return result
 
# insert_into_trips() function

def insert_into_trips(data, conn):
    query = f"""INSERT INTO trips values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'
##################################################################################
                                #CHALLENGE#
##################################################################################

## STATIC ENDPOINT

@app.route('/trips/average_duration')
def route_trip_average_duration():
    conn = make_connection()
    trips = get_trip_average_duration(conn)
    return trips.to_json()

# get_trip_average_duration() function

def get_trip_average_duration(conn):
    query = f"""SELECT AVG(duration_minutes) FROM trips"""
    result = pd.read_sql_query(query, conn)
    return result

##################################################################################

## DYNAMIC ENDPOINT

@app.route('/trips/average_duration/<bikeid>')
def route_bike_id(bikeid):
    conn = make_connection()
    trips = get_bike_id(bikeid, conn)
    return trips.to_json()

# get_bike_id() function

def get_bike_id(bikeid, conn):
    query = f"""SELECT id FROM trips WHERE bikeid = {bikeid}"""
    result = pd.read_sql_query(query, conn)
    return result

##################################################################################

## POST ENDPOINT

@app.route('/json/subscriber_type', methods=['POST']) 
def json_subs():
    input_data = request.get_json() # Get the input as dictionary
    specified_date = input_data

    conn = make_connection()
    query = f"SELECT * FROM trips WHERE start_time LIKE ('2016-01%')"
    selected_data = pd.read_sql_query(query, conn)

    result = selected_data.groupby('subscriber_type').agg({
    'bikeid' : 'count', 
    'duration_minutes' : 'mean'})

    return result.to_json()

if __name__ == '__main__':
    app.run(debug=True, port=5000)

