from flask import Flask, jsonify, Response
from flask import request as flask_request
from flask import render_template
import pandas as pd
import geopandas as gpd
from flask_sqlalchemy import SQLAlchemy
from geoalchemy2 import Geometry
import csv
from dataclasses import dataclass
from datetime import datetime, timedelta
import requests
import xml.etree.ElementTree as ET
from flask_apscheduler import APScheduler
from apscheduler.schedulers.background import BackgroundScheduler
import os
from dotenv import load_dotenv, find_dotenv
from pymongo import MongoClient
import asyncio
import aiohttp
from bson import json_util
import json
import math
from shapely.geometry import Point
import numpy as np

load_dotenv(find_dotenv())
password = os.environ.get("MONGODB_PWD")

connection_string = f"mongodb+srv://twright:{password}@windfarm.seyuy3o.mongodb.net/?retryWrites=true&w=majority"

cluster = MongoClient(connection_string)

db = cluster['Test']
collection = db['Test']

def insert_many_windspeeds(data):
    collection = db['Test']
    collection.insert_many(data)
    print(f"Windspeed data inserted into database at {datetime.now()}")

def gather_wind_data():
    print(f"Background Task started at {datetime.now()}")
    windfarm_df = pd.read_csv("Windfarm_WebScraped_DataV3.csv")
    now = datetime.now().replace(microsecond=0, second=0, minute=0)
    current_time = now.strftime('%Y-%m-%dT%H:%M')
    timestamp = datetime.today().replace(microsecond=0, second=0, minute=0)
    #Begin a session which keeps the same connection open for all api calls
    with requests.Session() as session:
        # Create a list of the wind speed results results using list comprehension 
        windspeeds = [gather_session_urls(session = session,name = row['Wind Farm Name'],lon = row['Longitude'],lat = row['Latitude'],rotordiameter = row['Rotor diameter'], numberofturbines = row['Number of Turbines'] , cutinspeed=row['Cut-in wind speed'], cutoutspeed=row['Cut-off wind speed'], ratedspeed=row['Rated wind speed'], current_time = current_time,timestamp = timestamp) for index, row in windfarm_df.iterrows()]
        for windspeed in windspeeds:
            print(windspeed)
        insert_many_windspeeds(windspeeds)

def windpower(windspeed: float,rotordiameter: float, cutinspeed: float, cutoutspeed: float, ratedspeed:float):
     # Initiate constants:
    air_density = 1.225  # Standard air density at sea level in kg/m^3
    power_coefficient = 0.593  # Ideal power coefficient (Betz limit)

    #Swept area
    swept_area = math.pi * rotordiameter**2
    #Calculate the power produced by a wind turbine.

    # Power produced by the wind turbine in watts.
    #If the windspeed is less than the cutin wind speed, the turbine does not generate power
    if windspeed < cutinspeed:
        windfarmpower = 0
    #If the windspeed is greater than the rated wind speed wind speed then turbine generates power at it's rated wind speed
    elif windspeed >= ratedspeed:
        windfarmpower =  0.5 * air_density * swept_area * ratedspeed**3 * power_coefficient
    #If the windspeed is greater than the cutout wind speed, the turbine does not generate power for safety reasons
    elif windspeed >= cutoutspeed:
        windfarmpower =  0
    #If the windspeed is between the cutin and cutoff then turbine generates a power curve
    else:
        windfarmpower = 0.5 * air_density * swept_area * windspeed**3 * power_coefficient
    return windfarmpower




def gather_session_urls(session,name,lon,lat, rotordiameter, numberofturbines,cutinspeed, cutoutspeed, ratedspeed, current_time,timestamp):
    url = 'http://metwdb-openaccess.ichec.ie/metno-wdb2ts/locationforecast'
    qto = current_time
    qfrom = qto
    params= {"lat": lat, "long": lon, "from":qfrom, "to": qto}
    with session.get(url, params=params) as resp:
        try:
            response_text = resp.text
            root = ET.fromstring(response_text)
            wind_speed_element = root.find(".//windSpeed")
            wind_direction_element = root.find(".//windDirection")
            # Extract the wind speed and wind direciton html attribute values
            if wind_speed_element is not None:
                print(f"Wind Speed for {name} at lat={lat}, lon={lon}: {wind_speed_element.get('mps')}mps, at {wind_direction_element.get('deg')} degrees")
                windspeed = wind_speed_element.get("mps")
                winddirection = wind_direction_element.get('deg')
                if windspeed:
                    #Calculate the windpower generated by the windfarm at this speed
                    windfarmpower = numberofturbines*windpower(windspeed = float(windspeed), rotordiameter = float(rotordiameter), cutinspeed= float(cutinspeed), cutoutspeed= float(cutoutspeed), ratedspeed= float(ratedspeed))
                    print(windfarmpower)
                else:
                    print("Windspeed value was empty")
                    windfarmpower = 0
                data = {"metadata":{"Wind Farm Name":name,
                                    "Latitude":lat,
                                    "Longitude":lon},
                        "timestamp": timestamp,
                        "windspeed":windspeed,
                        "windpower":windfarmpower,
                        "winddirection": winddirection}
                print(f"Background Task Executed at {datetime.now()}")
                return data
            else:
                print("Wind Speed element not found in the XML.")
                return None
        except Exception as e:
            print(f"Error processing wind data for lat={lat}, lon={lon}: {e}")
            return None
            
def test_scheduler():
    print("Hello World")

# set configuration values
class Config:
    SCHEDULER_API_ENABLED = True

# Initialize app
app = Flask(__name__)
app.config.from_object(Config())
basedir = os.path.abspath(os.path.dirname(__file__))


# Initialize scheduler
scheduler = BackgroundScheduler()
# Add the task to the scheduler to run at the start of every hour
scheduler.add_job(gather_wind_data, 'cron', hour = "*", minute="15")

# Start the scheduler
scheduler.start()

@app.route('/')
def index():
    #Read Windfarm data into a pandas dataframe
    windfarm_data = pd.read_csv("Windfarm_WebScraped_Datav3.csv")
    print(windfarm_data.to_json(orient="records"))
    return render_template('index.html',windfarm_data = windfarm_data.to_json(orient="records"))

@app.route("/windfarm_details", methods = ['POST'])
def windfarm_details():
    #Read Windfarm data into a pandas dataframe
    windfarm_data = pd.read_csv("Windfarm_WebScraped_Datav3.csv")
    if flask_request.method == 'POST':
        windfarm_name = flask_request.json['Windfarm']
        windfarm_details= windfarm_data.loc[windfarm_data['Wind Farm Name']== windfarm_name ]
        # Convert DataFrame to JSON string
        json_data = windfarm_details.to_json(orient='records')
    # Return JSON response
    return json.loads(json_data)

@app.route("/choropleth")
def choropleth():
    geojson_path = 'Counties_-_National_Statutory_Boundaries_-_2019_-_Generalised_20m.geojson'
    print(geojson_path)
    try:
        with open(geojson_path, 'r', encoding='utf-8') as file:
            geojson_data = file.read()
            return Response(geojson_data, content_type='application/json')
    except FileNotFoundError:
        return jsonify({"error": "GeoJSON file not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/lookup", methods=['POST'])
def lookup():
    if flask_request.method == 'POST':
        windfarm_name = flask_request.json['Windfarm']
        print(windfarm_name)
        #This is a MongoDB Query on a collection to check the data for a Wind farm
        windfarm_query = collection.find({"metadata.Wind Farm Name": windfarm_name})
        return_query = []
        for row in windfarm_query:
            print(row)
            return_query.append(row)
    return json.loads(json_util.dumps(return_query))

@app.route("/windpower")
def aggregate_windpower():
    #Read Counties data into a geopandas dataframe
    geojson_path = 'Counties_-_National_Statutory_Boundaries_-_2019_-_Generalised_20m.geojson'
    counties_boundaries = gpd.read_file(geojson_path)
    #This is a MongoDB Query on a collection to check the data for a Wind farm
    aggregate_windpower = collection.aggregate([
        {
            "$group":
            {"_id": "$metadata.Wind Farm Name", "Latitude": {"$first": "$metadata.Latitude"},
            "Longitude": {"$first": "$metadata.Longitude"}, "windpower": {"$sum": "$windpower"}}
        }
        ])
    #Create pandas dataframe
    windpower_df = gpd.GeoDataFrame(aggregate_windpower)
    # Create a GeoSeries of Point geometries from latitude and longitude
    windpower_df['geometry'] = [Point(lon, lat) for lon, lat in zip(windpower_df['Longitude'], windpower_df['Latitude'])]
    windpower_df.crs ="EPSG:4326"

    # Use json_util.dumps to serialize each MongoDB document in the list
    json_documents = [json.loads(json_util.dumps(doc)) for doc in aggregate_windpower]
    # Perform spatial join to match points and polygons
    pointInPolys = gpd.tools.sjoin(windpower_df, counties_boundaries, predicate="within", how='left')

    grouped_by_county = pointInPolys.groupby('COUNTY')['windpower'].sum().reset_index()

    print(grouped_by_county)
    # Assuming 'point_in_polys' is your GeoDataFrame
    

    # Merge based on the 'county' column
    merged_df = pd.merge(counties_boundaries, grouped_by_county, on='COUNTY', how='inner')
    geojson_str = merged_df.to_json()

    return geojson_str

@app.route("/wind")
def wind():
    return render_template('wind.html')

@app.route("/vectorfield")
def vectorfield():
    # Specify timestamp
    target_time = "2024-02-11T19:00:00"  # Example time, format: YYYY-MM-DDTHH:MM:SS

    # Query to find the last set of wind speed entries submitted at a certain time
    query = [
    {
        '$sort': {
            'metadata.Wind Farm Name': 1, 
            'timestamp': -1
        }
    }, {
        '$group': {
            '_id': '$metadata.Wind Farm Name', 
            'lastEntry': {
                '$first': '$$ROOT'
            }
        }
    }, {
        '$replaceRoot': {
            'newRoot': '$lastEntry'
        }
    }
]


    # Execute the aggregation pipeline
    result = collection.aggregate(query)
    i =0
    wind_speeds = []
    wind_angles = []
    latitudes = []
    longitudes =[]
    # Print the results
    for entry in result:
        wind_speeds.append(entry['windspeed'])
        wind_angles.append(entry['winddirection'])
        latitudes.append(entry['metadata']['Latitude'])
        longitudes.append(entry['metadata']['Longitude'])

    # Combine latitudes and longitudes into tuples using zip
    coordinates = list(zip(latitudes, longitudes))

    # Convert wind_angles to a numeric type (if necessary)
    wind_angles = np.array(wind_angles, dtype=float) 
    # Convert wind_angles to a numeric type (if necessary)
    wind_speeds = np.array(wind_speeds, dtype=float)         
    # Convert wind angles from degrees to radians (since trigonometric functions in numpy expect radians)
    wind_angles_rad = np.deg2rad(wind_angles)
    # Calculate u and v components of wind vectors using trigonometric functions
    u_components = -wind_speeds * np.sin(wind_angles_rad)  # Negative sign because positive angles represent clockwise rotation
    v_components = -wind_speeds * np.cos(wind_angles_rad)  # Negative sign because positive angles represent clockwise rotation
    # Combine u_components and v_components into tuples using zip
    wind_vectors = list(zip(u_components, v_components))
    interpolated_wind_vectors = IrelandGrid(wind_vectors,coordinates)
    return render_template('wind.html',coordinates = coordinates, interpolated_wind_vectors = interpolated_wind_vectors)

def IrelandGrid(wind_vectors,coordinates):
    ireland_lat_min = 51.296276
    ireland_lat_max = 55.413426
    ireland_lon_min = -10.684204
    ireland_lon_max = -5.361328
    cellSize = 0.1
    num_rows = math.floor((ireland_lat_max - ireland_lat_min)/ cellSize)
    print(num_rows)
    num_cols = math.floor((ireland_lon_max - ireland_lon_min)/ cellSize)
    print(num_cols)

    # Define grid cells and their locations
    grid_cells = []  # List of grid cell coordinates

    # Iterate over rows and columns to generate grid cell coordinates
    for i in range(num_rows):
        for j in range(num_cols):
            # Calculate the latitude and longitude for the center of the grid cell
            lat_center = ireland_lat_min + (i + 0.5) * cellSize
            lon_center = ireland_lon_min + (j + 0.5) * cellSize
            # Append the coordinates to the grid_cells list as a tuple
            grid_cells.append((lat_center, lon_center))



    # Define IDW parameters
    power_parameter = 2  # Power parameter for weighting

    # Initialize arrays to store interpolated wind vectors
    interpolated_u = np.zeros(len(grid_cells))
    interpolated_v = np.zeros(len(grid_cells))
    coordinates = np.array(coordinates)

    # Iterate through each grid cell
    for i, (grid_x, grid_y) in enumerate(grid_cells):
        total_weight = 0
        weighted_sum_u = 0
        weighted_sum_v = 0
        
        # Calculate distances to sampled locations
        distances = np.sqrt((grid_x - coordinates[:, 0])**2 + (grid_y - coordinates[:, 1])**2)
        
        # Calculate weights
        weights = 1 / distances**power_parameter
        
        # Iterate through sampled wind vectors
        for j, (sampled_u, sampled_v) in enumerate(wind_vectors):
            # Calculate weighted wind vector components
            weighted_sum_u += sampled_u * weights[j]
            weighted_sum_v += sampled_v * weights[j]
            total_weight += weights[j]
        
        # Normalize interpolated wind vector
        if total_weight != 0:
            interpolated_u[i] = weighted_sum_u / total_weight
            interpolated_v[i] = weighted_sum_v / total_weight

    # Interpolated wind vectors for each grid cell
    interpolated_wind_vectors = list(zip(interpolated_u, interpolated_v))
    return interpolated_wind_vectors

if __name__ == '__main__':
        
    if not scheduler.running:
        scheduler.start()
        
    app.run(debug=True)