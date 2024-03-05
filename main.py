from flask import Flask, jsonify, Response
from flask import request as flask_request
from flask import render_template
import pandas as pd
import geopandas as gpd
from dataclasses import dataclass
from datetime import datetime, timedelta
import requests
import xml.etree.ElementTree as ET
from flask_apscheduler import APScheduler
import os
from dotenv import load_dotenv, find_dotenv
from pymongo import MongoClient
from bson import json_util
import json
import math
import numpy as np
import math


load_dotenv(find_dotenv())
password = os.environ.get("MONGODB_PWD")

connection_string = f"mongodb+srv://twright:{password}@windfarm.seyuy3o.mongodb.net/?retryWrites=true&w=majority"

cluster = MongoClient(connection_string)

db = cluster['Test']
collection = db['Test']

# collection.delete_many({})




            
# set configuration values for APscheduler
class Config:
    SCHEDULER_API_ENABLED = True

# Initialize app
app = Flask(__name__)
app.config.from_object(Config())

# initialize scheduler
scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()

basedir = os.path.abspath(os.path.dirname(__file__))

def insert_many_windspeeds(data):
    collection = db['Test']
    collection.insert_many(data)
    print(f"Windspeed data inserted into database at {datetime.now()}")

# cron examples
@scheduler.task('cron', id='scrape_wind_data', hour = "*", minute="08")
def gather_wind_data():
    with scheduler.app.app_context():
        print(f"Background Task started at {datetime.now()}")
        windfarm_df = pd.read_csv("Windfarm_WebScraped_DataV4.csv")
        now = datetime.now().replace(microsecond=0, second=0, minute=0)
        current_time = now.strftime('%Y-%m-%dT%H:%M')
        timestamp = datetime.today().replace(microsecond=0, second=0, minute=0)
        #Begin a session which keeps the same connection open for all api calls
        with requests.Session() as session:
            # Create a list of the wind speed results results using list comprehension 
            windspeeds = [gather_session_urls(session = session,name = row['Wind Farm Name'],lon = row['Longitude'],lat = row['Latitude'],rotordiameter = row['Rotor diameter'], numberofturbines = row['Number of Turbines'] , cutinspeed=row['Cut-in wind speed'], cutoutspeed=row['Cut-off wind speed'], ratedspeed=row['Rated wind speed'], current_time = current_time,timestamp = timestamp, county = row['COUNTY']) for index, row in windfarm_df.iterrows()]
            for windspeed in windspeeds:
                print(windspeed)
            insert_many_windspeeds(windspeeds)

def windpower(windspeed: float,rotordiameter: float, cutinspeed: float, cutoutspeed: float, ratedspeed:float):
     # Initiate constants:
    air_density = 1.225  # Standard air density at sea level in kg/m^3
    power_coefficient = 0.3 # Ideal power coefficient (Betz limit) is 0.593 but 0.3 is more typical

    #Swept area
    swept_area = math.pi * (rotordiameter/2)**2
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




def gather_session_urls(session,name,lon,lat, rotordiameter, numberofturbines,cutinspeed, cutoutspeed, ratedspeed, current_time,timestamp,county):
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
                    if math.isnan(windfarmpower):
                        windfarmpower = 0
                    else:
                        windfarmpower = windfarmpower
                else:
                    print("Windspeed value was empty")
                    windfarmpower = 0
                data = {"metadata":{"Wind Farm Name":name,
                                    "Latitude":lat,
                                    "Longitude":lon},
                        "timestamp": timestamp,
                        "windspeed":windspeed,
                        "windpower":windfarmpower,
                        "winddirection": winddirection,
                        "County":county}
                print(f"Background Task Executed at {datetime.now()}")
                return data
            else:
                print("Wind Speed element not found in the XML.")
                return None
        except Exception as e:
            print(f"Error processing wind data for lat={lat}, lon={lon}: {e}")
            return None


#Read Counties data into a geopandas dataframe
geojson_path = 'Counties_-_National_Statutory_Boundaries_-_2019_-_Generalised_20m.geojson'
with open(geojson_path, 'r', encoding='utf-8') as file:
            test_data = json.load(file)
counties_boundaries = gpd.read_file(geojson_path)
counties_boundaries= counties_boundaries[['COUNTY','geometry']]
county_geometry = [dict(_id = feature['properties']['COUNTY'], geometry = feature['geometry']) for feature in test_data['features']]


@app.route('/')
def index():
    #Read Windfarm data into a pandas dataframe
    windfarm_data = pd.read_csv("Windfarm_WebScraped_Datav4.csv")
    print(windfarm_data.to_json(orient="records"))
    return render_template('index.html',windfarm_data = windfarm_data.to_json(orient="records"))

@app.route("/windfarm_details", methods = ['POST'])
def windfarm_details():
    #Read Windfarm data into a pandas dataframe
    windfarm_data = pd.read_csv("Windfarm_WebScraped_Datav4.csv")
    if flask_request.method == 'POST':
        windfarm_name = flask_request.json['Windfarm']
        windfarm_details= windfarm_data.loc[windfarm_data['Wind Farm Name']== windfarm_name ]
        # Convert DataFrame to JSON string
        json_data = windfarm_details.to_json(orient='records')
    # Return JSON response
    return json.loads(json_data)

@app.route("/choropleth")
def choropleth():
    geojson_path = 'Coast_-_National_250k_Map_Of_Ireland.geojson'
    print(geojson_path)
    try:
        with open(geojson_path, 'r', encoding='utf-8') as file:
            geojson_data = file.read()
            return Response(geojson_data, content_type='application/json')
    except FileNotFoundError:
        return jsonify({"error": "GeoJSON file not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/NIchoropleth")
def NIchoropleth():
    geojson_path = 'northern-ireland_1319.geojson'
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
    # Calculate the timestamp for the last hour
    current_time = datetime.utcnow()
    one_hour_ago = current_time - timedelta(hours=1)
    # Define the aggregation pipeline
    pipeline = [
    {
        '$sort': {
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
        '$group': {
            '_id': '$lastEntry.County', 
            'totalWindPower': {
                '$sum': '$lastEntry.windpower'
            }
        }
    }
]
    
    aggregate_windpower = collection.aggregate(pipeline)


    # Use json_util.dumps to serialize each MongoDB document in the list
    county_windpower = [json.loads(json_util.dumps(doc)) for doc in aggregate_windpower]
    print(county_windpower)
    # End time measurement
    print(f"After json_util.dumps to serialize each MongoDB document:{datetime.utcnow()}")
    #Convert list to Pandas DataSeries
    series_county_windpower = pd.DataFrame(county_windpower)
    #Rename _id column to COUNTY
    series_county_windpower.rename(columns={"_id":"COUNTY"}, inplace = True)
    print(series_county_windpower)
    
    # # Merge based on the 'county' column
    merged_df = pd.merge(counties_boundaries, series_county_windpower, on='COUNTY')
    
    #Convert DataFrame to geojson string
    
    geojson_str = merged_df.to_json()
    #End time measurement
 
    # After all dataframe manipulation
    print(f"After all dataframe manipulation:{datetime.utcnow()}")
    # Iterate through wind power data
    # Merged list with combined data

    print(f"Start time:{datetime.utcnow()}")
    # Create a dictionary for efficient lookups by _id
    geometry_lookup = {d['_id']: d['geometry'] for d in county_geometry}

    # Merge data using list comprehension
    merged_data = [{**wind_power, 'geometry': geometry_lookup.get(wind_power['_id'])} for wind_power in county_windpower]
    geo_merged_data = {"type": "FeatureCollection","features": merged_data}
    print(f"End time:{datetime.utcnow()}")
    # Print the merged data

    return geojson_str

@app.route("/wind")
def wind():
    return render_template('wind.html')

@app.route("/windvectors")
def windvectors():
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
    interpolated_windvectors = IrelandGrid(wind_vectors,coordinates)
    #return render_template('wind.html',coordinates = coordinates, interpolated_wind_vectors = interpolated_wind_vectors)

    return interpolated_windvectors

def IrelandGrid(wind_vectors,coordinates):
    ireland_lat_min = 50.296276
    ireland_lat_max = 56.413426
    ireland_lon_min = -11.684204
    ireland_lon_max = -4.361328
    cellSize = 0.1 # degree step
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

    interpolated_u = interpolated_u.tolist()
    interpolated_v = interpolated_v.tolist()
    wind_vectors = [{'header': {'parameterCategory': 2, 
                         'parameterNumber': 2, 
                         'lo1': ireland_lon_min, 
                         'la1': ireland_lat_max, 
                         'dx': cellSize, 
                         'dy': cellSize, 
                         'nx': num_cols, 
                         'ny': num_rows, 
                         'refTime': '2021-11-09T19:00:00Z'},
             'data': interpolated_u},
             {'header': {'parameterCategory': 2, 
                         'parameterNumber': 3, 
                         'lo1': ireland_lon_min, 
                         'la1': ireland_lat_max,  
                         'dx': cellSize, 
                         'dy': cellSize,
                         'nx': num_cols, 
                         'ny': num_rows, 
                         'refTime': '2021-11-09T19:00:00Z'},
              'data': interpolated_v}]
    # Interpolated wind vectors for each grid cell
    #windvectors ={"u": interpolated_u, "v": interpolated_v}
    json.dumps(wind_vectors)
    return wind_vectors

if __name__ == '__main__':
    app.run(debug=True)