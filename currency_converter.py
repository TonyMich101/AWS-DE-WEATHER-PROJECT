import json
from datetime import datetime
import requests  
import boto3
from decimal import Decimal
import os  # For reading environment variables

# Initialize DynamoDB resource
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table("weather")

def get_weather_data(city):  
    api_url = "http://api.weatherapi.com/v1/current.json"
    api_key = os.getenv("WEATHER_API_KEY")  # Fetch the API key from an environment variable
    
    params = {  
        "q": city,    
        "key": "bb9a253f0f2649c490e173137242010"
    }  
    try:
        response = requests.get(api_url, params=params)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {city}: {e}")
        return None  # Return None if the request fails

def lambda_handler(event, context):

    cities = ["Springs", "Johannesburg", "Boksburg", "Pretoria", "Durban", "Stellenbosch", "Bloemfontein", "Polokwane", "Benoni", "Umhlanga"]
    
    for city in cities:
        data = get_weather_data(city)
        
        if data is None:
            continue  # Skip if data couldn't be fetched
        
        # Check for 'current' key in the response to avoid KeyError
        if 'current' not in data:
            print(f"No 'current' data available for {city}. Response: {json.dumps(data, indent=2)}")
            continue  # Skip this city if 'current' key is missing
        
        try:
            # Extract required fields
            temp = data['current']['temp_c']
            wind_speed = data['current']['wind_mph']
            wind_dir = data['current']['wind_dir']
            pressure_mb = data['current']['pressure_mb']
            humidity = data['current']['humidity']
            
            print(city, temp, wind_speed, wind_dir, pressure_mb, humidity)
            current_timestamp = datetime.utcnow().isoformat()
            
            # Create the item to insert into DynamoDB
            item = {
                'city': city,
                'time': str(current_timestamp),
                'temp': Decimal(str(temp)),
                'wind_speed': Decimal(str(wind_speed)),
                'wind_dir': wind_dir,
                'pressure_mb': Decimal(str(pressure_mb)),
                'humidity': Decimal(str(humidity))
            }
            
            # Insert the item into DynamoDB
            table.put_item(Item=item)
        
        except KeyError as e:
            print(f"Missing data in API response for {city}: {e}")
            continue  # Skip this city if required data is missing

 