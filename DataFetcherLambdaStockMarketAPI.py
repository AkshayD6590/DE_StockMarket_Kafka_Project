import requests
import json
from confluent_kafka import Producer


def fetch_stock_data():
    url = "https://latest-stock-price.p.rapidapi.com/price"

    querystring = {"Indices": "NIFTY 50"}

    headers = {
        "X-RapidAPI-Key": "6aaf24f081msh54447b87c027433p1eff79jsna78813754cb4",
        "X-RapidAPI-Host": "latest-stock-price.p.rapidapi.com"
    }

    try:
        response = requests.get(url, headers=headers, params=querystring)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            data = response.json()  # Assuming the response is in JSON format
            print("API request successful")

            return data
        else:
            print(f"API request failed with status code {response.status_code}")
            print("Response:", response.text),
            return("Response:", response.text)

    except Exception as e:
        print("An error occurred:", str(e))

def publish_to_kafka(topic, message):
    print('Inside Publish to kafka function', topic, message)  # for test
    kafka_brokers = '127.0.0.1:9092'

    producer_conf = {'bootstrap.servers': kafka_brokers}
    
    producer = Producer(producer_conf)

    try:
        # Produce a sample message to the specified topic
        producer.produce(topic, value=message)
        # Wait for any outstanding messages to be delivered and delivery reports received
        producer.flush()
        print(f"Sample message '{message}' sent to topic '{topic}' successfully.")
    except Exception as e:
        print(f"Error: {e}")

stock_data = fetch_stock_data()
print('The data is :', stock_data)  #For Testing
    
for record in stock_data:

    identifier = record['identifier']

    if identifier == 'AXISBANKEQN' or identifier == 'HDFCBANKEQN':
        publish_to_kafka('Financial', json.dumps(record))
    elif identifier == 'BPCLEQN' or identifier == 'ONGCEQN':
        publish_to_kafka('OilGas', json.dumps(record))
    elif identifier == 'ITCEQN' or identifier == 'BRITANNIAEQN':
        publish_to_kafka('Healthcare', json.dumps(record))
    elif identifier == 'ITCEQN' or identifier == 'BRITANNIAEQN':
        publish_to_kafka('FMCG', json.dumps(record))
    else:
        publish_to_kafka('Other',json.dumps(record))
#test_var=lambda_handler()
#print(test_var)    
