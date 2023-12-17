import http.client
import json
from confluent_kafka import Producer

def lambda_handler(event, context):
    stock_data = fetch_stock_data()
    print('The data is :', stock_data)
    # Categorize and publish data to Kafka topics
    for record in stock_data:
        if record['identifier'] in ['AXISBANKEQN', 'HDFCBANKEQN']:
            publish_to_kafka('Financial', record['stock_data'])
        elif record['identifier'] in ['BPCLEQN', 'ONGCEQN']:
            publish_to_kafka('OilGas', record['stock_data'])
        elif record['identifier'] in ['ITCEQN', 'BRITANNIAEQN']:
            publish_to_kafka('Healthcare', record['stock_data'])
        elif record['identifier'] in ['ITCEQN', 'BRITANNIAEQN']:
            publish_to_kafka('FMCG', record['stock_data'])
        else:
            return 'Other', record['identifier']    

    return {
        'statusCode': 200,
        'body': json.dumps(stock_data)
    }
        
def fetch_stock_data():
    host = "latest-stock-price.p.rapidapi.com"
    path = "/price"
    query = "Indices=NIFTY%2050"

    headers = {
        'X-RapidAPI-Key': '6aaf24f081msh54447b87c027433p1eff79jsna78813754cb4',
        'X-RapidAPI-Host': host
    }

    try:
        connection = http.client.HTTPSConnection(host)
        connection.request("GET", f"{path}?{query}", headers=headers)
        response = connection.getresponse()

        if response.status == 200:
            data = response.read()
            return json.loads(data)
        else:
            return {
                'error': f"Unexpected status code: {response.status}"
            }

    except Exception as e:
        return {
            'error': f"Error fetching stock data: {e}"
        }

    finally:
        connection.close()

def publish_to_kafka(topic, message):
    kafka_brokers = 'localhost:9092'
    producer_config = {
        'bootstrap.servers': kafka_brokers,
    }
    producer = Producer(producer_config)
    producer.produce(topic, value=message)
    producer.flush()