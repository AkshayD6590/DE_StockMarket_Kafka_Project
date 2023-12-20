from confluent_kafka import Consumer, KafkaError
import boto3
import json
from datetime import datetime, timedelta
import time
from botocore.exceptions import ClientError
from decimal import Decimal

# Kafka configuration
kafka_config = {
    'bootstrap.servers': '127.0.0.1:9092',
    'group.id': 'data_consumer_group',
    'auto.offset.reset': 'earliest'
}

dynamodb = boto3.resource('dynamodb', region_name='ap-south-1')  # e.g., 'us-east-1'

def create_dynamodb_table(table_name):
    prefix = 'AD_Stock_'
    full_table_name = prefix + table_name

    # Check if the table already exists
    try:
        dynamodb.Table(full_table_name).load()
    except dynamodb.meta.client.exceptions.ResourceNotFoundException:
        # Table does not exist, create it
        table = dynamodb.create_table(
            TableName=full_table_name,
            KeySchema=[
                {'AttributeName': 'key', 'KeyType': 'HASH'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'key', 'AttributeType': 'S'}
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        table.meta.client.get_waiter('table_exists').wait(TableName=full_table_name)

def process_message(message, table):
    try:
        print('LN 44 : Inside Process_message function :',message)   ##For test delete later
        identifier = message.get('identifier')

        print('LN 47: Inside Process_message function Key is:',identifier, 'AND TABLE Name is :',table)   ##For test delete later
        if identifier is None:
            raise KeyError("'key' field is missing in the message")

        data = convert_floats_to_decimal(message)
        print('LN:52 Inside Process_message function data is :',data)   ##For test delete later
        if data is None:
            raise KeyError("'data' field is missing in the message")

        print('LN 56 : Reacehd Put')
        table.put_item(Item={'key':identifier, 'data': data})
    except KeyError as e:
        print(f"Error processing message: {e}")
        return
#To conver float value to decimal
def convert_floats_to_decimal(data):
    if isinstance(data, dict):
        return {k: convert_floats_to_decimal(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_floats_to_decimal(item) for item in data]
    elif isinstance(data, float):
        return Decimal(str(data))
    else:
        return data


def consume_and_store_data():
    consumer = Consumer(kafka_config)

    topics = ['Financial', 'OilGas', 'Healthcare', 'FMCG', 'Other']
    consumer.subscribe(topics)

    for topic in topics:
        # Create DynamoDB table for each topic
        create_dynamodb_table(topic)

    try:
        while True:
            msg = consumer.poll(1000)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break

            # Process the message and put it into DynamoDB
            topic = msg.topic()
            full_table_name = 'AD_Stock_' + topic
            table = dynamodb.Table(full_table_name)
            
            # Check if the table exists, and create it if not
            try:
                table.load()
            except dynamodb.meta.client.exceptions.ResourceNotFoundException:
                create_dynamodb_table(topic)

            message_data = json.loads(msg.value().decode('utf-8'))
            ##print(message_data) ##For test delete later
            process_message(message_data, table)

            # Sleep for 1 minute
            time.sleep(60)

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_and_store_data()
