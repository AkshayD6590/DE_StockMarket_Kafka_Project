import boto3
from kafka import KafkaConsumer

dynamodb = boto3.resource('dynamodb')
table_name_prefix = 'AD_Stock'

def lambda_handler(event, context):
    try:        
        kafka_topics = ['Financial', 'OilGas', 'Healthcare', 'FMCG']

        for topic in kafka_topics:
            
            table_name = f'{table_name_prefix}_{topic}Table'

            if not table_exists(table_name):
                create_dynamodb_table(table_name)

            consumer = KafkaConsumer(topic, bootstrap_servers=['b-3.mskclusterstockmarket.0c3osp.c2.kafka.ap-south-1.amazonaws.com:9098,b-1.mskclusterstockmarket.0c3osp.c2.kafka.ap-south-1.amazonaws.com:9098,b-2.mskclusterstockmarket.0c3osp.c2.kafka.ap-south-1.amazonaws.com:9098'])
            
            for message in consumer:
                data = process_kafka_message(message.value)
                store_data_in_dynamodb(data, table_name)

    except Exception as e:
        print(f"Error in Data Consumer Lambda: {str(e)}")

def table_exists(table_name):
    try:
        dynamodb.Table(table_name).table_status
        return True
    except dynamodb.meta.client.exceptions.ResourceNotFoundException:
        return False

def create_dynamodb_table(table_name):
    dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {'AttributeName': 'YourPrimaryKey', 'KeyType': 'HASH'},
        ],
        AttributeDefinitions=[
            {'AttributeName': 'YourPrimaryKey', 'AttributeType': 'S'},
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5, 
            'WriteCapacityUnits': 5
        }
    )

def process_kafka_message(message):
    # logic to extract and process data from Kafka message and Return the processed data
    pass

def store_data_in_dynamodb(data, table_name):
    try:
        table = dynamodb.Table(table_name)
        table.put_item(Item=data)

    except Exception as e:
        print(f"Error storing data in DynamoDB for table {table_name}: {str(e)}")
