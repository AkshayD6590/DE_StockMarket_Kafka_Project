import boto3
import json

def lambda_handler(event, context):
    try:
         # Extract relevant stock market data from DynamoDB event
        stock_data = event["Records"][0]["dynamodb"]["NewImage"]
    
        # Extract the table name that triggered the Lambda function
        table_name = event["Records"][0]["eventSourceARN"].split(":")[5].split("/")[1]

        # Map DynamoDB table names to corresponding SNS topics
        sns_topic_mapping = {
            "AD_Stock_Financial": "arn:aws:sns:ap-south-1:179621506842:StockMarketTopic_Financial",
            "AD_Stock_FMCG": "arn:aws:sns:ap-south-1:179621506842:StockMarketTopic_FMCG",
            "AD_Stock_Healthcare": "arn:aws:sns:ap-south-1:179621506842:StockMarketTopic_Healthcare",
            "AD_Stock_OilGas": "arn:aws:sns:ap-south-1:179621506842:StockMarketTopic_OilGas"
        }

        # Get the corresponding SNS topic ARN
        sns_topic_arn = sns_topic_mapping.get(table_name)

        if sns_topic_arn is not None:
            sns_client = boto3.client("sns")

            # Handling DynamoDB data types
            #desired_fields = ["symbol", "identifier", "lastPrice", "dayLow", "dayHigh"]
            #filtered_data = {field: get_field_value(stock_data, field) for field in desired_fields}

            # Include all the respective data about the stock market in the message
            
            filter_data = json.dumps(stock_data, indent=2)
            filter_data_dict = json.loads(filter_data)
            details = {
                        "identifier": filter_data_dict['data']['M']['identifier']['S'],
                        "symbol": filter_data_dict['data']['M']['symbol']['S'],
                        "open": filter_data_dict['data']['M']['open']['N'],
                        "lastPrice": filter_data_dict['data']['M']['lastPrice']['N'],
                        "dayHigh": filter_data_dict['data']['M']['dayHigh']['N'],
                        "dayLow": filter_data_dict['data']['M']['dayLow']['N']
                        }
            formatted_message = "\n".join([f"{key} : {value}" for key, value in details.items()])
            
            sns_client.publish(
                TopicArn=sns_topic_arn,
                Message=formatted_message,
                Subject="Stock Market Update"
            )
        else:
            print(f"Table '{table_name}' not mapped to any SNS topic.")
    except Exception as e:
        print(f"Error: {e}")

def get_field_value(data, field):
    # Helper function to get the field value from DynamoDB data
    return data.get(field, {}).get(list(data[field])[0], "")
