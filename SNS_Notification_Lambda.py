import boto3
import json

def lambda_handler(event, context):
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
        
        # Include all the respective data about the stock market in the message
        message = json.dumps(stock_data, indent=2)
        
        sns_client.publish(
            TopicArn=sns_topic_arn,
            Message=message,
            Subject="Stock Market Update"
        )
    else:
        print(f"Table '{table_name}' not mapped to any SNS topic.")
