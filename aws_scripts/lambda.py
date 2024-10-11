import json
import boto3

# def lambda_handler(event, context):
#     print(event)
#     # TODO implement
#     return {
#         'statusCode': 200,
#         'body': json.dumps('Hello from Lambda!')
#     }

def lambda_handler(event, context):
    # Get the S3 bucket and file details from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']
    
    # Initialize Glue client
    glue_client = boto3.client('glue')
    
    try:
        # Start Glue job
        response = glue_client.start_job_run(
            JobName='redi_glue_job',
            Arguments={
                '--source_path': f's3://{bucket}/{file_key}',
                '--target_path': f's3://{bucket}/transformed/'
            }
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps(f'Started Glue job with run ID: {response["JobRunId"]}')
        }
        
    except Exception as e:
        print(f'Error: {str(e)}')
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error starting Glue job: {str(e)}')
        }