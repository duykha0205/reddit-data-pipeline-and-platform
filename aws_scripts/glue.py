
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import concat_ws

# Get job parameters - now including source_path and target_path from Lambda
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'source_path', 'target_path'])

# Initialize contexts and job
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read from source S3 path (passed by Lambda)
AmazonS3_source = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": "\"",
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [args['source_path']],
        "recurse": True
    },
    transformation_ctx="AmazonS3_source"
)

# Convert to DataFrame and apply transformations
df = AmazonS3_source.toDF()

# Your existing transformation logic
df_combined = df.withColumn('ESS_updated', concat_ws('-', df['edited'], df['spoiler'], df['stickied']))
df_combined = df_combined.drop('edited', 'spoiler', 'stickied')

# Convert back to DynamicFrame
S3bucket_node_combined = DynamicFrame.fromDF(df_combined, glueContext, 'S3bucket_node_combined')

# Write to target S3 path (passed by Lambda)
AmazonS3_target = glueContext.write_dynamic_frame.from_options(
    frame=S3bucket_node_combined,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": args['target_path'],
        "partitionKeys": []
    },
    transformation_ctx="AmazonS3_target"
)

job.commit()