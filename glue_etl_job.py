import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 1. Load Cleaned Category Data (Reference)
predicate_pushdown = "region in ('ca','gb','us')"
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "db_youtube_cleaned", table_name = "cleaned_statistics_reference_data", transformation_ctx = "datasource0", push_down_predicate = predicate_pushdown)

# 2. Load Raw Video Statistics
datasource1 = glueContext.create_dynamic_frame.from_catalog(database = "db_youtube_raw", table_name = "raw_statistics", transformation_ctx = "datasource1")

# 3. Join the datasets
applymapping1 = ApplyMapping.apply(frame = datasource1, mappings = [("video_id", "string", "video_id", "string"), ("trending_date", "string", "trending_date", "string"), ("title", "string", "title", "string"), ("channel_title", "string", "channel_title", "string"), ("category_id", "long", "category_id", "long"), ("publish_time", "string", "publish_time", "string"), ("tags", "string", "tags", "string"), ("views", "long", "views", "long"), ("likes", "long", "likes", "long"), ("dislikes", "long", "dislikes", "long"), ("comment_count", "long", "comment_count", "long"), ("thumbnail_link", "string", "thumbnail_link", "string"), ("comments_disabled", "boolean", "comments_disabled", "boolean"), ("ratings_disabled", "boolean", "ratings_disabled", "boolean"), ("video_error_or_removed", "boolean", "video_error_or_removed", "boolean"), ("region", "string", "region", "string")], transformation_ctx = "applymapping1")

resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_struct", transformation_ctx = "resolvechoice2")

dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")

# Join on category_id
join4 = Join.apply(datasource0, dropnullfields3, 'id', 'category_id', transformation_ctx = "join4")

# 4. Write to S3 Analytics Layer in Parquet
datasink5 = glueContext.write_dynamic_frame.from_options(frame = join4, connection_type = "s3", connection_options = {"path": "s3://youtube-analytics-layer-project", "partitionKeys": ["region", "category_id"]}, format = "parquet", transformation_ctx = "datasink5")

job.commit()
