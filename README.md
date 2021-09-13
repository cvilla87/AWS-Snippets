# AWS Snippets

1. [AWS CLI](#awscli)
    1. [General](#general)
    2. [S3](#s3)
2. [S3 - Python SDK](#s3-boto3)
    1. [Boto3 - Resource](#s3-resource)
    2. [Boto3 - Client](#s3-client)
    3. [Other snippets](#s3-snippets)
3. [Lambda functions](#lambda)
4. [Redshift](#redshift)
    1. [Metadata Tables](#metadata-tables)
    2. [Snippets](#redshift-snippets)
5. [Glue Spark](#glue-spark)
6. [Glue Data Catalog](#glue-catalog)


## AWS CLI <a name="awscli"></a>

### General <a name="general"></a>

| Action                                        | Code                                                          |
| --------------------------------------------- | ------------------------------------------------------------- |
| Check AWS CLI version                         | `aws --version`                                               |
| Get current AWS credentials                   | `aws configure list`                                          |
| Get IAM user information                      | `aws iam get-user --user-name user`                           |
| Details about the IAM user or role being used | `aws sts get-caller-identity`                                 |
| Get details about AWS Secret id               | `aws secretsmanager get-secret-value --secret-id <secret_id>` |
| List profiles (only with AWS CLI v2)          | `aws configure list-profiles`                                 |
| Enable debugging                              | `aws s3 ls --debug`                                           |
| Actions for specific profile                  | `aws s3 ls --profile prod`                                    |


### S3 <a name="s3"></a>

| Action                                   | Code                                                                                                                     |
| ---------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| List files from S3                       | `aws s3 ls s3://mybucket/path/to/folder/`                                                                                |
| List files recursively from S3           | `aws s3 ls s3://mybucket/path/to/folder/ --recursive`                                                                    |
| Copy S3 object (folder or file)          | `aws s3 cp s3://mybucket/path/to/file1 s3://mybucket/path/to/file2`                                                      |
| Delete recursively S3 objects            | `aws s3 rm --recursive s3://mybucket/path/to/folder/ --exclude "*2020-08*"`                                              |
| Delete recursively only some S3 objects  | `aws s3 rm --recursive s3://mybucket/path/to/folder/ --exclude "*" --include "*2021-05-14*"`                             |
| Copy recursively S3 objects              | `aws s3 cp s3://mybucket/path/to/folder1/ s3://mybucket/path/to/folder2/ --recursive`                                    |
| Copy S3 objects with include and exclude | `aws s3 cp s3://mybucket/logs/ s3://mybucket2/logs/ --recursive --exclude "*" --include "*.log"`                         |
| Rename folder / Move recursively         | `aws s3 mv --recursive s3://mybucket/path/to/wrong_folder/ s3://mybucket/path/to/correct_folder/`                        |
| Sync S3 folders                          | `aws s3 sync s3://mybucket/path/to/folder1/ s3://mybucket/path/to/folder2/`                                              |
| Sync S3 folders with dry run             | `aws s3 sync s3://mybucket/path/to/folder1/ s3://mybucket/path/to/folder2/ --dryrun`                                     |
| Sync S3 folders excluding extension      | `aws s3 sync s3://mybucket/path/to/folder1/ s3://mybucket/path/to/folder2/ --exclude "*.txt"`                            |
| Sync S3 folders excluding folder         | `aws s3 sync ./folder/ s3://mybucket/path/to/folder/ --exclude "*folder1/*"`                                             |
| Sync S3 folders with ACL                 | `aws s3 sync s3://mybucket/path/to/folder1/ s3://mybucket/path/to/folder2/ --acl bucket-owner-full-control --sse AES256` |

Read partially a file:  
`aws s3 cp s3://mybucket/path/to/file.txt - | head`

Get disk space used for S3 path:  
`aws s3 ls --summarize --human-readable --recursive s3://mybucket/path/to/folder/ | tail -2`  

Both `cp` and `sync` commands can be used in the following combinations:
- `<LocalPath> <S3Uri>`
- `<S3Uri> <LocalPath>`
- `<S3Uri> <S3Uri>`

`--dryrun` option can be added to simulate instead of actually executing the action.



## S3 - Python SDK (Boto3) <a name="s3-boto3"></a>

### Boto3 - Resource <a name="s3-resource"></a>
Resources represent an object-oriented interface to Amazon Web Services (AWS).  
They provide a higher-level abstraction than the raw, low-level calls made by service clients.  

To use resources, you invoke the `resource()` method of a `Session` and pass in a service name:  
`s3 = boto3.resource('s3')`  


| Action                   | Code                                                                                                                                   |
| ------------------------ | -------------------------------------------------------------------------------------------------------------------------------------- |
| List buckets             | `for bucket in s3.buckets.all():`<br>`if 'abc' in bucket.name:`<br>    `print(bucket.name)`                                            |
| Define S3 bucket         | `bucket = s3.Bucket('bucket_name')`                                                                                                    |
| List bucket objects      | `for obj in bucket.objects.all():`<br>    `print(obj.key)`                                                                             |
| Wait for bucket creation | `bucket.wait_until_exists()`                                                                                                           |
| Define S3 object         | `obj = s3.Object('bucket_name', 'path/to/file.txt')`                                                                                   |
| S3 object attributes     | `obj.key`, `obj.last_modified`, `obj.content_length`, `obj.e_tag`, ...                                                                           |
| Download file            | `obj.download_file('population.xlsx')`                                                                                                 |
| Delete file              | `obj.delete()`                                                                                                                         |
| Read a file              | `response = obj.get()`<br>`data = response['Body'].read()`                                                                             |
| Partially read a file    | `obj.get(Range='bytes=0-100')`                                                                                                         |
| Copy a file              | `src = {'Bucket': bucket_name, 'Key': 'local_path/to/file.txt'}`<br>`s3.meta.client.copy(src, bucket_name, 'remote_path/to/file.txt')` |
| Copy a file              | `src = {'Bucket': bucket_name, 'Key': 'local_path/to/file.txt'}`<br>`bucket.copy(src, 'remote_path/to/file.txt')`                      |


### Boto3 - Client <a name="s3-client"></a>
Clients provide a low-level interface to AWS whose methods map close to 1:1 with service APIs.  
All service operations are supported by clients.  

They are created in a similar fashion to resources: `s3 = boto3.client('s3')`  


| Action              | Code                                                                                                                                   |
| ------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| List buckets        | `resp = s3.list_buckets()`<br>`for bucket in resp['Buckets']:`<br>    `print(f'{bucket["Name"]}')`                                     |
| List bucket objects | `resp = s3.list_objects_v2(Bucket=bucket_name, Prefix='path/to/folder')`<br>`for obj in resp['Contents']:`<br>    `files = obj['Key']` |
| Get bucket ACL      | `s3.get_bucket_acl('bucket_name')`                                                                                                     |
| Download file       | `s3.download_file('bucket_name', 'remote_path/to/file.txt', 'local_path/to/file.txt')`                                                 |
| Download file       | `with open('local_path/to/file.txt', "wb") as f:`<br>    `s3.download_fileobj('bucket_name', 'remote_path/to/file.txt', f)`            |
| Upload file         | `s3.upload_file('local_path/to/file.txt', 'bucket_name', 'remote_path/to/file.txt')`                                                   |
| Upload file         | `with open('local_path/to/file.txt', "rb") as f:`<br>    `s3.upload_fileobj(f, 'bucket_name', 'remote_path/to/file.txt')`              |
| Delete file         | `s3.delete_object('bucket_name', 'remote_path/to/file.txt')`                                                                           |


### Other snippets <a name="s3-snippets"></a>
**Read S3 objects**  
```python
import s3fs

s3_fs = s3fs.S3FileSystem()
with s3_fs.open('my-bucket/my-file.txt') as f:
    data = f.read()
```

Read the number of lines of multiple objects:  
```python
s3_resource = boto3.resource('s3')
bucket = s3_resource.Bucket('bucket_name')

for s3_obj in bucket.objects.filter(Prefix='path/to/s3/folder'):
    if 0 < s3_obj.size < 10000000:  # Between 0 and 10 MB
        print('Reading filename:', s3_obj.key.split('/')[-1])
        file = s3_obj.get()["Body"].read()
        print('Number of lines:', str(file.decode('utf8')).count('\n'))
```


**Delete old objects**  
```python
import datetime
import boto3

s3_resource = boto3.resource('s3')

bucket = s3_resource.Bucket('bucket_name')
path = 'path/to/folder/'
number_days = 365
current_date = datetime.date.today()

for obj in bucket.objects.filter(Prefix=path):
    if obj.last_modified.date() < current_date - datetime.timedelta(days=number_days):
        obj.delete()
```


**Get list of filtered objects**
```python
list_s3 = []
for s3_obj in bucket.objects.filter(Prefix=s3_input_path):
    # We filter out the not valid files (normally files with $ are lock or other type of files with no actual data)
    if '$' not in s3_obj.key:
        list_s3.append(s3_obj.key)
```


**Upload file**
```python
s3_resource.meta.client.upload_file('file.txt', s3_output_bucket, s3_output_path)
```



## Lambda functions <a name="lambda"></a>
In case of needing to deploy a Lambda function with some extra packages:  
https://docs.aws.amazon.com/lambda/latest/dg/python-package.html


**[S3 image resizer](lambda-s3-image-resize.py)**  
Reads and image from a S3 bucket, resizes the image and then stores the new image in a different S3 bucket.  


**[Notification to webhook](lambda-chat-webhook.py)**    
From AWS documentation: https://aws.amazon.com/premiumsupport/knowledge-center/sns-lambda-webhooks-chime-slack-teams/    

Once the Lambda function is created, a trigger has to be added to start executing the function. For instance, it could be a SNS topic.  
In this case, SNS topic has messages from AWS Glue workflow executions, which the Lambda function checks first for its state,
if it's one of 'STOPPED', 'FAILED', 'TIMEOUT', it will execute the POST request to send the body message to the chat channel.

Webhook can be tested with curl:  
`curl -X POST -H "Content-Type: application/json" -d '{"text" : "This is a message from a Webex incoming webhook."}' "https://api.ciscospark.com/v1/webhooks/incoming/<incoming_webhook_url>"`

More info about Webex webhooks:  
https://developer.webex.com/docs/api/guides/webhooks  
[https://developer.webex.com/blog/using-webhooks](https://developer.webex.com/blog/using-webhooks---rooms-messages-memberships-and-more-)  



## Redshift <a name="redshift"></a>

### Metadata Tables <a name="metadata-tables"></a>

| Table                                          | Query                                                                                                           |
| ---------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| Databases                                      | `select a.oid, a.* from pg_database a;`                                                                         |
| Schemas                                        | `select * from PG_NAMESPACE s where nspname like '%schema%';`                                                   |
| Tables                                         | `select * from PG_TABLES where schemaname = 'schema';`                                                          |
| Internal tables definition                     | `select * from PG_TABLE_DEF a where a.column like '%some%';`                                                    |
| Tables info                                    | `select * from SVV_TABLE_INFO;`                                                                                 |
| External tables (for S3 objects)               | `select * from SVV_EXTERNAL_TABLES;`                                                                            |
| Columns                                        | `select table_schema, table_name, column_name from INFORMATION_SCHEMA.columns order by 1, 2, ordinal_position;` |
| Views                                          | `select * from PG_VIEWS where schemaname = 'schema';`                                                           |
| Users                                          | `select * from PG_USER where usename like '%user%' order by usesysid;`                                          |
| Load errors                                    | `select * from PG_CATALOG.stl_load_errors order by starttime desc;`                                             |
| Queries at the segment and<br>node slice level | `select * from SVL_S3LOG order by eventtime desc;`                                                              |
| Information about executed queries             | `select * from STL_QUERY;`                                                                                      |
| Output of executed queries                     | `select * from STL_QUERYTEXT;`                                                                                  |
| Metrics for completed queries                  | `select * from SVL_QUERY_METRICS;`                                                                              |
| Details for *return* steps in queries          | `select * from STL_RETURN where query = 9999;`                                                                  |
| Redshift load errors                           | `select * from STL_LOAD_ERRORS where query = 9999;`                                                             |
| Redshift load errors details                   | `select * from STL_LOADERROR_DETAIL where query = 9999;`                                                        |
| Spectrum Scan errors                           | `select * from SVL_S3LOG where message like 'Spectrum Scan Error%' order by eventtime desc;`                    |



### Snippets <a name="redshift-snippets"></a>

**Schema per user**  
```sql
select s.nspname as table_schema, s.oid as schema_id, u.usename as owner
from pg_catalog.pg_namespace s
join pg_catalog.pg_user u on u.usesysid = s.nspowner
    where owner = 'glue_user'
--  where table_schema like '%schema_name%'
order by table_schema;
```


**Casting**  
The following statements are equivalent:
```sql
select cast(pricepaid as integer) from sales;
select pricepaid::integer from sales;
select convert(integer, pricepaid) from sales;
```


**Execute statements from command line**  
We need to install `psql`, a terminal-based front end from PostgreSQL.  

Then we can run just one statement using:  
`psql "host=<host> user=<user> dbname=<dbname> port=5439 password=<pass>" -c "SELECT * FROM schema.table1 where field ='<name>';"`

Or execute more than one statement in one block using the following format:
```sh
psql "host=<host> user=<user> dbname=<dbname> port=5439 password=<pass>" <<EOF
SELECT * FROM schema.table1 limit 5;
SELECT * FROM schema.table2 limit 5;
EOF
```


**Update statistics metadata**  
This action will enable the query optimizer to generate more accurate query plans.
```sql
analyze;  -- analyzes the whole database
analyze tbl;
```


**Unload data to S3**  
The following command unload data from Redshift to S3:
```sql
UNLOAD ('select * from schema_name.table_name')
TO 's3://<bucket>/path/to/table/'
IAM_ROLE '<iam_role>'
FORMAT AS PARQUET
-- CSV DELIMITER AS '|'
PARTITION BY (date_field) INCLUDE
manifest verbose;
-- maxfilesize 1 gb;
```

`FORMAT` and `AS` are optional.  
With `INCLUDE`, the column indicated in the `PARTITION BY` will also be included in the data in the Parquet files.  

`manifest` option will include an extra file with the manifest, which will include the resulting files. And adding `verbose` to it will include many more details, for example:  
```
{
  "entries": [
    {"url":"s3://bucket/file_0000", "meta": { "content_length": 32295, "record_count": 10 }},
    {"url":"s3://bucket/file_0001", "meta": { "content_length": 32771, "record_count": 20 }},
    {"url":"s3://bucket/file_0002", "meta": { "content_length": 32302, "record_count": 10 }}
  ],
  "schema": {
    "elements": [
      {"name": "venueid", "type": { "base": "integer" }},
      {"name": "venuename", "type": { "base": "character varying", 25 }},
      {"name": "venuecity", "type": { "base": "character varying", 25 }}
    ]
  },
  "meta": {
    "content_length": 129178,
    "record_count": 55
  },
  "author": {
    "name": "Amazon Redshift",
    "version": "1.0.0"
  }
}
```


**Copy S3 files to Redshift**  
```sql
COPY schema.table
FROM 's3://path/to/folder'
IAM_ROLE 'arn:aws:iam::nnnnnnnnnnn:role/name-role'
REGION 'us-west-2'
FORMAT PARQUET
IGNOREHEADER 1
REMOVEQUOTES
DATEFORMAT 'YYYYMMDD'
NULL AS ''
DELIMITER '\t'
QUOTE AS '\b'
GZIP
TRUNCATECOLUMNS;
```

From fixed-width data:  
```sql
copy venue
from 's3://mybucket/data/venue_fw.txt' 
iam_role 'arn:aws:iam::0123456789012:role/MyRedshiftRole'
fixedwidth 'venueid:3,venuename:25,venuecity:12,venuestate:2,venueseats:6';
```

From CSV with header:  
```sql
COPY schema_name.table_name
FROM 's3://mybucket/data/file.csv'
IAM_ROLE 'arn:aws:iam::0123456789012:role/MyRedshiftRole'
FORMAT CSV
IGNOREHEADER 1;
```

Some interesting options to mention:
- **TRUNCATECOLUMNS**: Truncates data in columns to the appropriate number of characters so that it fits the column specification. Applies only to columns with a VARCHAR or CHAR data type, and rows 4 MB or less in size.
- **REMOVEQUOTES**: Removes surrounding quotation marks from strings in the incoming data. All characters within the quotation marks, including delimiters, are retained.
- **GZIP**: Specifies that the input file is in compressed gzip format, and it will be decompressed when it is loaded.



**INT fields - Working with empty strings**  
Errors like `pg.InternalError: ERROR: Invalid digit, Value ' ', Pos 0, Type: Integer` can appear when trying to load empty strings to INT fields.  

To avoid this, you can use `NULLIF` to ensure that empty strings are always returned as nulls.  
This expression compares two arguments and returns null if the arguments are equal.  
If they are not equal, the first argument is returned.  
This expression is the inverse of the `NVL` or `COALESCE` expression.

This line should be enough: `NULLIF(max_age,'')::integer`

```sql
insert into category
values(0,'','Special','Special');

select NULLIF(catgroup,'') from category
where catdesc='Special';

catgroup
----------
null
(1 row)
```


**Duplicates tables**  

1) **Using CTAS**  

By doing it this way, `new_table` inherits ONLY the basic column definitions, null settings and default values of the `original_table`. It does not inherit table attributes.
```sql
CREATE TABLE new_table AS SELECT * FROM original_table;
```

2) **CREATE TABLE LIKE**  

To inherit all table definitions, use **deep copy** method, which recreates and repopulates a table by using a bulk insert, which automatically sorts the table.
```sql
CREATE TABLE new_table (LIKE original_table);
INSERT INTO new_table (SELECT * FROM original_table);
```

The new table inherits the encoding, distkey, sortkey, and notnull attributes of the parent table. The new table doesn't inherit the primary key and foreign key attributes of the parent table, but you can add them using `ALTER TABLE`.

3) **Use original DDL**  
```sql
CREATE TABLE new_table ( … );
INSERT INTO new_table (select * from original_table);
DROP TABLE original_table;
ALTER TABLE new_table RENAME TO original_table;
```

4) **Create temporary table**  

If you need to retain the primary key and foreign key attributes of the parent table, or if the parent table has dependencies, you can use `CREATE TABLE ... AS (CTAS)` to create a temporary table, then truncate the original table and populate it from the temporary table.   

Using a temporary table improves performance significantly compared to using a permanent table, but there is a risk of losing data.  

A temporary table is automatically dropped at the end of the session in which it is created. `TRUNCATE` commits immediately, even if it is inside a transaction block. If the `TRUNCATE` succeeds but the session terminates before the subsequent `INSERT` completes, the data is lost. If data loss is unacceptable, use a permanent table.
```sql
CREATE TEMP TABLE new_table AS SELECT * FROM original_table;
TRUNCATE original_table;
INSERT INTO original_table (SELECT * FROM new_table);
DROP TABLE new_table;
```


**VACUUM**  
Re-sorts rows and reclaims space in either a specified table or all tables in the current database.  

*Warning*: Only the table owner or a superuser can effectively vacuum a table. If VACUUM is run without the necessary table privileges, the operation completes successfully but has no effect.

```sql
VACUUM tbl;
VACUUM:  -- this will do it in the whole database
```

For those tables that use the sort key, an automatic sort is executed, which lessens the need to run VACUUM to keep data in sort key order.  
To determine whether your table will benefit by running `VACUUM SORT`, you can execute the following query:
```sql
select "table", unsorted, vacuum_sort_benefit from svv_table_info where "table" like '%name%';
```


**Disk space used & Data distribution**  
```sql
SELECT owner AS node, diskno, used, capacity, used/capacity::numeric * 100 as percent_used
FROM stv_partitions
WHERE host = node
ORDER BY 1, 2;
```

```sql
SELECT name, count(*)
FROM stv_blocklist
JOIN (SELECT DISTINCT name, id as tbl from stv_tbl_perm) USING (tbl)
where name like '%drug%'
GROUP BY name;
```

```sql
select trim(name) as table, stv_blocklist.slice, stv_tbl_perm.rows
from stv_blocklist,stv_tbl_perm
where stv_blocklist.tbl=stv_tbl_perm.id
and stv_tbl_perm.slice=stv_blocklist.slice
and stv_blocklist.id > 10000 and name not like '%#m%'
and name not like 'systable%' and name like '%drug%'
group by name, stv_blocklist.slice, stv_tbl_perm.rows
order by 3 desc;
```


**Check table compression**  
```sql
ANALYZE COMPRESSION tbl;
```



## Glue Spark <a name="glue-spark"></a>

Python scripts:
  - **[Glue jobs](glue_jobs.py)**
  - **[Glue partitions](glue_partitions.py)**


| Action                          | Code                                                                           |
| ------------------------------- | ------------------------------------------------------------------------------ |
| Rename field                    | `newDyF = oldDyF.rename_field("old_name", "new_name")`                         |
| DynamicFrame to Spark DataFrame | `df = dyndf.toDF()`                                                            |
| Spark DataFrame to DynamicFrame | `dyndf = DynamicFrame.fromDF(df, glueContext, 'dyndfname')`                    |
| Write to parquet partitioning   | `dyndf.toDF().write.parquet('s3://location/file_part', partitionBy=['field'])` |


**Import AWS Glue libraries and initialize GlueContext**  
```python
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

sc = SparkContext.getOrCreate()
gc = GlueContext(sc)
```

**Create DynamicFrame from catalog**  
```python
persons = glueContext.create_dynamic_frame.from_catalog(
             database="legislators",
             table_name="persons_json")
```

**Write DynamicFrame to S3**  
```python
glueContext.write_dynamic_frame.from_options(frame = people_df,
          connection_type = "s3",
          connection_options = {"path": "s3://glue-sample-target/output-dir/file"},
          format = "parquet")
```



### Glue Data Catalog <a name="glue-catalog"></a>

Prerequisites:  
```python
glue_client = boto3.client('glue')
cloudwatch = boto3.client('logs')
```

| Action                  | Code                                                                              |
| ----------------------- | --------------------------------------------------------------------------------- |
| Get crawler             | `glue_client.get_crawler(Name='crawler_name')['Crawler']`                         |
| Start crawler           | `glue_client.start_crawler(Name='crawler_name')`                                  |
| Get database            | `glue_client.get_database(Name='db_name')`                                        |
| Get table               | `table = glue_client.get_table(DatabaseName='db_name', Name='tbl_name')`          |
| Get table location      | `table['Table']['StorageDescriptor']['Location']`                                 |
| Get table columns       | `columns = table['Table']['StorageDescriptor']['Columns']`                        |
| Get table columns names | `[c['Name'] for c in columns]`                                                    |
| Delete tables           | `glue_client.batch_delete_table(DatabaseName='db_name', TablesToDelete=tbl_list)` |


**Get tables**  
```python
tables = glue_client.get_tables(DatabaseName='db_name')['TableList']
tbl_list = [table['Name'] for table in tables]
```


**Workflow and job info + Run job**  
```python
# Workflow runs: It seems they come already ordered so there is no need to order the results.
response = glue_client.get_workflow_runs(Name=workflow_name)
wf_id = response['Runs'][0]['WorkflowRunId']

# Job runs: It seems they come already ordered so there is no need to order the results.
response = glue_client.get_job_runs(JobName=job_name, MaxResults=1)
for job in response['JobRuns']:
    job_id = job['Id']
    response = glue_client.get_job_run(JobName=job_name,
                                       RunId=job_id)
    print(f"Job {job_id} started at {job['StartedOn']} and has status {job['JobRunState']}")

response = glue_client.start_job_run(JobName=job_name,
                                     Arguments={
                                         '--arg1': 'value1',
                                         '--arg2': 'value2'}
                                     )
```


**Job logs**  
```python
response = cloudwatch.get_log_events(
    logGroupName='/path/to/logs_group',
    logStreamName=job_id,
    # startTime=12345678,
    # endTime=12345678,
)

with open('Logs_CloudWatch.txt', 'w') as f:
    for event in response["events"]:
        f.write(event["message"])
```


**Get table partitions**  
```python
partitions = glue_client.get_partitions(DatabaseName=db,TableName=tbl)['Partitions']
len(partitions)  # Number of partitions
partitions_lst = [part['Values'] for part in partitions]  # [['2020-11-01'], ['2020-12-01']]
partitions_lst = [part['Values'][0] for part in partitions]  # ['2020-11-01', '2020-12-01']
```


**Delete table partitions**  
Batch mode:
```python
# Execute get_partitions() and save it to "partitions" variable
batch = 25

for i in range(0, len(partitions), batch):
    to_delete = [{k: v[k]} for k, v in zip(["Values"] * batch, partitions[i: i + batch])]
    # to_delete -> [{'Values': ['2020-11-01']}, {'Values': ['2020-12-01']}]
    glue_client.batch_delete_partition(
        DatabaseName=database,
        TableName=table,
        PartitionsToDelete=to_delete)
```

Single partition mode:
```python
response = glue_client.delete_partition(
    DatabaseName=database,
    TableName=table,
    PartitionValues=['string',])
```
