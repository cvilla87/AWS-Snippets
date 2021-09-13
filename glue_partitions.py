import boto3

glue_client = boto3.client('glue')

# Input arguments
ATHENA = False  # Athena queries
DELETE = True
partitions = ['2021-05-31', '2021-06-31']
db_names = ['db1', 'db2']


for db in db_names:
    print(f'\n{db}')
    database = glue_client.get_database(Name=db)['Database']
    print(database['LocationUri'])

    tables = glue_client.get_tables(
        DatabaseName=db,
        # Expression='string',  # regular expression pattern to add filtering
        )['TableList']
    tbl_list = [table['Name'] for table in tables]

    for tbl in tbl_list:
        to_delete = [{'Values': [part]} for part in partitions]
        print(f'{tbl} -> {to_delete}')
        if ATHENA:
            for item in to_delete:
                print(f"ALTER TABLE {db}.{tbl} DROP PARTITION (meta__load_dttm = '{item['Values'][0]}');")

        if DELETE:
            try:
                # Delete partitions - In case of single value in partitions
                glue_client.delete_partition(
                    DatabaseName=db,
                    TableName=tbl,
                    PartitionValues=partitions)
            except:  # EntityNotFoundException
                print(f'Partition {partitions} for table {tbl} does not exist.')
