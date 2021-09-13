import boto3

glue_client = boto3.client('glue')
cloudwatch = boto3.client('logs')

# Input arguments
mode = 'LOGS'  # EXECUTION | LOGS
workflow_name = 'workflow_name'
job_name = 'job_name'

# They come already ordered so there is no need to order the results.
response = glue_client.get_workflow_runs(Name=workflow_name)
wf_id = response['Runs'][0]['WorkflowRunId']

response = glue_client.get_workflow_run(Name=workflow_name,
                                        RunId=wf_id)
print(f"Workflow {wf_id} started at {response['Run']['StartedOn']} and has status {response['Run']['Status']}")

response = glue_client.get_job_runs(JobName=job_name, MaxResults=1)
for job in response['JobRuns']:
    job_id = job['Id']
    response = glue_client.get_job_run(JobName=job_name,
                                       RunId=job_id)
    print(f"Job {job_id} started at {job['StartedOn']} and has status {job['JobRunState']}")


if mode == 'EXECUTION':
    response = glue_client.start_job_run(JobName=job_name,
                                         Arguments={
                                             '--arg1': 'val1',
                                             '--arg2': 'val2'}
                                         )
    print(response)

elif mode == 'LOGS':
    print(f'Getting logs for {job_id}')

    response = cloudwatch.get_log_events(
        logGroupName='/path/to/log_group_name',
        logStreamName=job_id,
        # startTime=12345678,
        # endTime=12345678,
    )

    with open('Logs_CloudWatch.txt', 'w') as f:
        for event in response["events"]:
            f.write(event["message"])
