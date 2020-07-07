import datetime
import boto3
from botocore.exceptions import ClientError
import uuid
import json

dynamodb = boto3.client('dynamodb', 'us-east-1')
TABLE_NAME = 'chapter_21_github'


def put_repo(customer_id, repo_name,):
    created_at = datetime.datetime.now()
    try:
        result = dynamodb.put_item(
            TableName=TABLE_NAME,
            Item={
                'PK': {'S': 'REPO#{}#{}'.format(customer_id, repo_name)},
                'SK': {'S': 'REPO#{}#{}'.format(customer_id, repo_name)},
                "RepoName": {"S": repo_name},
                "RepoOwner": {"S": customer_id},
                "Created_At": {"S": created_at.isoformat()},
            }
        )
        # prints results from a succesful add
        print(result)
    except ClientError as e:
        print(e)


'''use this to put a new repo into the table, we will use the same repo name later too add our issues thuse creating an item collection.'''
# put_repo('jonathanbradbury', 'dynamodb_examples')


''' Now that we have a repo, lets define a function that will let us add issues to it'''


def add_issues_to_repo(customer_id, repo_name, issue_number, status):
    created_at = datetime.datetime.now()
    try:
        result = dynamodb.put_item(
            TableName=TABLE_NAME,
            Item={
                'PK': {'S': 'REPO#{}#{}'.format(customer_id, repo_name)},
                'SK': {'S': 'ISSUE#{}'.format(str(issue_number).zfill(9))},
                "RepoName": {"S": repo_name},
                "RepoOwner": {"S": customer_id},
                "Created_At": {"S": created_at.isoformat()},
                "IssueNumber": {"N": str(issue_number)},
                "Status": {"S": status}
            }
        )
        # prints results from a succesful add
        print(result)
    except ClientError as e:
        print(e)


'''When you add an issue you'll notice that it appears above the repo name. This is intentional.'''
# add_issues_to_repo('jonathanbradbury', 'dynamodb_examples', 1, 'Open')


'''Now we will make a function that will let us query our data. You can get data about the repo, and its most recent issues
Since the numbers are zero padded the most recent will always be closes to the SK that is your repo name. You can use that a 
pivot point. Scan index forward false means you go UP from the pivot point. Limit 11 means you get the piviot and the most recent 10
items above it (which is our issues).'''


def get_repo_and_issues(customer_id, repo_name):
    try:
        result = dynamodb.query(
            TableName=TABLE_NAME,
            KeyConditionExpression="#pk = :pk",
            ExpressionAttributeNames={
                "#pk": "PK", },
            ExpressionAttributeValues={
                ":pk": {"S": 'REPO#{}#{}'.format(customer_id, repo_name)},
            },
            ScanIndexForward=False,
            Limit=11)
        # prints results from a succesful query
        print(result)
        return result
    except ClientError as e:
        print(e)


# get_repo_and_issues('jonathanbradbury', 'dynamodb_examples')

''' this is just for fun, it shows you can query relational data which is the repo owner / create time, and some of it's issues (only the first one)'''


def print_pretty_results(result):
    repo_owner = result['Items'][0]['RepoOwner']['S']
    repo_created = result['Items'][0]['Created_At']['S']
    repo_name = result['Items'][0]['RepoName']['S']

    issue_number = result['Items'][1]['IssueNumber']['N']
    issue_created = result['Items'][1]['Created_At']['S']
    issue_status = result['Items'][1]['Status']['S']

    print('\nThe repo owner is {}\nit was created at {}\nthe name of repo is {}.\nThe most recent issue was created at {}.\nThe issue status is {}\nThe total number of issues is {}'.format(
        repo_owner, repo_created, repo_name, issue_created, issue_status, issue_number))


print_pretty_results(get_repo_and_issues(
    'jonathanbradbury', 'dynamodb_examples'))
