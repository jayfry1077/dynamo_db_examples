
import datetime
import boto3
from botocore.exceptions import ClientError
from botocore.exceptions import ValidationError
import uuid
import json

dynamodb = boto3.client('dynamodb', 'us-east-1')
TABLE_NAME = 'chapter_21_github'


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
        print(result)
        return result
    except ClientError as e:
        print(e)


def update_issue_status(customer_id, repo_name, issue_number, status):
    try:
        result = dynamodb.update_item(
            TableName=TABLE_NAME,
            Key={
                'PK': {'S': 'REPO#{}#{}'.format(customer_id, repo_name)},
                'SK': {'S': 'ISSUE#{}'.format(str(issue_number).zfill(9))}
            },
            AttributeUpdates={
                'Status': {'Value': {'S': 'Closed'}}
            }
        )
        print(result)
    except ClientError as e:
        print(e)


def query_repo_and_pull_request(customer_id, repo_name):
    results = dynamodb.query(
        TableName=TABLE_NAME,
        IndexName='GSI1-index',
        ExpressionAttributeValues={
            ':GSI1PK': {
                'S': 'REPO#{}#{}'.format(customer_id, repo_name),
            },
        }, KeyConditionExpression='GSI1PK = :GSI1PK'
    )

    print(results)


def add_issues_to_repo_with_increment(customer_id, repo_name, status):
    created_at = datetime.datetime.now()
    try:
        resp = dynamodb.update_item(
            TableName=TABLE_NAME,
            Key={
                'PK': {'S': 'REPO#{}#{}'.format(customer_id, repo_name)},
                'SK': {'S': 'REPO#{}#{}'.format(customer_id, repo_name)}
            },
            UpdateExpression="SET #count = #count + :incr",
            ExpressionAttributeNames={
                "#count": "IssuesAndPullRequestCount",
            },
            ExpressionAttributeValues={
                ":incr": {"N": "1"}
            },
            ReturnValues='UPDATED_NEW'
        )

        current_count = resp['Attributes']['IssuesAndPullRequestCount']['N']

        result = dynamodb.put_item(
            TableName=TABLE_NAME,
            Item={
                'PK': {'S': 'REPO#{}#{}'.format(customer_id, repo_name)},
                'SK': {'S': 'ISSUE#{}'.format(str(current_count).zfill(9))},
                "RepoName": {"S": repo_name},
                "RepoOwner": {"S": customer_id},
                "Created_At": {"S": created_at.isoformat()},
                "IssueNumber": {"N": str(current_count)},
                "Status": {"S": status}
            }
        )
        print(result)
    except ClientError as e:
        print(e)


def put_pull_request_with_increment(customer_id, repo_name):
    created_at = datetime.datetime.now()
    try:
        resp = dynamodb.update_item(
            TableName=TABLE_NAME,
            Key={
                'PK': {'S': 'REPO#{}#{}'.format(customer_id, repo_name)},
                'SK': {'S': 'REPO#{}#{}'.format(customer_id, repo_name)}
            },
            UpdateExpression="SET #count = #count + :incr",
            ExpressionAttributeNames={
                "#count": "IssuesAndPullRequestCount",
            },
            ExpressionAttributeValues={
                ":incr": {"N": "1"}
            },
            ReturnValues='UPDATED_NEW'
        )

        current_count = resp['Attributes']['IssuesAndPullRequestCount']['N']

        result = dynamodb.put_item(
            TableName=TABLE_NAME,
            Item={
                'PK': {'S': 'PR#{}#{}#{}'.format(customer_id, repo_name, str(current_count).zfill(9))},
                'SK': {'S': 'PR#{}#{}#{}'.format(customer_id, repo_name, str(current_count).zfill(9))},
                "RepoName": {"S": repo_name},
                "RepoOwner": {"S": customer_id},
                "Created_At": {"S": created_at.isoformat()},
                "PullRquestNumber": {"N": str(current_count)},
                "GSI1PK": {"S": 'REPO#{}#{}'.format(customer_id, repo_name)},
                "GSI1SK": {"S": 'PR#{}'.format(str(current_count).zfill(9))}
            }
        )
        print(result)
    except ClientError as e:
        print(e)


def get_issues_with_filter(customer_id, repo_name, status_filter):
    try:
        result = dynamodb.query(
            TableName=TABLE_NAME,
            KeyConditionExpression="#pk = :pk",
            FilterExpression="attribute_not_exists(#status) OR #status = :status",
            ExpressionAttributeNames={
                "#pk": "PK",
                "#status": "Status"
            },
            ExpressionAttributeValues={
                ":pk": {"S": 'REPO#{}#{}'.format(customer_id, repo_name)},
                ":status": {"S": status_filter}
            },
            ScanIndexForward=False
        )

        print(result)
        return result
    except ClientError as e:
        print(e)


def query_repo_and_forks(customer_id, repo_name):
    results = dynamodb.query(
        TableName=TABLE_NAME,
        IndexName='GSI2-index',
        ExpressionAttributeValues={
            ':GSI2PK': {
                'S': 'REPO#{}#{}'.format(customer_id, repo_name),
            },
        }, KeyConditionExpression='GSI2PK = :GSI2PK'
    )

    print(results)


def query_stars_no_issues(repo_owner, repo_name):
    result = dynamodb.query(
        TableName=TABLE_NAME,
        KeyConditionExpression="#pk = :pk AND #sk >= :sk",
        ExpressionAttributeNames={
            "#pk": "PK",
            "#sk": "SK"
        },
        ExpressionAttributeValues={
            ":pk": {"S": 'REPO#{}#{}'.format(repo_owner, repo_name)},
            ":sk": {"S": 'REPO#{}#{}'.format(repo_owner, repo_name)}
        },
        ScanIndexForward=True
    )

    print(result)


def query_issues_no_stars(repo_owner, repo_name):
    result = dynamodb.query(
        TableName=TABLE_NAME,
        KeyConditionExpression="#pk = :pk AND #sk <= :sk",
        ExpressionAttributeNames={
            "#pk": "PK",
            "#sk": "SK"
        },
        ExpressionAttributeValues={
            ":pk": {"S": 'REPO#{}#{}'.format(repo_owner, repo_name)},
            ":sk": {"S": 'REPO#{}#{}'.format(repo_owner, repo_name)}
        },
        ScanIndexForward=True
    )

    print(result)


def add_stars_to_repo_with_increment(repo_owner, repo_name, starring_user):
    try:
        result = dynamodb.transact_write_items(
            TransactItems=[
                {
                    "Put": {
                        "Item": {
                            'PK': {'S': 'REPO#{}#{}'.format(repo_owner, repo_name)},
                            'SK': {'S': 'STAR#{}'.format(starring_user)},
                            "RepoName": {"S": repo_name},
                            "RepoOwner": {"S": repo_owner},
                            "StarringUser": {"S": starring_user}
                        },
                        "TableName": TABLE_NAME,
                        "ConditionExpression": "attribute_not_exists(SK)"
                    }
                },
                {
                    "Update": {
                        "Key": {
                            'PK': {'S': 'REPO#{}#{}'.format(repo_owner, repo_name)},
                            'SK': {'S': 'REPO#{}#{}'.format(repo_owner, repo_name)}
                        },
                        "TableName": TABLE_NAME,
                        "ConditionExpression": "attribute_exists(PK)",
                        "UpdateExpression": "SET #count = #count + :incr",
                        "ExpressionAttributeNames": {
                            "#count": "StarCount"
                        },
                        "ExpressionAttributeValues": {
                            ":incr": {"N": "1"}
                        }
                    }
                }
            ]
        )
        print(result)
    except ClientError as e:
        print(e)


def add_forks_to_repo_with_increment(repo_owner, repo_name, forking_user):
    try:
        created_at = datetime.datetime.now()
        result = dynamodb.transact_write_items(
            TransactItems=[
                {
                    "Put": {
                        "Item": {
                            'PK': {'S': 'REPO#{}#{}'.format(forking_user, repo_name)},
                            'SK': {'S': 'REPO#{}#{}'.format(forking_user, repo_name)},
                            "RepoName": {"S": repo_name},
                            "RepoOwner": {"S": forking_user},
                            "Created_At": {"S": created_at.isoformat()},
                            "GSI1PK": {"S": 'REPO#{}#{}'.format(forking_user, repo_name)},
                            "GSI1SK": {"S": 'REPO#{}#{}'.format(forking_user, repo_name)},
                            "GSI2PK": {"S": 'REPO#{}#{}'.format(repo_owner, repo_name)},
                            "GSI2SK": {"S": 'FORK#{}'.format(forking_user)},
                            "IssuesAndPullRequestCount": {"N": '0'},
                            "StarCount": {"N": '0'},
                            "ForkCount": {"N": '0'}
                        },
                        "TableName": TABLE_NAME,
                        "ConditionExpression": "attribute_not_exists(PK)"
                    }
                },
                {
                    "Update": {
                        "Key": {
                            'PK': {'S': 'REPO#{}#{}'.format(repo_owner, repo_name)},
                            'SK': {'S': 'REPO#{}#{}'.format(repo_owner, repo_name)}
                        },
                        "TableName": TABLE_NAME,
                        "ConditionExpression": "attribute_exists(PK)",
                        "UpdateExpression": "SET #count = #count + :incr",
                        "ExpressionAttributeNames": {
                            "#count": "ForkCount"
                        },
                        "ExpressionAttributeValues": {
                            ":incr": {"N": "1"}
                        }
                    }
                }
            ]
        )
        print(result)
    except ClientError as e:
        print(e)


def put_user_with_org(user, org, payment_plan):
    try:
        created_at = datetime.datetime.now()
        result = dynamodb.put_item(
            TableName=TABLE_NAME,
            Item={
                'PK': {'S': 'ACCOUNT#{}'.format(user)},
                'SK': {'S': 'ACCOUNT#{}'.format(user)},
                "Type": {"S": 'User'},
                "UserName": {"S": user},
                "CreatedAt": {"S": created_at.isoformat()},
                "Organizations": {"M": org},
                "PaymentPlan": {"M": payment_plan},
                "GSI3PK": {'S': 'ACCOUNT#{}'.format(user)},
                "GSI3SK": {'S': 'ACCOUNT#{}'.format(user)}
            }
        )
        print(result)
    except ClientError as e:
        print(e)


def put_org(org, payment_plan):
    try:
        created_at = datetime.datetime.now()
        result = dynamodb.put_item(
            TableName=TABLE_NAME,
            Item={
                'PK': {'S': 'ACCOUNT#{}'.format(org)},
                'SK': {'S': 'ACCOUNT#{}'.format(org)},
                "Type": {"S": 'Organization'},
                "OrganizationName": {"S": org},
                "CreatedAt": {"S": created_at.isoformat()},
                "PaymentPlan": {"M": payment_plan},
                "GSI3PK": {'S': 'ACCOUNT#{}'.format(org)},
                "GSI3SK": {'S': 'ACCOUNT#{}'.format(org)}
            }
        )
        print(result)
    except ClientError as e:
        print(e)


def put_membership(org, member):
    created_at = datetime.datetime.now()
    try:
        result = dynamodb.put_item(
            TableName=TABLE_NAME,
            Item={
                'PK': {'S': 'ACCOUNT#{}'.format(org)},
                'SK': {'S': 'MEMBERSHIP#{}'.format(member)},
                "Role": {"S": 'Member'},
                "UserName": {"S": member},
                "CreatedAt": {"S": created_at.isoformat()},
            }
        )
        print(result)
    except ClientError as e:
        print(e)


def put_repo_with_GSI_GSI2_GSI3_increment(account_id, repo_name):
    created_at = datetime.datetime.now()
    try:
        result = dynamodb.put_item(
            TableName=TABLE_NAME,
            Item={
                'PK': {'S': 'REPO#{}#{}'.format(account_id, repo_name)},
                'SK': {'S': 'REPO#{}#{}'.format(account_id, repo_name)},
                "RepoName": {"S": repo_name},
                "RepoOwner": {"S": account_id},
                "Created_At": {"S": created_at.isoformat()},
                "GSI1PK": {"S": 'REPO#{}#{}'.format(account_id, repo_name)},
                "GSI1SK": {"S": 'REPO#{}#{}'.format(account_id, repo_name)},
                "GSI2PK": {"S": 'REPO#{}#{}'.format(account_id, repo_name)},
                "GSI2SK": {"S": 'REPO#{}'.format(repo_name)},
                "GSI3PK": {"S": 'ACCOUNT#{}'.format(account_id)},
                "GSI3SK": {"S": '#{}'.format(created_at.isoformat())},
                "IssuesAndPullRequestCount": {"N": '0'},
                "StarCount": {"N": '0'},
                "ForkCount": {"N": '0'}
            }
        )
        # prints results from a succesful add
        print(result)
    except ClientError as e:
        print(e)


def query_user_info(account_name):
    results = dynamodb.query(
        TableName=TABLE_NAME,
        IndexName='GSI3-index',
        ExpressionAttributeValues={
            ':GSI3PK': {
                'S': 'ACCOUNT#{}'.format(account_name),
            },
        }, KeyConditionExpression='GSI3PK = :GSI3PK'
    )

    print(results)


if __name__ == '__main__':
    # add our repo
    put_repo_with_GSI_GSI2_GSI3_increment(
        'jonathan.bradbury@gmail.com', 'Dynamo_is_awesome')

    # Add an issue to the repo, it will auto assign an issue number starting at 1
    add_issues_to_repo_with_increment(
        'jonathan.bradbury@gmail.com', 'Dynamo_is_awesome', 'OPEN')

    # This should be issue 2
    add_issues_to_repo_with_increment(
        'jonathan.bradbury@gmail.com', 'Dynamo_is_awesome', 'CLOSED')

    # create a pull request for the repo
    put_pull_request_with_increment(
        'jonathan.bradbury@gmail.com', 'Dynamo_is_awesome')

    # add a star to my repo and increment the total star count
    add_stars_to_repo_with_increment(
        'jonathan.bradbury@gmail.com', 'Dynamo_is_awesome', 'Robbie Kohler')

    # forking a repo turns the fork into esentially another repo, so you can run the above functions on the fork
    add_forks_to_repo_with_increment(
        'jonathan.bradbury@gmail.com', 'Dynamo_is_awesome', 'Robbie.Kohler@gmail.com')

    # for example i can add a star to the forked repo
    add_stars_to_repo_with_increment(
        'Robbie.Kohler@gmail.com', 'Dynamo_is_awesome', 'Jonathan Bradbury')

    # puts an org and payment plan type.
    put_org('SuperEvilMegaCorp',  {'planType': {
        'S': 'PRO'}, 'planStartDate': {'S': '2020-07-08 09:00:00'}})

    # add a user to an org
    put_membership('SuperEvilMegaCorp', 'Jonathan Bradbury')

    # add a user , with an org type
    put_user_with_org('jonathan.bradbury@gmail.com', {'SuperEvilMegaCorp': {'S': 'Member'}}, {'planType': {
        'S': 'PRO'}, 'planStartDate': {'S': '2020-07-08 09:00:00'}})

    # since our item collect has both issues and stars we run this to just get issues.
    query_issues_no_stars('jonathan.bradbury@gmail.com', 'Dynamo_is_awesome')

    # now we get stars but no issues
    query_stars_no_issues('jonathan.bradbury@gmail.com', 'Dynamo_is_awesome')

    query_repo_and_forks('jonathan.bradbury@gmail.com', 'Dynamo_is_awesome')

    query_repo_and_pull_request(
        'jonathan.bradbury@gmail.com', 'Dynamo_is_awesome')

    query_user_info('jonathan.bradbury@gmail.com')

    get_issues_with_filter('jonathan.bradbury@gmail.com',
                           'Dynamo_is_awesome', 'OPEN')

    get_repo_and_issues('jonathan.bradbury@gmail.com', 'Dynamo_is_awesome')
