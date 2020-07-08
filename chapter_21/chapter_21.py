
import datetime
import boto3
from botocore.exceptions import ClientError
from botocore.exceptions import ValidationError
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


# print_pretty_results(get_repo_and_issues(
#     'jonathanbradbury', 'dynamodb_examples'))


''' another one just for fun, this will update the status of an existing issue'''


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
        # prints results from a succesful add
        print(result)
    except ClientError as e:
        print(e)


# update_issue_status('jonathanbradbury', 'dynamodb_examples', 1, 'Closed')

''' When we start considering combining the repos with pull request we have a need for a secondary index. The function below to put a repo is the exact same
however it has the GSIPK and SK included. '''


def put_repo_with_GSI(customer_id, repo_name):
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
                "GSI1PK": {"S": 'REPO#{}#{}'.format(customer_id, repo_name)},
                "GSI1SK": {"S": 'REPO#{}#{}'.format(customer_id, repo_name)},
                "IssuesAndPullRequestCount": {"N": '0'},
            }
        )
        # prints results from a succesful add
        print(result)
    except ClientError as e:
        print(e)


def put_pull_request(customer_id, repo_name, pull_request_number):
    created_at = datetime.datetime.now()
    try:
        result = dynamodb.put_item(
            TableName=TABLE_NAME,
            Item={
                'PK': {'S': 'PR#{}#{}#{}'.format(customer_id, repo_name, str(pull_request_number).zfill(9))},
                'SK': {'S': 'PR#{}#{}#{}'.format(customer_id, repo_name, str(pull_request_number).zfill(9))},
                "RepoName": {"S": repo_name},
                "RepoOwner": {"S": customer_id},
                "Created_At": {"S": created_at.isoformat()},
                "PullRquestNumber": {"N": str(pull_request_number)},
                "GSI1PK": {"S": 'REPO#{}#{}'.format(customer_id, repo_name)},
                "GSI1SK": {"S": 'PR#{}'.format(str(pull_request_number).zfill(9))}
            }
        )
        # prints results from a succesful add
        print(result)
    except ClientError as e:
        print(e)


''' So now lets put a new repo that have the GSI. Also add some issues to your repo.'''
# put_repo_with_GSI('jonathanbradbury', 'example_repo')
# add_issues_to_repo('jonathanbradbury', 'example_repo', 2, 'Open')

'''now lets add a pull request for that repo
"ID is shared across Issues and Pull Requests in a Repo. You won’t
have an Issue and a Pull Request with the same ID in a Repo."
For a real life example try not to give your pull request the same ID as an issue
'''

# put_pull_request('jonathanbradbury', 'example_repo', 5)


''' Now that we have a shared secondary index on both our repos and our pull request
we can do a query to join the info'''


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


''' When you look at the dynamo table, you will see that the values returned by this query are all seperate items in the table
we group them on the GSI1PK and 1 query returns all the results. So now we can see all the pull request and the data for the REPO main item.
Note, we did not add the GSI to the ISSUE items, but if we did they would show up too'''

# query_repo_and_pull_request('jonathanbradbury', 'example_repo')


''' The next pattern talks about tracking the number of pull request and issues in a seperate attribute. So we're going to 
reuse the put issue and pull request, but add some code to update the issue/pullrequest count attribute
This will let uss add issues and pull request and not have to worry about what number we need to use'''

#put_repo_with_GSI('jonathanbradbury', 'example_repo')


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
        # prints results from a succesful add
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
        # prints results from a succesful add
        print(result)
    except ClientError as e:
        print(e)


# add_issues_to_repo_with_increment('jonathanbradbury', 'example_repo', 'Closed')
# put_pull_request_with_increment('jonathanbradbury', 'example_repo')


''' now lets get our issue items by open or closed status'''


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


''' now lets get our issues items by open or closed status'''
# get_issues_with_filter('jonathanbradbury', 'example_repo', 'Closed')


''' Okay so now we're adding forks, which means we need to haave another GSI'''


def put_repo_with_GSI_GSI2_increment(customer_id, repo_name):
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
                "GSI1PK": {"S": 'REPO#{}#{}'.format(customer_id, repo_name)},
                "GSI1SK": {"S": 'REPO#{}#{}'.format(customer_id, repo_name)},
                "GSI2PK": {"S": 'REPO#{}#{}'.format(customer_id, repo_name)},
                "GSI2SK": {"S": 'REPO#{}'.format(repo_name)},
                "IssuesAndPullRequestCount": {"N": '0'},
                "StarCount": {"N": '0'},
                "ForkCount": {"N": '0'}
            }
        )
        # prints results from a succesful add
        print(result)
    except ClientError as e:
        print(e)


# put_repo_with_GSI_GSI2_increment('jonathanbradbury', 'another_example')
# add_issues_to_repo_with_increment(
#     'jonathanbradbury', 'another_example', 'Open')


'''Now lets add some forks!'''


def put_fork_with_GSI_GSI2_increment(customer_id, repo_owner, repo_name):
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
                "GSI1PK": {"S": 'REPO#{}#{}'.format(customer_id, repo_name)},
                "GSI1SK": {"S": 'REPO#{}#{}'.format(customer_id, repo_name)},
                "GSI2PK": {"S": 'REPO#{}#{}'.format(repo_owner, repo_name)},
                "GSI2SK": {"S": 'FORK#{}'.format(customer_id)},
                "IssuesAndPullRequestCount": {"N": '0'},
            }
        )
        # prints results from a succesful add
        print(result)
    except ClientError as e:
        print(e)


# put_fork_with_GSI_GSI2_increment(
#     'Zoidberg', 'jonathanbradbury', 'another_example')


''' now lets get our repo and all the forks'''


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


# query_repo_and_forks('jonathanbradbury', 'another_example')


''' okay now lets give users the ability to star a repo'''


def add_stars_to_repo(customer_id, repo_name, starring_user):
    try:
        result = dynamodb.put_item(
            TableName=TABLE_NAME,
            Item={
                'PK': {'S': 'REPO#{}#{}'.format(customer_id, repo_name)},
                'SK': {'S': 'STAR#{}'.format(starring_user)},
                "RepoName": {"S": repo_name},
                "RepoOwner": {"S": customer_id},
                "StarringUser": {"S": str(starring_user)}
            }
        )
        # prints results from a succesful add
        print(result)
    except ClientError as e:
        print(e)


# add_stars_to_repo('jonathanbradbury', 'another_example', 'Robbie Kohler')


''' Okay now lets query stars for a repo 
Notice "#pk = :pk AND #sk >= :sk" This is where the alphabetical ordering of dynamo comes into play. We always have
our issues ABOVE the SK and our stars BELOW. So you can use sk >=SK to get all the items below (stars) , and
<= sk to get all the items above (issues)
'''

'''Note the queries below are returning items from the same collection'''


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


# query_stars_no_issues('jonathanbradbury', 'another_example')
# query_issues_no_stars('jonathanbradbury', 'another_example')


''' the above examples for adding forks and stars are great, but we also want to count 
the number of forks and stars. We will do the same thing we did for the issue and pull request
tracker'''


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


# add_stars_to_repo_with_increment(
#     'jonathanbradbury', 'another_example', 'RobbieKohler')

'''and our queries still work no issues'''
# query_stars_no_issues('jonathanbradbury', 'another_example')
# query_issues_no_stars('jonathanbradbury', 'another_example')


''' time to update the forks API to update the forks number '''


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


''' if someone were to star our forked repo, they can just use the star function we already have, since we set up forks to be just like normal repos'''
# add_forks_to_repo_with_increment(
#     'jonathanbradbury', 'another_example', 'RobbieKohler')


''' i'm leaving out messages and reactions (for now ) since they're not tied to any data, you just query them out, thats a simple mode we have seen a dozen times already'''


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
        # prints results from a succesful add
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
        # prints results from a succesful add
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


''' okay now lets create a new repo that includes GSI3, then we will add accounts for the user of this repo,
we will add an org and make this user a member of that org'''

# put_repo_with_GSI_GSI2_GSI3_increment('Flexo', 'how_to_bend')

# put_user_with_org('Flexo', {'Bending_Inc': {'S': 'Member'}}, {'planType': {
#                   'S': 'PRO'}, 'planStartDate': {'S': '2020-07-08 09:00:00'}})

# put_org('Bending_inc', {'planType': {
#     'S': 'PRO'}, 'planStartDate': {'S': '2020-07-08 09:00:00'}})

# put_membership('Bending_inc', 'Flexo')

''' Now that our repos and our users share a GSI3, we can query on GSI3 to get user info, and what repos they have.'''


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


query_user_info('Flexo')
