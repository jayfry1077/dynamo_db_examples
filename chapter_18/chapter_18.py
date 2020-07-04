import datetime
import boto3
from botocore.exceptions import ClientError
import uuid

'''This file contains examples for using a Session Store Table (Chapter 18 of The DynamoDB Book)'''
'''The schema for our table is a PK named session_token, and a secondary index of username. Attributes are created_at, expires_at, and TTL(epoch time)'''


dynamodb = boto3.client('dynamodb', 'us-east-1')


# Here is how we would add a sesson to the table for a user, there is no TTL attribute yet.
def put_item_into_db(username, session_token=str(uuid.uuid4())):

    created_at = datetime.datetime.now()
    expires_at = created_at + datetime.timedelta(days=7)

    try:
        result = dynamodb.put_item(
            TableName='chapter_18_session_store',
            Item={
                "session_token": {"S": session_token},
                "username": {"S": username},
                "created_at": {"S": created_at.isoformat()},
                "expires_at": {"S": expires_at.isoformat()}
            },
            ConditionExpression="attribute_not_exists(session_token)"
        )
        # prints results from a succesful add
        print(result)
    except ClientError as e:
        print(e)


'''Run this function if you want to put an item into the dynamoDB table. Don't provide a UUID but change the user name'''
# put_item_into_db('taco_bell')

'''take the uuid geneareted from the request above and pass it to the function, you will now get an error.'''
# put_item_into_db('joe_user', 'c8698d80-ef05-47b7-9978-70c51c8c8de6')
'''Even though the username is different our Condition is specifically on the session_token, so the request fails because we're using a token that already exist'''
'''An error occurred (ConditionalCheckFailedException) when calling the PutItem operation: The conditional request failed'''


'''Now we will put an item with an Attribute of TTL (you can name the attritbue what you like when setting up TTL on the table, I just named it TTL. The attribute value must be in epoch time.)'''
'''FROM AWS: TTL is a mechanism to set a specific timestamp for expiring items from your table. The timestamp should be expressed as an attribute on the items in the table.
The attribute should be a Number data type containing time in epoch format. Once the timestamp expires, the corresponding item is deleted from the table in the background'''

# same as adding an item above, but now we add the TTL attribute in epoch time.


def put_item_into_db_with_ttl(username, session_token=str(uuid.uuid4())):

    created_at = datetime.datetime.now()
    expires_at = created_at + datetime.timedelta(days=7)
    epoch = int(expires_at.timestamp())

    try:
        result = dynamodb.put_item(
            TableName='chapter_18_session_store',
            Item={
                "session_token": {"S": session_token},
                "username": {"S": username},
                "created_at": {"S": created_at.isoformat()},
                "expires_at": {"S": expires_at.isoformat()},
                "TTL": {"N": str(epoch)}
            },
            ConditionExpression="attribute_not_exists(session_token)"
        )
        # prints results from a succesful add
        print(result)
    except ClientError as e:
        print(e)


'''Put an item into dynamo with a TTL attribute'''
'''Run this twice so you can get the same User with multiple session tokens. We will delete them all in the next example'''
# put_item_into_db_with_ttl('TTL_USER')


'''now we're going to use our global secondary index on the username attribute to group all session tokens for that user, then we will delete them all'''


def get_tokens_by_username(username):
    # This is a SCAN opeartion but thats okay becuase we're using our secondary index to reduce the number of items to just the ones for this user (which would be 2 in this case.)
    # So even if you have a table with 1 million users as attributes, a scan on the secondary index will still run quick becuase of what I explained above.
    results = dynamodb.query(
        TableName='chapter_18_session_store',
        IndexName='username-index',
        ExpressionAttributeValues={
            ':username': {
                'S': username,
            },
        }, KeyConditionExpression='username = :username'
    )

    return results


def delete_session_tokes_for_user(items):
    for result in items['Items']:
        dynamodb.delete_item(
            TableName='chapter_18_session_store',
            Key={
                'session_token': result['session_token']
            }
        )


'''We pass the results from get_tokens_by_username to delete session tokens for user'''
# delete_session_tokes_for_user(get_tokens_by_username('TTL_USER'))
