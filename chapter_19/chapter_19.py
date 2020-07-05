import datetime
import boto3
from botocore.exceptions import ClientError
import uuid
import json

'''This file contains examples for using a Session Store Table (Chapter 19 of The DynamoDB Book)
I reccomend looking at the dynamo table after every function run so you get an idea of what it looks like.'''

'''Goals:
• Create Customer (unique on both username and email address)
• Create / Update / Delete Mailing Address for Customer
• Place Order
• Update Order
• View Customer & Most Recent Orders for Customer
• View Order & Order Items

'''

dynamodb = boto3.client('dynamodb', 'us-east-1')

TABLE_NAME = 'chapter19_ecom_customer_and_items'

'''
Customer
PK: CUSTOMER#<Username>
SK: CUSTOMER#<Username>
Email
PK: CUSTOMEREMAIL#<Email>
SK: CUSTOMEREMAIL#<Email>
'''


def add_customer_and_email(customer_name, customer_email):
    '''We want to add a customer by id and email, and have both be unique.
    To do this we need to add each individually as an item, but only count it as a success
    if both write, hence we use a transaction'''

    try:
        response = dynamodb.transact_write_items(
            TransactItems=[
                {
                    'Put': {
                        'TableName': TABLE_NAME,
                        'Item': {
                            'PK': {'S': 'CUSTOMER#{}'.format(customer_name)},
                            'SK': {'S': 'CUSTOMER#{}'.format(customer_name)},
                            'Username': {'S': customer_name},
                            'Name': {'S': customer_name}
                        },
                        # Since the Primary key will hold different attributes, email / username, we just keep it generic with PK
                        'ConditionExpression': 'attribute_not_exists(PK)'
                    }
                },
                {
                    'Put': {
                        'TableName': TABLE_NAME,
                        'Item': {
                            'PK': {'S': 'CUSTOMEREMAIL#{}'.format(customer_email)},
                            'SK': {'S': 'CUSTOMEREMAIL#{}'.format(customer_email)},
                        },
                        'ConditionExpression': 'attribute_not_exists(PK)'
                    }
                }
            ]
        )
        print(response)
    except ClientError as e:
        print(e)


'''If you run this twice the second time should fail, put your own user name and email try it out twice to make the failure happen!
If either of them are the same as an existing item it will fail. Thats the power of transactions.'''
# add_customer_and_email('Jonathan_Bradbury', 'Jonathan.bradbury@gmail.com')


def add_customer_and_email_addresses(customer_name, customer_email, addresses):
    '''Same as above but now with addresses. Since its just an attribute the additon of the items requires no other changes.'''

    try:
        response = dynamodb.transact_write_items(
            TransactItems=[
                {
                    'Put': {
                        'TableName': TABLE_NAME,
                        'Item': {
                            'PK': {'S': 'CUSTOMER#{}'.format(customer_name)},
                            'SK': {'S': 'CUSTOMER#{}'.format(customer_name)},
                            'Username': {'S': customer_name},
                            'Name': {'S': customer_name},
                            'Addresses': {'M':  address}
                        },
                        # Since the Primary key will hold different attributes, email / username, we just keep it generic with PK
                        'ConditionExpression': 'attribute_not_exists(PK)'
                    }
                },
                {
                    'Put': {
                        'TableName': TABLE_NAME,
                        'Item': {
                            'PK': {'S': 'CUSTOMEREMAIL#{}'.format(customer_email)},
                            'SK': {'S': 'CUSTOMEREMAIL#{}'.format(customer_email)},
                        },
                        'ConditionExpression': 'attribute_not_exists(PK)'
                    }
                }
            ]
        )
        print(response)
    except ClientError as e:
        print(e)


'''remember if you use the same customer email and name this will fail. If we just wanted to update an existing item we can but thats not what we're doing here'''

address = {"Home": {"M": {"Street": "Hello"}}}
#    "Business": {"Street": "1 Taco Way", "City": "Taco City"}}

add_customer_and_email_addresses(
    'Jonathan_B', 'JB@gmail.com', address)
