import datetime
import boto3
from botocore.exceptions import ClientError
import uuid
import json
import ulid

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


def add_customer_and_email_with_addresses(customer_name, customer_email, addresses):
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
                            'Email address': {'S': customer_email},
                            'Name': {'S': customer_name},
                            'Addresses': {'M':  addresses}
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

# address = {"Home": {
#     "M": {"Street": {"S": "123 Fake Street"}, "City": {"S": "Fake City"}}}, "Business": {"M": {"Street": {"S": "1 Taco Way"}, "City": {"S": "Taco City"}}}}

# add_customer_and_email_with_addresses(
#     'Jonathan_B', 'JB@gmail.com', address)


'''Now we're going to use dynamo item collections to store ORDER items with the CUSTOMER name.
We put #ORDER infront of the ORDER ID so that'''


def put_order_into_db(customer_id, order_id=str(ulid.ulid())):
    created_at = datetime.datetime.now()
    try:
        result = dynamodb.put_item(
            TableName=TABLE_NAME,
            Item={
                'PK': {'S': 'CUSTOMER#{}'.format(customer_id)},
                'SK': {'S': '#ORDER#{}'.format(order_id)},
                "order_id": {"S": order_id},
                "created_at": {"S": created_at.isoformat()},
                "status": {"S": 'SHIPPED'},
                "Amount": {"N": '67.43'},
                "number_items": {"N": '7'}
            }
        )
        # prints results from a succesful add
        print(result)
    except ClientError as e:
        print(e)


'''The key here is to use the same customer id you used before , however when we use a different sort key, it now becomes an item collection.
Running this multiple times will give the user multiple orders. I reccomend doing it. We're using ulid so that the order numbers are sorted.
You will notice the #ORDER is always above the CUSTOMER SK which is intentional'''

# put_order_into_db('Jonathan_B')


def ret_customer_and_most_recent_orders(customer_id):
    resp = dynamodb.query(
        TableName=TABLE_NAME,
        KeyConditionExpression='#pk = :pk',
        ExpressionAttributeNames={
            '#pk': 'PK'
        },
        ExpressionAttributeValues={
            ':pk': {'S': 'CUSTOMER#{}'.format(customer_id)}
        },
        ScanIndexForward=False,
        Limit=11
    )
    print(resp)


'''Since we set up the order ID to be lexigraphical they're already in order. We can use ScanIndexForward False to return that most recent 10 items from the customer name we specify
With this we get the customer details, and the order details.'''

# ret_customer_and_most_recent_orders('Jonathan_B')


'''Now we need to add the order items.
We Will add a Global secondary index with a Sort Key using the OrderID. Since our order items, and our orders will have the same index of GSI1PK we can group them togeather'''


def put_order_into_db_with_GSI(customer_id, order_id=str(ulid.ulid())):
    created_at = datetime.datetime.now()
    try:
        result = dynamodb.put_item(
            TableName=TABLE_NAME,
            Item={
                'PK': {'S': 'CUSTOMER#{}'.format(customer_id)},
                'SK': {'S': '#ORDER#{}'.format(order_id)},
                "order_id": {"S": order_id},
                "created_at": {"S": created_at.isoformat()},
                "status": {"S": 'SHIPPED'},
                "Amount": {"N": '67.43'},
                "number_items": {"N": '7'},
                "GSI1PK": {"S": 'ORDER# {}'.format(order_id)},
                "GSI1SK": {"S": 'ORDER# {}'.format(order_id)}
            }
        )
        # prints results from a succesful add
        print(result)
    except ClientError as e:
        print(e)


'''For this we're going to add orders just like we did above, but we will inclde the global secondary index and sortkey this time'''

# put_order_into_db_with_GSI('Jonathan_B')

# Get the order ID from the dynamo table


def put_order_items(order_id, item_id):

    try:
        result = dynamodb.put_item(
            TableName=TABLE_NAME,
            Item={
                'PK': {'S': 'ORDER#{0}#ITEM#{1}'.format(order_id, item_id)},
                'SK': {'S': 'ORDER#{0}#ITEM#{1}'.format(order_id, item_id)},
                "order_id": {"S": '88da49e72b80'},
                "item_id": {"S": item_id},
                "Description": {"S": 'Tacos & Bells'},
                "Price": {"N": '67.43'},
                "GSI1PK": {"S": 'ORDER#{0}'.format(order_id)},
                "GSI1SK": {"S": 'ITEM#{0}'.format(item_id)}
            }
        )
        # prints results from a succesful add
        print(result)
    except ClientError as e:
        print(e)


'''add some different order items each time you call to make it more fun
remember get an existing order id from dynamo'''
# put_order_items('01ECGPW001CM7KKWW13X1P2KV7', '199997642')


'''lastly we want to query all the items in an order by Order ID'''


def query_items_by_order(order_id):
    results = dynamodb.query(
        TableName=TABLE_NAME,
        IndexName='GSI1PK-GSI1SK-index',
        ExpressionAttributeValues={
            ':GSI1PK': {
                'S': 'ORDER#{}'.format(order_id),
            },
        }, KeyConditionExpression='GSI1PK = :GSI1PK'
    )

    print(results)


# query_items_by_order('01ECGPW001CM7KKWW13X1P2KV7')
