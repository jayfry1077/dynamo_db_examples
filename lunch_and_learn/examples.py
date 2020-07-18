import datetime
import boto3
from botocore.exceptions import ClientError
from botocore.exceptions import ValidationError
import uuid
import json

dynamodb = boto3.client('dynamodb', 'us-east-1')
TABLE_NAME = 'lunch_and_learn'


def put_owner(owner_name, franchise_name, owner_phone, owner_email, PPP):
    created_at = datetime.datetime.now()
    try:
        result = dynamodb.put_item(
            TableName=TABLE_NAME,
            Item={
                'PK': {'S': 'OWNER#{}'.format(owner_name)},
                'SK': {'S': 'OWNER#{}'.format(owner_name)},
                "FranchiseName": {"S": franchise_name},
                "OwnerPhone": {"S": owner_phone},
                "OwnerEmail": {"S": owner_email},
                "PPP": {"S": PPP},
                "Created_At": {"S": created_at.isoformat()},
            }
        )
        print(result)
    except ClientError as e:
        print(e)


def get_owner(owner_name):
    try:
        result = dynamodb.query(
            TableName=TABLE_NAME,
            KeyConditionExpression="#pk = :pk AND #sk = :sk",
            ExpressionAttributeNames={
                "#pk": "PK",
                "#sk": "SK"},
            ExpressionAttributeValues={
                ":pk": {'S': 'OWNER#{}'.format(owner_name)},
                ":sk": {'S': 'OWNER#{}'.format(owner_name)}
            },
        )

        print(result)
        return result
    except ClientError as e:
        print(e)


def get_owner_and_stores(owner_name):
    try:
        result = dynamodb.query(
            TableName=TABLE_NAME,
            KeyConditionExpression="#pk = :pk",
            ExpressionAttributeNames={
                "#pk": "PK", },
            ExpressionAttributeValues={
                ":pk": {'S': 'OWNER#{}'.format(owner_name)},
            },
        )

        print(result)
        return result
    except ClientError as e:
        print(e)


def add_store_to_owner(owner_name, store_number, store_phone, store_email, store_address, status, territory, region, market, area):
    created_at = datetime.datetime.now()
    try:
        result = dynamodb.put_item(
            TableName=TABLE_NAME,
            Item={
                'PK': {'S': 'OWNER#{}'.format(owner_name)},
                'SK': {'S': 'STORE#{}'.format(store_number.zfill(6))},
                "StorePhone": {"S": store_phone},
                "StoreEmail": {"S": store_email},
                "StoreAddress": {"S": store_address},
                "Status": {"S": status},
                "Territory": {"S": territory},
                "Region": {"S": region},
                "Market": {"S": market},
                "Area": {"S": area},
                "GSI1": {"S": 'STORE#{}'.format(store_number.zfill(6))},
                "Created_At": {"S": created_at.isoformat()},
            }
        )
        print(result)
    except ClientError as e:
        print(e)


def add_store(store_number):
    created_at = datetime.datetime.now()
    try:
        result = dynamodb.put_item(
            TableName=TABLE_NAME,
            Item={
                'PK': {'S': 'STORE#{}'.format(store_number)},
                'SK': {'S': 'STORE#{}'.format(store_number.zfill(6))},
                "Created_At": {"S": created_at.isoformat()},
            }
        )
        print(result)
    except ClientError as e:
        print(e)


def get_store_with_filter(owner_name, territory, region, market, area):
    try:
        result = dynamodb.query(
            TableName=TABLE_NAME,
            KeyConditionExpression="#pk = :pk",
            FilterExpression="#terr = :terr AND #market = :market AND #region = :region AND #area = :area",
            ExpressionAttributeNames={
                "#pk": "PK",
                "#terr": "Territory",
                "#market": "Market",
                "#region": "Region",
                "#area": "Area"
            },
            ExpressionAttributeValues={
                ":pk": {'S': 'OWNER#{}'.format(owner_name)},
                ":terr": {"S": territory},
                ":region": {"S": region},
                ":market": {"S": market},
                ":area": {"S": area},
            }
        )

        print(result)
        return result
    except ClientError as e:
        print(e)


def get_owner_info_by_store(store, owner_name):
    try:
        result = dynamodb.query(
            TableName=TABLE_NAME,
            IndexName='GSI1-index',
            KeyConditionExpression="GSI1 = :GSI1",
            ExpressionAttributeValues={
                ":GSI1": {'S': 'OWNER#{}'.format(owner_name)}
            }
        )

        print(result)
        return result
    except ClientError as e:
        print(e)


def get_owner_info_by_store_bad(store_number, owner_name):
    try:
        result = dynamodb.query(
            TableName=TABLE_NAME,
            KeyConditionExpression="#pk = :pk",
            FilterExpression="#store = :store_number",
            ExpressionAttributeNames={
                "#pk": "PK",
                "#store": "GSI1"
            },
            ExpressionAttributeValues={
                ":pk": {'S': 'OWNER#{}'.format(owner_name)},
                ":store_number": {'S': 'STORE#{}'.format(store_number)}
            }
        )

        print(result)
        return result
    except ClientError as e:
        print(e)


def add_employee_to_store(store_number, employee_id, employee_name, employee_age, employee_role):
    created_at = datetime.datetime.now()
    try:
        result = dynamodb.put_item(
            TableName=TABLE_NAME,
            Item={
                'PK': {'S': 'STORE#{}'.format(store_number.zfill(6))},
                'SK': {'S': 'EMPLOYEE#{}'.format(employee_id)},
                'GSI1': {'S': 'STORE#{}'.format(store_number.zfill(6))},
                'Name': {'S': employee_name},
                'Age': {'N': employee_age},
                'Role': {'S': employee_role},
                'GSI2': {'S': 'EMPLOYEE#{}'.format(employee_id)},
                "Created_At": {"S": created_at.isoformat()},
            }
        )
        print(result)
    except ClientError as e:
        print(e)


def get_employees_by_store(store_number):
    try:
        result = dynamodb.query(
            TableName=TABLE_NAME,
            KeyConditionExpression="#pk = :pk AND begins_with(#sk, :sk)",
            ExpressionAttributeNames={
                "#pk": "PK",
                "#sk": "SK"
            },
            ExpressionAttributeValues={
                ":pk": {'S': 'STORE#{}'.format(store_number)},
                ":sk": {'S': 'EMPLOYEE#'}
            }
        )
        print(result)
        return result
    except ClientError as e:
        print(e)


def get_store_by_employeeID(employee_id):
    try:
        result = dynamodb.query(
            TableName=TABLE_NAME,
            IndexName='GSI2-index',
            KeyConditionExpression="GSI2 = :GSI2",
            ExpressionAttributeValues={
                ":GSI2": {'S': 'EMPLOYEE#{}'.format(employee_id)}
            }
        )

        print(result)
        return result
    except ClientError as e:
        print(e)


def add_menu_items_to_store(store_number, menu_item_id, price, tax, description):
    created_at = datetime.datetime.now()
    try:
        result = dynamodb.put_item(
            TableName=TABLE_NAME,
            Item={
                'PK': {'S': 'STORE#{}'.format(store_number.zfill(6))},
                'SK': {'S': 'ITEM#{}'.format(menu_item_id)},
                'ItemID': {'S': menu_item_id},
                'Price': {'S': price},
                'Tax': {'S': tax},
                'description': {'S': description},
                "Created_At": {"S": created_at.isoformat()},
            }
        )
        print(result)
    except ClientError as e:
        print(e)


def get_items_by_store(store_number):
    try:
        result = dynamodb.query(
            TableName=TABLE_NAME,
            KeyConditionExpression="#pk = :pk AND begins_with(#sk, :sk)",
            ExpressionAttributeNames={
                "#pk": "PK",
                "#sk": "SK"
            },
            ExpressionAttributeValues={
                ":pk": {'S': 'STORE#{}'.format(store_number)},
                ":sk": {'S': 'ITEM#'}
            }
        )
        print(result)
        return result
    except ClientError as e:
        print(e)

# put_owner('Jonathan_Bradbury', "Taco Hut",
#           '714-394-5161', 'jonathan.bradbury@yum.com', 'PPP1234567')

# get_owner('Jonathan_Bradbury')

# get_owner_and_stores('Jonathan_Bradbury')

# add_store_to_owner('Jonathan_Bradbury', '000017', '555-555-1514', '000017@tacobell.com',
#                    '17 Burrito road, Mexican Pizza City, CA, 92617', 'OPEN', 'Territory1', 'Region1', 'Market1', 'Area2')

# get_store_with_filter('Jonathan_Bradbury', 'Territory1',
#                       'Region1', 'Market1', 'Area1')

# add_store('000017')

# get_owner_info_by_store('000017', 'Jonathan_Bradbury')


'''This shows that using filter expressions scans more data and cost more money'''
# get_owner_info_by_store_bad('000017', 'Jonathan_Bradbury')

# add_employee_to_store('000015', 'jxb7210',
#                       'Jonathan Bradbury', '32', 'Service Champion')


# get_employees_by_store('000015')

# get_store_by_employeeID('jxb7210')


# add_menu_items_to_store('000017', '00004', '3.50', '.27', 'Baja Blast')

# get_items_by_store('000017')
