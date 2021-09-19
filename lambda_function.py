import os
import sys
import json
import logging
import pymysql

#rds config
rds_host  = os.environ['connection_str']
name = os.environ['db_username']
password = os.environ['db_password']
db_name = os.environ['db_name']

logger = logging.getLogger()
logger.setLevel(logging.INFO)

#connect to rds - mysql db
try:
    conn = pymysql.connect(host=rds_host, user=name, passwd=password, db=db_name, connect_timeout=5)
except pymysql.MySQLError as e:
    logger.error("ERROR: Unexpected error: Could not connect to MySQL instance")
    logger.error(e)
    sys.exit()

logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")

def retrieve_contacts(page, items, orderBy, order):
    """
    Retrieve all contacts from db
    """
    resDict = {}
    offsetVal = (page - 1) * items;

    cursor = conn.cursor()
    selectQuery = 'SELECT name, phone_num FROM contacts ORDER BY {0} {1} LIMIT {2} OFFSET {3}'.format(orderBy, order, items, offsetVal)
    cursor.execute(selectQuery)
    rows = cursor.fetchall()
    
    countQuery = 'SELECT COUNT(*) FROM contacts'
    cursor.execute(countQuery)
    count = cursor.fetchone()
    
    resDict['data'] = rows
    resDict['count'] = count[0]
    
    return resDict
    
def create_contacts(name, phoneNum):
    """
    Insert contacts to db
    """
    cursor = conn.cursor()
    insertQuery = '''INSERT INTO contacts(name, phone_num) VALUES ('%s', '%s')''' % (name, phoneNum)
    
    try:
        cursor.execute(insertQuery)
        conn.commit()
        return [True, 'Contact creation successful for {0}'.format(name)]
    except Exception as e:
        conn.rollback()
        logger.error(e)
        errorMsg = str(e)
        
        if name in errorMsg:
            return [False, "Duplicate Name"]
        else:
            return [False, errorMsg]

def lambda_handler(event, context):
    
    """
    HTTP status:
    200 - db update successful
    500 - db update fail
    422 - missing required parameter i.e. POST request missing name or phoneNum in body
    """

    # call retrieve_contacts func
    if event.get('rawPath') == '/getContacts':
        page ,items = 1, 10
        availOrderBy = {'name', 'phone_num'}
        availOrder = {'ASC', 'DESC'}
        orderBy = 'name'
        order = 'ASC'
        
        queryStrParam = event.get('queryStringParameters')
        
        if queryStrParam:
            if 'page' in queryStrParam: 
                page = int(queryStrParam.get('page'))
            
            if 'items' in queryStrParam:
                items = int(queryStrParam.get('items'))
            
            if 'orderBy' in queryStrParam:
                orderBy = queryStrParam.get('orderBy')
        
            if 'order' in queryStrParam:
                order = queryStrParam.get('order').upper()
            
        if orderBy not in availOrderBy or order not in availOrder:
            return {
                'statusCode': 422,
                'body': json.dumps('Invalid sorting')
            }
        
        res = retrieve_contacts(page, items, orderBy, order)
        
        return {
            'statusCode': 200,
            'body': json.dumps(res)
        }
    
    # call create_contacts func
    else:
        name = phoneNum = ''
        
        bodyStr = event.get('body').strip().replace('\r\n', '')
        body = json.loads(bodyStr)
        
        if 'name' in body:
            name = body.get('name')
            
        if 'phoneNum' in body:
            phoneNum = body.get('phoneNum')
        
        if name != '' and phoneNum != '':
            res, msg = create_contacts(name, phoneNum)
            
            if res:
                statusCode = 200
            else:
                statusCode = 500
            
            return {
                'statusCode': statusCode,
                'body': json.dumps(msg)
            }
        else:
            return {
                'statusCode': 422,
                'body': json.dumps('Missing required parameter')
            }