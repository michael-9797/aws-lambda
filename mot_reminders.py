import boto3
import json
import logging
import sys
import os
from dateutil.relativedelta import relativedelta
from datetime import datetime
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr


#Fixed list of people to be exempt from MOT reminders
exempt = [
        "LYDD","TCC","FRANK MECHANIC", "VIVIAN", "S&A", "JASPER", "JOE MECHANIC", "VALCO", "AHMED MOTORS", "MAURITIAN ALI", "BURHAN"
    ]

def lambda_handler(event, context):
    logging.getLogger().setLevel(logging.INFO)
    
    #Calculates the current date, goes back a year and stores this to startDate
    startDate = datetime.now().date() - relativedelta(years=1) + relativedelta(days=1)
    window_days=6
    date_range = get_dates(startDate, window_days)

    #Initialise results lists
    customers_to_text = []
    empty_phone_numbers = []

    logging.info(f"start date: {date_range[0]}  end date: {date_range[-1]}")
    
    #Retrieves all records for the next six days, based on last year's data
    customers = query(date_range)
    
    #Checks for customers without a phone number, and eliminates traders from the text messages list
    for c in customers:
        if not c["NAME"] in exempt:
            if c["PHONE NUMBER"]:
                customers_to_text.append(c)
            else:
                empty_phone_numbers.append(c)
        else:
            if c["PHONE NUMBER"]:
                customers_to_text.append(c)
        
    logging.info(f"{len(customers_to_text)} customers to text" )
    logging.info(f"{len(empty_phone_numbers)} records had not phone number" )
    
    ####################################  SET UP SNS  ###############################################
    
    client = boto3.client("sns","eu-west-2")
    client.set_sms_attributes(
        attributes={
            'DefaultSMSType': 'Transactional',
            'DeliveryStatusSuccessSamplingRate': '100',
            'DefaultSenderID': "Terrys-Ltd"   
        }
    )
    
    ####################################  TEXT CUSTOMERS  ###########################################
    
    phone_number = "+447933856902"
    for customer in customers_to_text[:2]:
        logging.info(f"raw: {customer}, type {type(customer)}")
        msg = f"Your vehicle with Registration: {customer['REG']} is due for its MOT. Please contact Terry's Ltd to make a booking: 02088008862 or 07957217217"
        response = client.publish(
            PhoneNumber=f'{phone_number}',
            Message=msg
        )
        logging.info(response)
    
    
 
    
   
    
        #for b in empty_phone_numbers:
        #    client.publish(
        #        PhoneNumber=f'{g["PHONE NUMBER"]}', <- change to your email address!!
        #        Message=f"Hi Terry, the following customers had no phone number linked to their record. Please text them manually."
        #    )

       

    
       

#####################################  QUERY  ##############################################


#Return list of dates in window as strings in ISO fomat
def get_dates(start_date,window):
    dates = [ (start_date + relativedelta(days=day)).strftime("%Y-%m-%d") for day in range(0,window+1) ]
    return dates
    
#Query dynamodb over a range of dates
def query(dates, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('MOT')
    
    #Get items 
    customers = []
    
    for date in dates:
        query_kwargs = {
        "IndexName":"DATE-index",
        "KeyConditionExpression": Key('DATE').eq(date)
        }
    
        response = table.query(
                     **query_kwargs
                )
         
        customers.extend(response["Items"])
        
        while "LastEvaluatedKey" in response:
            query_kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]
            response = table.query(
                     **query_kwargs
                     )
            customers.extend(response["Items"])

    return customers

