# Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import boto3
import json
import base64
import logging
import os
import pg
import pgdb
import random
import string
import time

from urllib.parse import quote as urlQuote
from botocore.exceptions import ClientError

# Setup logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# SSM Parameters
region_name = os.environ['REGION_NAME']
enviroment = os.environ['ENVIRONMENT']
app = os.environ['APP']

# ECS Parameters
ecs_cluster = os.environ['ECS_CLUSTER']
restart_reason = os.environ['RESTART_REASON']
serviceName = os.environ['SERVICE_NAME']
timeBetween = os.environ['TIME_BETWEEN']

#  RDS Parameters
port = 5432
dbname = os.environ['DATABASE_NAME']
dbhost = os.environ['DATABASE_HOST']
dbuser = os.environ['DATABASE_USERNAME']


# Create a SSM and ECS client
session = boto3.session.Session()
ssm_client = session.client(
    service_name='ssm',
    region_name=region_name
)
ecs = boto3.client('ecs')


def lambda_handler(event, context):
    # Current Secret via SSM
    current_secret = get_current_secret(
        '/rds/'+enviroment+'/'+enviroment+'-'+app+'/PASSWORD')
    # Connect to RDS using current secret
    conn = get_connection(current_secret)
    # Generates new random secret
    new_secret = generate_secret()
    # Connects to RDS and rotate secret
    rotate_secret(conn, new_secret, current_secret)
    # IMPORT! checks if the new secret is valid before update any SSM parameter
    test_result = get_connection(new_secret)
    # If new secret succefully connects to RDS then update SSM for application
    if test_result != None:
        logger.info('Secret successfully rotated')
        update_parameter_ssm(
            '/rds/'+enviroment+'/'+enviroment+'-'+app+'/PASSWORD', new_secret)
        database_url = build_db_url(new_secret)
        update_parameter_ssm(
            '/app/'+enviroment+'/'+app+'/DATABASE_URL', database_url)
        # Restart application container
        restart_tasks()
    else:
        # If new secret is invalid SSM parameters remains current secret
        logger.info('Secret NOT rotate, SSM parameters not updated')


# Get current secret from SSM


def get_current_secret(parameter):
    logger.info('Getting current secret from:')
    logger.info(parameter)
    response = ssm_client.get_parameter(
        Name=parameter,
        WithDecryption=True
    )
    return response['Parameter']['Value']

# Get connection to RDS for rotation and secret validation


def get_connection(secret):
    logger.info('Connecting to RDS:')
 # Try to obtain a connection to the db
    try:
        conn = pgdb.connect(host=dbhost,
                            user=dbuser,
                            password=secret,
                            database=dbname,
                            port=port,
                            connect_timeout=5)
        return conn
    except pg.InternalError:
        logger.error('Fail could not connect to database')
        return None


# Genrates random password with exluded charactes as postgres does noe accept them


def generate_secret(size=32, chars=string.ascii_letters + string.digits + "!#$%&()*+,-.;<=>?[]^_{|}~"):
    # Puntuation exluding :/@"\'\\
    return ''.join(random.choice(chars) for _ in range(size))

# Uses connection and rotates password


def rotate_secret(conn, new_secret, current_secret):
    try:
        with conn.cursor() as cur:
            # Check if the user exists, if not create it and grant it all permissions from the current role
            # If the user exists, just update the password
            cur.execute("SELECT 1 FROM pg_roles where rolname = %s",
                        (dbuser,))
            if len(cur.fetchall()) == 0:
                create_role = "CREATE ROLE \"%s\"" % dbuser
                cur.execute(create_role + " WITH LOGIN PASSWORD %s",
                            (current_secret,))
                cur.execute("GRANT \"%s\" TO \"%s\"" %
                            (dbuser, dbuser))
            else:
                alter_role = "ALTER USER \"%s\"" % dbuser
                cur.execute(alter_role + " WITH PASSWORD %s",
                            (new_secret,))

            conn.commit()

    finally:
        conn.close()

# Build final Database URL


def build_db_url(secret_data):
    return 'postgres://'+dbuser+':'+urlQuote(secret_data)+'@'+dbhost+':'+str(port)+'/'+dbname

# Update SSM parameters


def update_parameter_ssm(parameter, value):
    logger.info('Updating parameter:')
    logger.info(parameter)
    args = {
        'Name': parameter,
        'Value': value,
        'Overwrite': True,
        'Type': "SecureString"
    }
    response = ssm_client.put_parameter(**args)
    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        exit(1)
    else:
        return response['ResponseMetadata']['HTTPStatusCode']

# Restart application containers


def restart_tasks():
    try:
        response = ecs.list_tasks(
            cluster=ecs_cluster,
            serviceName=serviceName
        )
    except ClientError as e:
        raise e

    logger.info("finishGetTasks: Successfully got list of tasks.")

    tasks_list = response['taskArns']

    for task in tasks_list:
        try:
            ecs.stop_task(
                cluster=ecs_cluster,
                task=task,
                reason=restart_reason
            )
        except ClientError as e:
            raise e
        time.sleep(int(timeBetween))
    logger.info(
        "finishRestartTask: Successfully restarted tasks from cluster %s." % (ecs_cluster))
