#!/usr/bin/env python3
import requests
from datetime import date
import boto3
from botocore.exceptions import ClientError
import yfinance as yf
import json
import pymysql
import re
from bs4 import BeautifulSoup
import time

def get_meta_block(trade_id):
    url = f"https://www.capitoltrades.com/trades/{trade_id}"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to load trade page: {response.status_code}")
    soup = BeautifulSoup(response.text, "html.parser")
    meta_tag = soup.find("meta", attrs={"property": "og:description"})
    if not meta_tag or not meta_tag.get("content"):
        raise Exception("Description meta tag not found")
    return meta_tag["content"]

def get_secret():
    secret_name = "algotrader-secret-credentials"
    region_name = "us-east-1"
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e
    secret = get_secret_value_response['SecretString']
    return json.loads(secret)

def data_preprocess(meta_block):
    trade_info = {}
    match = re.match(
        r"(.*?) (bought|sold) \$([\d,]+) of (.*?) on (\d{4}-\d{2}-\d{2})\. (He|She) filed the trade after (\d+) days\.",
        meta_block
    )
    if match:
        trade_info = {
            "name": match.group(1),
            "action": match.group(2),
            "amount": int(match.group(3).replace(",", "")),
            "company": match.group(4),
            "date": match.group(5),
            "filing_delay_days": int(match.group(7))
        }
    else:
        print("No match.")
    return trade_info

def insert_one_row(cursor, trade_id):
    try:
        metablock = get_meta_block(str(trade_id))
        row = data_preprocess(metablock)

        insert_sql = """
        INSERT IGNORE INTO congressional_holdings 
        (name, action, amount, company, date, filing_delay_days)
        VALUES (%s, %s, %s, %s, %s, %s);
        """
        values = (
            row["name"],
            row["action"],
            row["amount"],
            row["company"],
            row["date"],
            row["filing_delay_days"]
        )
        cursor.execute(insert_sql, values)
        print(f"Trade ID {trade_id} inserted.")
        return True  # Success
    except Exception as e:
        print(f"Trade ID {trade_id} failed: {e}")
        return False  # Failure

def load_last_trade_id(filepath="congress_holdings_data/last_trade_id.json"):
    try:
        with open(filepath, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

def save_last_trade_id(data, filepath="congress_holdings_data/last_trade_id.json"):
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)
        
def update_data():
    secret = get_secret()
    conn = pymysql.connect(
        host=secret['host'],
        port=secret['port'],
        user=secret['username'],
        password=secret['password'],
        database=secret['dbInstanceIdentifier']
    )
    today = str(date.today())
    tracker = load_last_trade_id()
    # Use today’s ID or fall back to most recent date
    if today in tracker:
        start_id = tracker[today]
    else:
        if tracker:
            most_recent = max(tracker.keys())
            start_id = tracker[most_recent]
        else:
            start_id = 20003780000  # Default fallback starting point
    max_to_check = 50
    with conn.cursor() as cursor:
        for trade_id in range(start_id + 1, start_id + 1 + max_to_check):
            success = insert_one_row(cursor, trade_id)
            if success:
                tracker[today] = trade_id
                save_last_trade_id(tracker)
            time.sleep(0.5)
            conn.commit()
    conn.close()
#start!
update_data()