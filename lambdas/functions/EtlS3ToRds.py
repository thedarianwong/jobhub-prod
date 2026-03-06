import csv
import json
import logging
import os
import re
from datetime import datetime, timedelta
from io import StringIO

import boto3
import psycopg2

logging.basicConfig(level=logging.INFO)

TABLE_COLUMNS = [
    'job_url', 'site', 'title', 'company', 'company_url', 'location',
    'job_type', 'date_posted', 'date_fetched', 'interval', 'min_amount',
    'max_amount', 'currency', 'is_remote', 'num_urgent_words', 'benefits',
    'emails', 'description', 'city',
]

CANADA_RE = re.compile(r'Canada|, CA$')
NULL_VALUES = {'', 'nan', 'NaN', 'None', 'none', 'NULL'}


def get_db_connection():
    return psycopg2.connect(
        host=os.environ['DB_HOST'],
        port=os.environ['DB_PORT'],
        dbname=os.environ['DB_NAME'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD'],
        sslmode='require',
    )


def read_csv_from_s3(bucket, key):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj['Body'].read().decode('utf-8')


def process_rows(content):
    cutoff = (datetime.now() - timedelta(days=30)).date()
    today = str(datetime.now().date())
    reader = csv.DictReader(StringIO(content))
    rows = []

    for row in reader:
        location = row.get('location', '')
        if not location or not CANADA_RE.search(location):
            continue

        date_str = row.get('date_posted', '')
        try:
            date_posted = datetime.strptime(date_str[:10], '%Y-%m-%d').date()
        except (ValueError, TypeError):
            continue
        if date_posted < cutoff:
            continue

        city = location.split(',')[0].strip()

        record = {}
        for col in TABLE_COLUMNS:
            if col == 'city':
                record[col] = city
            elif col == 'date_fetched':
                record[col] = today
            else:
                val = row.get(col, '')
                record[col] = None if val in NULL_VALUES else val

        rows.append(record)

    return rows


def insert_jobs(rows, conn):
    if not rows:
        return 0
    cols = list(rows[0].keys())
    insert_cols = ', '.join([f'"{c}"' for c in cols])
    placeholders = ', '.join(['%s'] * len(cols))
    sql = f'INSERT INTO jobs ({insert_cols}) VALUES ({placeholders}) ON CONFLICT (job_url) DO NOTHING'
    inserted = 0
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(sql, [row[c] for c in cols])
            inserted += cur.rowcount
    return inserted


def delete_stale_jobs(conn):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM jobs WHERE date_posted < NOW() - INTERVAL '30 days'")
        deleted = cur.rowcount
    logging.info("Deleted %d stale jobs.", deleted)


def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    logging.info("Processing s3://%s/%s", bucket, key)

    content = read_csv_from_s3(bucket, key)
    rows = process_rows(content)
    logging.info("Rows after filtering: %d", len(rows))

    with get_db_connection() as conn:
        inserted = insert_jobs(rows, conn)
        delete_stale_jobs(conn)

    logging.info("Inserted %d new jobs.", inserted)
    return {
        'statusCode': 200,
        'body': json.dumps({'inserted': inserted, 'processed': len(rows)}),
    }
