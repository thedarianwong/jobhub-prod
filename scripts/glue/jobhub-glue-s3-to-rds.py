import os
import sys
import boto3
import logging
import psycopg2
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

TABLE_COLUMNS = [
    'job_url', 'site', 'title', 'company', 'company_url', 'location',
    'job_type', 'date_posted', 'date_fetched', 'interval', 'min_amount',
    'max_amount', 'currency', 'is_remote', 'num_urgent_words', 'benefits',
    'emails', 'description', 'city',
]

def load_environment_variables():
    try:
        from awsglue.utils import getResolvedOptions
        args = getResolvedOptions(sys.argv, ['S3_PATH', 'DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD'])
        return args
    except Exception:
        from dotenv import load_dotenv
        load_dotenv()
        return {
            'DB_HOST': os.getenv('DB_HOST'),
            'DB_PORT': os.getenv('DB_PORT'),
            'DB_NAME': os.getenv('DB_NAME'),
            'DB_USER': os.getenv('DB_USER'),
            'DB_PASSWORD': os.getenv('DB_PASSWORD'),
            'S3_PATH': os.getenv('S3_PATH'),
        }

def is_s3_path(file_path):
    return file_path.startswith('s3://')

def read_csv(file_path):
    logging.info("Reading data from %s", file_path)
    try:
        if is_s3_path(file_path):
            s3 = boto3.client('s3')
            bucket_name = file_path.split('/')[2]
            s3_path = '/'.join(file_path.split('/')[3:])
            obj = s3.get_object(Bucket=bucket_name, Key=s3_path)
            df = pd.read_csv(BytesIO(obj['Body'].read()))
        else:
            df = pd.read_csv(file_path)
    except Exception as e:
        logging.error("Error reading CSV file: %s", e)
        sys.exit(1)

    if 'Unnamed: 0' in df.columns:
        df = df.drop(columns=['Unnamed: 0'])

    return df

def process_data(df):
    df['date_posted'] = pd.to_datetime(df['date_posted'], format='%Y-%m-%d', errors='coerce')

    one_month_ago = datetime.now() - timedelta(days=30)
    df = df[df['date_posted'] >= one_month_ago]
    df = df.sort_values(by='date_posted', ascending=False)

    df_canada = df[df['location'].str.contains(r'Canada|, CA$', regex=True, na=False)].copy()

    df_canada['city'] = df_canada['location'].apply(lambda x: x.split(',')[0].strip())
    df_canada['date_fetched'] = datetime.now().date()

    cols_to_insert = [c for c in TABLE_COLUMNS if c in df_canada.columns]
    return df_canada[cols_to_insert]

def delete_stale_jobs(conn):
    with conn.cursor() as cursor:
        cursor.execute("DELETE FROM jobs WHERE date_posted < NOW() - INTERVAL '30 days'")
    logging.info("Deleted stale jobs older than 30 days.")

def load_to_postgres(df, db_config):
    logging.info("Loading data into PostgreSQL...")
    try:
        with psycopg2.connect(
            host=db_config["DB_HOST"],
            port=db_config["DB_PORT"],
            dbname=db_config["DB_NAME"],
            user=db_config["DB_USER"],
            password=db_config["DB_PASSWORD"],
            sslmode="require",
        ) as conn:
            with conn.cursor() as cursor:
                insert_columns = ', '.join([f'"{col}"' for col in df.columns])
                placeholders = ', '.join(['%s'] * len(df.columns))
                insert_sql = f'INSERT INTO jobs ({insert_columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING'
                for row in df.itertuples(index=False, name=None):
                    cursor.execute(insert_sql, row)
            delete_stale_jobs(conn)
    except Exception as e:
        logging.error("Error loading data into PostgreSQL: %s", e)
        sys.exit(1)
    else:
        logging.info("Data loaded successfully into the 'jobs' table.")

def main(file_path):
    db_config = load_environment_variables()
    df = read_csv(file_path)
    df_processed = process_data(df)
    load_to_postgres(df_processed, db_config)

if __name__ == "__main__":
    db_config = load_environment_variables()
    file_path = db_config.get('S3_PATH', 'aggregated_jobs.csv')
    main(file_path)
