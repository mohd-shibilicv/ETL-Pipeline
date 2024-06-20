from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import snscrape.modules.twitter as sntwitter
import pandas as pd
from sqlalchemy import create_engine


# ETL Functions (same as above)
def extract_tweets(query, limit=100):
    tweets = []
    for tweet in sntwitter.TwitterSearchScraper(query).get_items():
        if len(tweets) == limit:
            break
        tweets.append(
            {
                "date": tweet.date,
                "id": tweet.id,
                "content": tweet.content,
                "username": tweet.user.username,
                "replyCount": tweet.replyCount,
                "retweetCount": tweet.retweetCount,
                "likeCount": tweet.likeCount,
                "quoteCount": tweet.quoteCount,
            }
        )
    return pd.DataFrame(tweets)


def transform_tweets(df):
    df["date"] = pd.to_datetime(df["date"])
    df["content"] = df["content"].str.replace("\n", " ")
    df["username"] = df["username"].str.lower()
    return df


def load_tweets(df, db_name, table_name):
    engine = create_engine(f"sqlite:///{db_name}.db")
    df.to_sql(table_name, con=engine, if_exists="replace", index=False)


def run_etl():
    QUERY = "Python -filter:retweets"
    LIMIT = 100
    DB_NAME = "twitter_data"
    TABLE_NAME = "tweets"

    tweets_df = extract_tweets(QUERY, LIMIT)
    transformed_df = transform_tweets(tweets_df)
    load_tweets(transformed_df, DB_NAME, TABLE_NAME)


# Airflow DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 6, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "twitter_etl_dag",
    default_args=default_args,
    description="A simple Twitter ETL pipeline",
    schedule=timedelta(days=1),
)

etl_task = PythonOperator(
    task_id="run_etl",
    python_callable=run_etl,
    dag=dag,
)
