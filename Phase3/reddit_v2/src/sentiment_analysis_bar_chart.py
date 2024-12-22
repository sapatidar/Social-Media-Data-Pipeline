import os
import threading
from concurrent.futures import ThreadPoolExecutor
from pymongo import MongoClient
from textblob import TextBlob
import matplotlib.pyplot as plt
import pandas as pd
from logger_setup import setup_logger

# Logger setup
logger = setup_logger("sentiment_analysis", log_file='logs/sentiment_analysis.log', max_bytes=10*1024*1024, backup_count=5)

# MongoDB Configurations
MONGODB_URI = "mongodb://vchoudhary:password@127.0.0.1:27017/jobMarketDB"
COMMENTS_COLLECTION = "reddit_comments"
POSTS_COLLECTION = "reddit_posts"
BATCH_SIZE = 10000
MAX_WORKERS = 10

# Define the sentiment analysis function
def analyze_sentiment(text):
    analysis = TextBlob(text)
    if analysis.sentiment.polarity > 0:
        return "positive"
    elif analysis.sentiment.polarity < 0:
        return "negative"
    else:
        return "neutral"

# Define function to process a batch
def process_batch(records, sentiments):
    for record in records:
        try:
            text = record.get("body", "") or record.get("selftext", "") or ""
            if text:
                sentiment = analyze_sentiment(text)
                subreddit = record.get("subreddit", "unknown")
                sentiments.append({"subreddit": subreddit, "sentiment": sentiment})
        except Exception as e:
            logger.error(f"Error processing record {record.get('_id', 'unknown')}: {e}")
    logger.info("Batch processed")


# Query MongoDB and process data
def fetch_and_analyze_sentiments(subreddit=None, date_from=None, date_to=None):
    """
    Fetch and analyze sentiments from MongoDB based on subreddit and date range filters.

    Args:
        subreddit (str): Subreddit name to filter (optional).
        date_from (str): Start date in ISO format (YYYY-MM-DD) (optional).
        date_to (str): End date in ISO format (YYYY-MM-DD) (optional).

    Returns:
        list: Analyzed sentiments.
    """
    try:
        client = MongoClient(MONGODB_URI)
        db = client["jobMarketDB"]

        # Query comments and posts collections
        collections = [COMMENTS_COLLECTION, POSTS_COLLECTION]
        sentiments = []
        
        # Construct filter condition
        filter_condition = {"subreddit": {"$ne": "politics"}}
        if subreddit:
            filter_condition["subreddit"] = subreddit
        if date_from or date_to:
            filter_condition["utc"] = {}
            if date_from:
                filter_condition["utc"]["$gte"] = date_from
            if date_to:
                filter_condition["utc"]["$lte"] = date_to

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for collection_name in collections:
                collection = db[collection_name]

                # Fetch total record count
                total_records = collection.count_documents(filter_condition)
                logger.info(f"Found {total_records} records in {collection_name} for sentiment analysis.")

                # Fetch records in batches
                skip = 0
                while skip < total_records:
                    batch = list(collection.find(filter_condition).skip(skip).limit(BATCH_SIZE))
                    if not batch:
                        break
                    executor.submit(process_batch, batch, sentiments)
                    skip += BATCH_SIZE

        df = pd.DataFrame(sentiments)
        sentiment_counts = df.groupby(["subreddit", "sentiment"]).size().unstack(fill_value=0)
        return sentiment_counts.to_dict()

    except Exception as e:
        logger.error(f"Error fetching and analyzing sentiments: {e}")
        return []


# Plot sentiment bar chart
def plot_sentiment_bar_chart(sentiments):
    try:
        # Convert to DataFrame
        df = pd.DataFrame(sentiments)
        sentiment_counts = df.groupby(["subreddit", "sentiment"]).size().unstack(fill_value=0)

        # Plot bar chart
        sentiment_counts.plot(kind="bar", stacked=True, figsize=(12, 6))
        plt.title("Sentiment Distribution by Subreddit")
        plt.xlabel("Subreddits")
        plt.ylabel("Counts")
        plt.xticks(rotation=45)

        # Save the graph as an image
        script_directory = os.path.dirname(os.path.abspath(__file__))  # Get current script directory
        graph_file_path = os.path.join(script_directory, "sentiment_bar_chart.png")
        plt.savefig(graph_file_path, bbox_inches="tight")
        logger.info(f"Bar graph saved as image at: {graph_file_path}")

        plt.show()

    except Exception as e:
        logger.error(f"Error while plotting sentiment bar chart: {e}")
