import os
import threading
from queue import Queue
from pymongo import MongoClient
from textblob import TextBlob
from collections import defaultdict
import matplotlib.pyplot as plt
from logger_setup import setup_logger
from datetime import datetime
import pymongo

# Logger setup
logger = setup_logger("4chan_sentiment", log_file="logs/4chan_sentiment.log", max_bytes=10*1024*1024, backup_count=5)

# Multithreading setup
NUM_THREADS = 8
BATCH_SIZE = 1000  # Number of records per batch

# Function for sentiment analysis
def analyze_sentiment(comment):
    try:
        analysis = TextBlob(comment)
        polarity = analysis.sentiment.polarity
        if polarity > 0:
            return "positive"
        elif polarity < 0:
            return "negative"
        else:
            return "neutral"
    except Exception as e:
        logger.error(f"Sentiment analysis error: {e}")
        return "neutral"

# Worker function for processing a batch
def process_batch(queue, sentiment_counts, lock):
    while True:
        task = queue.get()
        if task is None:
            break  # Exit signal

        records = task
        local_counts = defaultdict(int)

        for record in records:
            try:
                comment = record.get("comment", "")
                if not comment:
                    continue

                # Perform sentiment analysis
                sentiment = analyze_sentiment(comment)
                local_counts[sentiment] += 1
            except Exception as e:
                logger.error(f"Error processing record ID {record.get('_id')}: {e}")

        # Update the shared counts with thread safety
        with lock:
            for sentiment, count in local_counts.items():
                sentiment_counts[sentiment] += count

        queue.task_done()

# Main function for sentiment analysis
def fetch_and_analyze_sentiments(date_from=None, date_to=None):
    try:
        client = pymongo.MongoClient("mongodb://vchoudhary:password@127.0.0.1:27017/jobMarketDB")
        db = client['jobMarketDB']

        sentiment_counts = defaultdict(int)  # {sentiment: count}
        lock = threading.Lock()
        queue = Queue()

        # Start worker threads
        threads = []
        for _ in range(NUM_THREADS):
            thread = threading.Thread(target=process_batch, args=(queue, sentiment_counts, lock))
            thread.start()
            threads.append(thread)

        # Fetch records in batches from the job market collection
        collection = db['4chan_posts_comments']
        last_id = None
        if isinstance(date_from, str):
            date_from = datetime.fromisoformat(date_from)
        if isinstance(date_to, str):
            date_to = datetime.fromisoformat(date_to)

        # Build the date filter query
        date_filter = {}
        if date_from:
            date_filter["$gte"] = date_from.timestamp()
        if date_to:
            date_filter["$lte"] = date_to.timestamp()

        while True:
            batch_query = {}
            if last_id:
                batch_query["_id"] = {"$gt": last_id}

            if date_filter:
                batch_query["timestamp"] = date_filter

            records = list(collection.find(batch_query).sort("_id").limit(BATCH_SIZE))
            if not records:
                break

            queue.put(records)
            last_id = records[-1]["_id"]

        # Signal threads to stop
        queue.join()
        for _ in range(NUM_THREADS):
            queue.put(None)
        for thread in threads:
            thread.join()

        logger.info("Sentiment analysis for job market completed successfully.")
        return sentiment_counts

    except Exception as e:
        logger.error(f"Error during sentiment analysis: {e}")

# Function to generate the bar chart
def generate_bar_chart(sentiment_counts):
    try:
        sentiments = ["positive", "neutral", "negative"]
        sentiment_values = [sentiment_counts.get(sentiment, 0) for sentiment in sentiments]

        plt.bar(sentiments, sentiment_values, color=["green", "gray", "red"])
        plt.xlabel("Sentiments")
        plt.ylabel("Count")
        plt.title("Sentiment Analysis for Job Market")
        plt.tight_layout()

        # Save chart as image
        plt.savefig("img/4chan_job_market_sentiment.png")
        plt.close()
        logger.info("Bar chart saved as '4chan_job_market_sentiment.png'")
    except Exception as e:
        logger.error(f"Error generating bar chart: {e}")

# Main entry point
if __name__ == "__main__":
    logger.info("Starting sentiment analysis for all available data")
    sentiment_counts = fetch_and_analyze_sentiments()
    generate_bar_chart(sentiment_counts)
