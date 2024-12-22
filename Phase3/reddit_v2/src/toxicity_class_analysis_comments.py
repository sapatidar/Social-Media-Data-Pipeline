import pymongo
import pandas as pd
import matplotlib.pyplot as plt
from concurrent.futures import ThreadPoolExecutor
from logger_setup import setup_logger

# Setup logger
logger = setup_logger("reddit_analysis", log_file='logs/reddit_analysis.log', max_bytes=10*1024*1024, backup_count=5)

def process_chunk(chunk):
    processed_data = []
    for record in chunk:
        processed_data.append({
            "date": pd.to_datetime(record["utc"]).date(),
            "is_normal": record.get("toxicity", {}).get("class") != "flag"
        })
    logger.info("Chunk processed")
    return processed_data

def chunk_generator(cursor, chunk_size):
    chunk = []
    for record in cursor:
        chunk.append(record)
        if len(chunk) == chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk

def toxicity_data_analysis(subreddit=None):
    """Main method to handle flow."""
    # Setup logger
    logger = setup_logger("reddit_analysis", log_file='logs/reddit_analysis.log', max_bytes=10*1024*1024, backup_count=5)

    # Setup MongoDB client
    client = pymongo.MongoClient("mongodb://vchoudhary:password@127.0.0.1:27017/jobMarketDB")
    db = client['jobMarketDB']
    collection = db['reddit_comments']

    # Query setup
    query = {"subreddit": {"$ne": "politics"}}  # Default query excludes 'politics'
    if subreddit:
        query = {"subreddit": subreddit}  # Override query to filter by specific subreddit

    cursor = collection.find(
        query,
        {"utc": 1, "toxicity.class": 1}
    )

    # Process data in chunks
    chunk_size = 10000
    data = []
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_chunk, chunk) for chunk in chunk_generator(cursor, chunk_size)]
        for future in futures:
            data.extend(future.result())

    # Convert to DataFrame and aggregate
    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"])

    aggregated = df.groupby("date").is_normal.value_counts().unstack(fill_value=0)
    aggregated.rename(columns={True: "Normal", False: "Flagged"}, inplace=True)

    return aggregated.to_dict()
