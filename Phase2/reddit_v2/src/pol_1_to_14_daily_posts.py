from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt

# Retrieve environment variables
MONGODB_URI = "mongodb://vchoudhary:password@127.0.0.1:27017/jobMarketDB"
MONGODB_DATABASE_NAME = "jobMarketDB"
REDDIT_COLLECTION_NAME = "reddit_posts"

# Connect to MongoDB
client = MongoClient(MONGODB_URI)
db = client[MONGODB_DATABASE_NAME]
collection = db[REDDIT_COLLECTION_NAME]

# Define date range
start_date = "2024-11-01"
end_date = "2024-11-15"

# Query MongoDB for the data in the 'politics' subreddit
query = {
    "subreddit": "politics",
    "utc": {
        "$gte": f"{start_date}T00:00:00Z",
        "$lte": f"{end_date}T23:59:59Z"
    }
}
data = list(collection.find(query))

# Convert data into a DataFrame
df = pd.DataFrame(data)

# Convert utc to datetime with timezone
df['date'] = pd.to_datetime(df['utc'])

# Convert start_date and end_date to timezone-aware
start_date = pd.Timestamp(start_date).tz_localize('UTC')
end_date = pd.Timestamp(end_date).tz_localize('UTC')

# Filter data for the given date range
df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]

# Resample data daily
daily_counts = df.resample('D', on='date').size()

# Generate a complete daily date range
full_range = pd.date_range(start=start_date, end=end_date, freq='D', tz='UTC')

# Reindex to include all days, filling missing values with 0
daily_counts = daily_counts.reindex(full_range, fill_value=0)

# Plot the data
plt.figure(figsize=(14, 7))
plt.plot(daily_counts.index, daily_counts.values, marker='o')

# x-axis to show all dates
plt.title("Number of Submissions per day in r/politics (Nov 1-14, 2024)", fontsize=16)
plt.xlabel("Date", fontsize=14)
plt.ylabel("Number of Submissions", fontsize=14)
plt.xticks(daily_counts.index, [date.strftime('%Y-%m-%d') for date in daily_counts.index], rotation=45)

plt.grid()
plt.tight_layout()

# Save the plot as a PNG file
output_file = "img/pol_comments_per_day_politics.png"
plt.savefig(output_file)
print(f"Plot saved as {output_file}")