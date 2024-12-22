from fastapi import FastAPI, Query
from typing import Optional
from fastapi.middleware.cors import CORSMiddleware
from sentiment_analysis_bar_chart import fetch_and_analyze_sentiments
from subreddit_data_analysis_horizontal_bar import fetch_and_count_data
from toxicity_class_analysis_comments import toxicity_data_analysis

app = FastAPI()

origins = [
    "http://localhost:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get('/testconnection')
def testConnections():
    """
    Test Connection for API.
    """
    test_ok={"status":200}
    return test_ok

@app.get("/sentimentsReddit")
def get_sentiments(
    subreddit: Optional[str] = Query(None, description="Filter by subreddit name"),
    from_date: Optional[str] = Query(None, description="Start date in ISO format (YYYY-MM-DD)"),
    to_date: Optional[str] = Query(None, description="End date in ISO format (YYYY-MM-DD)")
):
    sentiments = fetch_and_analyze_sentiments(subreddit=subreddit, date_from=from_date, date_to=to_date)
    return sentiments

@app.get("/countsReddit")
def get_counts():
    counts = fetch_and_count_data()
    return counts

@app.get("/toxicityReddit")
def get_toxicity(
    subreddit: Optional[str] = Query(None, description="Filter by subreddit name")
):
    toxicity = toxicity_data_analysis(subreddit=subreddit)
    return toxicity
