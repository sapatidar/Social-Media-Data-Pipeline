import { useState, useEffect } from "react";
import SentimentChart4Chan from "./SentimentChart4Chan";
import SentimentChartReddit from "./SentimentChartReddit";
import SubRedditCounts from "./SubRedditCounts";

export default function Home() {
  const [connectionStatus, setConnectionStatus] = useState(0); // 0: Loading, 1: Connected, -1: Failed

  useEffect(() => {  
    const checkAPIConnection = async () => {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => {
        controller.abort(); // Abort the fetch request after 5 seconds
      }, 10000);

      try {
        console.log("Request: /testconnection");
        const response = await fetch("http://127.0.0.1:8000/testconnection", { signal: controller.signal });
        
        clearTimeout(timeoutId);

        if (response.ok) {
          await response.json();
          setConnectionStatus(1);
        } else {
          setConnectionStatus(-1);
        }
      } catch (error) {
        clearTimeout(timeoutId);

        if (error.name === "AbortError") {
          console.error("Connection request timed out.");
        } else {
          console.error("Error connecting backend API:", error);
        }
        setConnectionStatus(-1);
      }
    };

    checkAPIConnection();
  }, []);

  const renderContent = () => {
    switch (connectionStatus) {
      case 0:
        return (
          <div className="error-msg">
            <div className="app-loader">
              <div className="loader"></div>
              Loading...
            </div>
            Checking API connection...
          </div>
        );
      case -1:
        return (
          <div className="error-msg">
            Unable to connect to API. Please ensure the API service is running in backend.
          </div>
        );
      case 1:
        return (
          <div style={{padding:30}}>
            <h1 style={{padding:30, justifySelf:'center'}}>Job Market Sentiment Analysis</h1>
            <div className="chart-container">
              <div className='items' style={{ width: "600px", margin: "0 auto" }}> <SentimentChart4Chan /></div>
              <div className='items' style={{ width: "600px", margin: "0 auto" }}><SentimentChartReddit /></div>
              <div className='items' style={{ width: "600px", margin: "0 auto" }}><SubRedditCounts/></div>
            </div>
          </div>
        );
      default:
        return null;
    }
  };

  return renderContent();
}
