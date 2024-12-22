import React, { useEffect, useState } from 'react';
import { Bar } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend
} from 'chart.js';

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend
);

function SentimentChart4Chan() {
  const [data, setData] = useState({ positive: 0, neutral: 0, negative: 0 });
  const [loading, setLoading] = useState(false);
  
  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");
  
  // Fetch data with an optional date range
  const fetchData = (start = "", end = "") => {
    let url = 'http://127.0.0.1:8000/sentiments';
    const params = new URLSearchParams();
    if (start) params.append('startDate', start);
    if (end) params.append('endDate', end);
    
    if (params.toString()) {
      url += `?${params.toString()}`;
    }

    console.log("Request:", url);
    setLoading(true);
    fetch(url)
      .then(response => response.json())
      .then(json => {
        setData(json);
        setLoading(false);
      })
      .catch((error) => {
        console.error("Error fetching sentiments:", error);
        setLoading(false);
      });
  };

  useEffect(() => {
    // Initial fetch without date filters
    // fetchData();
  }, []);

  const handleFilter = () => {
    fetchData(startDate, endDate);
  };

  const chartData = {
    labels: ['Positive', 'Neutral', 'Negative'],
    datasets: [
      {
        label: 'Sentiment Counts',
        data: [data.positive, data.neutral, data.negative],
        backgroundColor: ['green', 'gray', 'red']
      },
    ],
  };

  const options = {
    responsive: true,
    plugins: {
      legend: { position: 'top' },
      title: {
        display: true,
        text: '4Chan Sentiment Analysis for Job Market',
      },
    },
  };

  return (
    <div>
      <h4 className='p1'>4Chan Sentiment Analysis for Job Market</h4>
      <div style={{ marginBottom: '1em' }}>
        <label style={{ marginRight: '1em' }}>
          Start Date: 
          <input 
            className='input-date'
            type="date" 
            value={startDate}
            onChange={e => setStartDate(e.target.value)}
            style={{ marginLeft: '0.5em' }}
          />
        </label>
        <label style={{ marginRight: '1em' }}>
          End Date:
          <input 
            className='input-date'
            type="date" 
            value={endDate}
            onChange={e => setEndDate(e.target.value)}
            style={{ marginLeft: '0.5em' }}
          />
        </label>
        <button className='filter-button' onClick={handleFilter}>Filter</button>
      </div>

      {loading && <div className="chart-loader"><div className="chart-loader-spin"></div>Loading...</div>}
      <Bar data={chartData} options={options} height="200px" width="300px"/>
    </div>
  );
}

export default SentimentChart4Chan;
