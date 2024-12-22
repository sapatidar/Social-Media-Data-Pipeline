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

function SentimentChartReddit() {
  const [data, setData] = useState({});
  const [loading, setLoading] = useState(false);
  
  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");
  const [subReddit, setSubReddit] = useState("");
  
  // Fetch data with an optional date range
  const fetchData = (start = "", end = "", subReddit="") => {
    let url = 'http://127.0.0.1:8001/sentimentsReddit';
    const params = new URLSearchParams();
    if (start) params.append('from_date', start);
    if (end) params.append('to_date', end);
    if(subReddit) params.append('subreddit', subReddit);
    
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
    setData({})
    fetchData(startDate, endDate, subReddit);
  };

  const chartData = subReddit
    ? {
        labels: ['Positive', 'Neutral', 'Negative'],
        datasets: [
          {
            label: 'Sentiment Counts',
            data: [
              data.positive?.[subReddit] || 0,
              data.neutral?.[subReddit] || 0,
              data.negative?.[subReddit] || 0,
            ],
            backgroundColor: ['green', 'gray', 'red'],
          },
        ],
      }
    : {
        labels: Object.keys(data.positive || {}),
        datasets: [
          {
            label: 'Positive',
            data: Object.values(data.positive || {}),
            backgroundColor: 'green',
          },
          {
            label: 'Neutral',
            data: Object.values(data.neutral || {}),
            backgroundColor: 'gray',
          },
          {
            label: 'Negative',
            data: Object.values(data.negative || {}),
            backgroundColor: 'red',
          },
        ],
      };

  const options = {
    responsive: true,
    plugins: {
      legend: { position: 'top' },
      title: {
        display: true,
        text: 'Reddit Sentiment Analysis for Job Market',
      },
    },
    scales: {
      x: {
        stacked: !subReddit, // Enable stacking if no subreddit is selected
      },
      y: {
        stacked: !subReddit, // Enable stacking if no subreddit is selected
      },
    },
  };

  const subReddits=[
    {value:"technology", display:'Technology'},
    {value:"csMajors", display:'Cs Majors'},
    {value:"cscareerquestions", display:'Cs Career Questions'},
    {value:"programming", display:'Programming'},
    {value:"jobs", display:'Jobs'},
    {value:"recruitinghell", display: 'Recruiting Hell'}
  ]

  return (
    <div>
      <h4 className='p1'>Reddit Sentiment Analysis:</h4>
      <div style={{ marginBottom: '1em' }}>
      <label style={{ marginRight: '1em' }}>
          Sub-Reddit: 
          <select 
            className='input-date'
            value={subReddit}
            onChange={e => setSubReddit(e.target.value)}
            style={{ marginLeft: '0.5em' }}
          >
            <option value="">Select a Sub-Reddit</option>
            {
              subReddits.map((obj)=> <option value={obj.value}>{obj.display}</option>)
            }
          </select>
        </label>
        <br/>
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

export default SentimentChartReddit;
