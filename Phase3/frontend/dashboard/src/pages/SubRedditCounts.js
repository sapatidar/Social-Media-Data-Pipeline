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

function SubRedditCounts() {
  const [data, setData] = useState({});
  const [loading, setLoading] = useState(false);

  const fetchData = () => {
    const url = 'http://127.0.0.1:8001/countsReddit';

    console.log("Request:", url);
    setLoading(true);
    fetch(url)
      .then(response => response.json())
      .then(json => {
        setData(json);
        setLoading(false);
      })
      .catch((error) => {
        console.error("Error fetching subreddit counts:", error);
        setLoading(false);
      });
  };

  useEffect(() => {
    // Fetch data when the component loads
    fetchData();
  }, []);

  const chartData = {
    labels: Object.keys(data),
    datasets: [
      {
        label: 'Subreddit Post Counts',
        data: Object.values(data),
        backgroundColor: ['#36a2eb', '#ff6384', '#ff9f40', '#4bc0c0', '#9966ff', '#ffcd56'],
      },
    ],
  };

  const options = {
    responsive: true,
    plugins: {
      legend: { position: 'top' },
      title: {
        display: true,
        text: 'Subreddit Post Counts',
      },
    },
  };

  return (
    <div>
      <h4 className='p1'>Subreddit Counts</h4>
      {loading && 
        <div className="chart-loader"><div className="chart-loader-spin"></div>Loading...</div>
      }
        <Bar data={chartData} options={options} height="200px" width="300px" />
     
    </div>
  );
}

export default SubRedditCounts;
