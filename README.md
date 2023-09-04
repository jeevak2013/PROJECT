# YouTube Data Analysis

![YouTube](https://img.shields.io/badge/YouTube-Data%20Analysis-red)

This Python project enables you to collect, store, and analyze data from multiple YouTube channels. It uses the YouTube Data API, MongoDB, SQLite, and Streamlit to provide an interactive interface for data exploration.

## Features

- Collect channel information, playlists, videos, and comments using YouTube Data API.
- Store data in MongoDB and SQLite databases.
- Analyze data using predefined SQL queries.
- Visualize results with tables and charts in a Streamlit dashboard.
- Multithreading for faster data processing.
- Error handling for robustness.

## Getting Started

1. Clone this repository.
2. Install required libraries: `pip install -r requirements.txt`.
3. Configure API keys and database connection strings in the code.
4. Run the script: `python main.py`.

## Usage

1. Input YouTube channel names in the Streamlit dashboard.
2. Choose from various analysis options.
3. Explore channel statistics, most viewed videos, and more.
4. Execute SQL queries to gain insights.

## Dependencies

- `googleapiclient`: To interact with the YouTube Data API.
- `pymongo`: For MongoDB integration.
- `sqlite3`: For SQLite database handling.
- `pandas`, `matplotlib`, `streamlit`: Data visualization and web interface.

## Acknowledgments

- YouTube Data API Documentation: https://developers.google.com/youtube/registering_an_application
- Streamlit Documentation: https://docs.streamlit.io/
