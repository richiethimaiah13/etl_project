# Movie ETL Project

This project is an **ETL (Extract, Transform, Load)** process that reads movie titles from a CSV file, fetches detailed movie information from the **OMDb API**, and loads the data into a **PostgreSQL** database. The data transformation ensures clean and valid data before it is loaded into the database.

## Features

- **Extract**: Fetch movie details from the OMDb API using the movie titles from a CSV file.
- **Transform**: Clean and process the data (e.g., handle missing or incorrect values).
- **Load**: Insert the data into a PostgreSQL database.

## Prerequisites

Before running the project, ensure you have the following:

- **Python 3.x**: The script is written in Python.
- **PostgreSQL**: Local or remote PostgreSQL server where the data will be stored.
- **OMDb API Key**: Obtain an API key by signing up at [OMDb API](http://www.omdbapi.com/apikey.aspx).
