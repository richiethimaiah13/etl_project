import pandas as pd
import requests
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Configuration from environment variables
OMDB_API_KEY = os.getenv("OMDB_API_KEY")
POSTGRES_URI = os.getenv("POSTGRES_URI")
CSV_FILE_PATH = 'movies.csv'

# Read movie titles from the CSV file
def read_movie_titles(csv_file):
    df = pd.read_csv(csv_file)
    return df['title'].tolist()

# Extract movie details using OMDb API
def extract_movies(titles, api_key):
    data = []
    for title in titles:
        response = requests.get(f'http://www.omdbapi.com/?t={title}&apikey={api_key}')
        if response.status_code == 200:
            movie = response.json()
            if movie.get('Response') == 'True':
                data.append({
                    'title': movie.get('Title'),
                    'year': movie.get('Year'),
                    'genre': movie.get('Genre'),
                    'imdb_rating': movie.get('imdbRating')
                })
    return pd.DataFrame(data)

# Transform the data to handle missing/incorrect values
def transform(df):
    df['year'] = pd.to_numeric(df['year'], errors='coerce')
    df['imdb_rating'] = pd.to_numeric(df['imdb_rating'], errors='coerce')
    return df.dropna(subset=['year', 'imdb_rating'])

# Load the data into PostgreSQL
def load(df, db_uri, table_name='movies'):
    engine = create_engine(db_uri)
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"Loaded {len(df)} records into the '{table_name}' table.")

# Run the ETL process
def run_etl():
    print("Reading movie titles from CSV...")
    movie_titles = read_movie_titles(CSV_FILE_PATH)

    print("Extracting data from OMDb API...")
    df_raw = extract_movies(movie_titles, OMDB_API_KEY)

    print("Transforming data...")
    df_clean = transform(df_raw)

    print("Loading data into PostgreSQL...")
    load(df_clean, POSTGRES_URI)

if __name__ == '__main__':
    run_etl()
