import pandas as pd
import locale
from utils import convert_italian_month, downsample_articles, downsample_all_newspapers
import os

locale.setlocale(locale.LC_TIME, 'it_IT.UTF-8')

path = "D:/PycharmProjects/Thesis/data/beta/"
dfs_dict = {}  # Dictionary to store all dataframes

files = os.listdir(path)

for file in files:
    # Process AI4Business articles
    if file == 'ai4business.csv':
        df = pd.read_csv(path + file)
        df.drop(columns='Unnamed: 0', inplace=True)
        df['date'] = df['date'].apply(convert_italian_month)
        df['date'] = pd.to_datetime(df['date'])
        df = df[df['date'].dt.year >= 2022]
        dfs_dict['ai4business'] = df

    # Process AI News articles
    elif file == 'ainews.csv':
        df = pd.read_csv(path + file)
        df.drop(columns=['Unnamed: 0.1', 'Unnamed: 0'], inplace=True)
        df['date'] = df['date'].apply(convert_italian_month)
        df['date'] = pd.to_datetime(df['date'])
        df = df[df['date'].dt.year >= 2022]
        dfs_dict['ainews'] = df

    # Process The Guardian articles
    elif file == 'guardian_ai_articles_with_ai.csv':
        df = pd.read_csv(path + file)
        df['date'] = pd.to_datetime(df['date'])
        df = df[df['date'].dt.year >= 2022]
        dfs_dict['guardian'] = df

    # Process Corriere della Sera articles
    elif file == 'il_corriere_della_sera_with_ai.csv':
        df = pd.read_csv(path + file)
        df.drop(columns='Unnamed: 0', inplace=True)
        df = df.dropna(subset=['date'])
        df['date'] = df['date'].apply(convert_italian_month)
        df['date'] = pd.to_datetime(df['date'])
        df = df[df['date'].dt.year >= 2022]
        dfs_dict['corriere'] = df

    # Process Il Sole 24 Ore articles
    elif file == 'ilsole24_with_ai.csv':
        df = pd.read_csv(path + file)
        df.drop(columns='Unnamed: 0', inplace=True)
        df = df.dropna(subset=['date'])
        df['date'] = df['date'].apply(convert_italian_month)
        df['date'] = pd.to_datetime(df['date'])
        df = df[df['date'].dt.year >= 2022]
        dfs_dict['sole24'] = df

    # Process New York Times articles
    elif file == 'nyt_ai_articles_with_ai.csv':
        df = pd.read_csv(path + file)
        df['date'] = pd.to_datetime(df['date'])
        df = df[df['date'].dt.year >= 2022]
        dfs_dict['nyt'] = df

    # Process Wired Italia articles
    elif file == 'wired_with_ai.csv':
        df = pd.read_csv(path + file)
        df = df.dropna(subset=['date'])
        df['date'] = pd.to_datetime(df['date'], format='%d.%m.%Y')
        df = df[df['date'].dt.year >= 2022]
        dfs_dict['wired'] = df

# Set downsampling parameters
method = 'fixed'  # fixed or 'proportional'
n_articles = 10   # IF method is 'fixed': minimum number of articles per newspaper ELSE: not used

# Apply downsampling to all dataframes
balanced_dfs = downsample_all_newspapers(dfs_dict, method=method, n_articles=n_articles)

# Combine all balanced dataframes into one
combined_df = pd.concat(balanced_dfs.values(), ignore_index=True)

# Save the balanced dataset
output_path = f"D:/PycharmProjects/Thesis/data/full_tests/beta_{method}_{n_articles}.csv"
combined_df.to_csv(output_path, index=False)

# Print statistics for verification
for newspaper in balanced_dfs:
    df = balanced_dfs[newspaper]
    df['year'] = df['date'].dt.year
    df['quarter'] = df['date'].dt.quarter
    result = df.groupby(['year', 'quarter']).size().reset_index(name='count')
    print(f"\n{newspaper.upper()}:")
    print(result)