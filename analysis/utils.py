import polars as pl
from collections import Counter


def create_word_contingency_table(filepath):
    """
    Creates a word contingency table from a CSV file.

    Parameters:
    filepath (str): Path to the CSV file

    Returns:
    polars.DataFrame: Contingency table with periods as rows and words as columns
    """
    # Read the CSV file
    df = pl.read_csv(filepath)

    # Create period column
    df = df.with_columns(
        (pl.col('year').cast(pl.Utf8) + ' - ' + pl.col('quarter').cast(pl.Utf8)).alias('period')
    )

    # Create contingency table
    contingency_table = (
        df
        # Explode text into individual words and convert to lowercase
        .with_columns([
            pl.col('word').str.to_lowercase().str.split(' ').alias('words')
        ])
        .explode('words')
        # Group by period and word, then count occurrences
        .group_by(['period', 'words'])
        .agg(
            pl.count().alias('count')
        )
        # Pivot to create the contingency table
        .pivot(
            values='count',
            index='period',
            columns='words',
            aggregate_function='sum'
        )
        .fill_null(0)
    )

    return contingency_table