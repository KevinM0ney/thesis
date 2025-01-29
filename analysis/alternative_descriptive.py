import polars as pl
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
from itertools import combinations
import numpy as np
from collections import defaultdict


def load_and_prepare_data(filepath: str) -> pl.DataFrame:
    """
    Load and prepare the dataset for analysis
    """
    print("Loading dataset...")
    df = pl.read_csv(filepath)
    return df


def create_word_frequency_analysis(df: pl.DataFrame):
    """
    Create word frequency analysis by language and newspaper
    """
    # Overall word frequency
    overall_freq = (df.group_by('word')
                    .agg(pl.len()
                         .alias('count'))
                    .sort('count', descending=True)
                    .head(20))

    # Word frequency by language
    freq_by_lang = (df.group_by(['word', 'language'])
                    .agg(pl.len()
                         .alias('count'))
                    .sort('count', descending=True)
                    .filter(pl.col('count') >= 10))

    # Split by language
    en_words = freq_by_lang.filter(pl.col('language') == 'en').head(20)
    it_words = freq_by_lang.filter(pl.col('language') == 'it').head(20)

    return overall_freq, en_words, it_words


def create_visualizations(df: pl.DataFrame, overall_freq, en_words, it_words):
    """
    Create visualizations for word statistics
    """
    # Create subplot figure
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=("Overall Top Words", "Top English Words",
                        "Top Italian Words", "Words per Newspaper"),
        specs=[[{"type": "bar"}, {"type": "bar"}],
               [{"type": "bar"}, {"type": "pie"}]]
    )

    # Overall top words
    fig.add_trace(
        go.Bar(x=overall_freq['word'].to_list(),
               y=overall_freq['count'].to_list(),
               name="Overall"),
        row=1, col=1
    )

    # Top English words
    fig.add_trace(
        go.Bar(x=en_words['word'].to_list(),
               y=en_words['count'].to_list(),
               name="English"),
        row=1, col=2
    )

    # Top Italian words
    fig.add_trace(
        go.Bar(x=it_words['word'].to_list(),
               y=it_words['count'].to_list(),
               name="Italian"),
        row=2, col=1
    )

    # Distribution by newspaper
    words_per_newspaper = (df.group_by('newspaper')
                           .agg(pl.len()
                                .alias('count'))
                           .sort('count', descending=True))

    fig.add_trace(
        go.Pie(labels=words_per_newspaper['newspaper'].to_list(),
               values=words_per_newspaper['count'].to_list(),
               name="By Newspaper"),
        row=2, col=2
    )

    # Update layout
    fig.update_layout(height=1000, width=1200, title_text="Word Analysis")
    fig.update_xaxes(tickangle=45)

    # Create temporal analysis
    temporal_fig = create_temporal_analysis(df)

    return fig, temporal_fig


def create_temporal_analysis(df: pl.DataFrame) -> go.Figure:
    """
    Create temporal analysis of word usage
    """
    # Group by year and quarter
    temporal = (df.group_by(['year', 'quarter', 'newspaper'])
                .agg(pl.len().alias('count')))

    # Create figure
    fig = go.Figure()

    # Add line for each newspaper
    for newspaper in df['newspaper'].unique().to_list():
        newspaper_data = temporal.filter(pl.col('newspaper') == newspaper)
        fig.add_trace(go.Scatter(
            x=[f"{year}Q{quarter}" for year, quarter in zip(newspaper_data['year'], newspaper_data['quarter'])],
            y=newspaper_data['count'],
            name=newspaper,
            mode='lines+markers'
        ))

    fig.update_layout(
        title="Word Usage Over Time by Newspaper",
        xaxis_title="Time Period",
        yaxis_title="Number of Words",
        height=600,
        width=1200
    )

    return fig


def analyze_word_length(df: pl.DataFrame) -> tuple[pl.DataFrame, go.Figure]:
    """
    Analyze word length distribution by language and newspaper
    """
    # Add word length column
    df_with_length = df.with_columns(
        pl.col('word').str.len_chars().alias('word_length')
    )

    # Average word length by newspaper and language
    avg_length = (df_with_length.group_by(['newspaper', 'language'])
                  .agg(pl.col('word_length').mean().alias('avg_length'),
                       pl.col('word_length').std().alias('std_length'))
                  .sort('avg_length', descending=True))

    # Create visualization
    fig = go.Figure()

    for language in df['language'].unique().to_list():
        lang_data = df_with_length.filter(pl.col('language') == language)

        fig.add_trace(go.Histogram(
            x=lang_data['word_length'].to_list(),
            name=language,
            opacity=0.7,
            nbinsx=20
        ))

    fig.update_layout(
        title="Word Length Distribution by Language",
        xaxis_title="Word Length",
        yaxis_title="Count",
        barmode='overlay'
    )

    return avg_length, fig


def analyze_word_cooccurrence(df: pl.DataFrame, min_count: int = 5) -> pl.DataFrame:
    """
    Analyze word co-occurrences within the same newspaper and time period
    """
    cooccurrence = defaultdict(int)

    # Group by newspaper, year, and quarter
    groups = df.group_by(['newspaper', 'year', 'quarter']).agg(
        pl.col('word').alias('words')
    )

    # Analyze co-occurrences
    for group in groups.iter_rows():
        words = group[3]  # words column
        # Get unique combinations of words
        for word1, word2 in combinations(set(words), 2):
            if word1 < word2:  # ensure consistent ordering
                cooccurrence[(word1, word2)] += 1

    # Convert to DataFrame and filter by minimum count
    cooc_df = pl.DataFrame({
        'word1': [pair[0] for pair in cooccurrence.keys()],
        'word2': [pair[1] for pair in cooccurrence.keys()],
        'count': list(cooccurrence.values())
    }).filter(pl.col('count') >= min_count)

    return cooc_df.sort('count', descending=True)


def analyze_seasonal_trends(df: pl.DataFrame) -> tuple[pl.DataFrame, go.Figure]:
    """
    Analyze seasonal trends in word usage
    """
    # Calculate quarterly averages
    seasonal = (df.group_by(['quarter', 'language'])
                .agg(pl.len().alias('count'))
                .sort(['language', 'quarter']))

    # Create visualization
    fig = go.Figure()

    for language in df['language'].unique().to_list():
        lang_data = seasonal.filter(pl.col('language') == language)

        fig.add_trace(go.Scatter(
            x=lang_data['quarter'].to_list(),
            y=lang_data['count'].to_list(),
            name=language,
            mode='lines+markers'
        ))

    fig.update_layout(
        title="Seasonal Word Usage Patterns",
        xaxis_title="Quarter",
        yaxis_title="Average Word Count"
    )

    return seasonal, fig


def calculate_lexical_similarity(df: pl.DataFrame) -> tuple[pl.DataFrame, go.Figure]:
    """
    Calculate lexical similarity between newspapers using Jaccard similarity
    """
    newspapers = df['newspaper'].unique().to_list()
    similarity_matrix = np.zeros((len(newspapers), len(newspapers)))

    # Calculate word sets for each newspaper
    newspaper_words = {
        newspaper: set(df.filter(pl.col('newspaper') == newspaper)['word'].to_list())
        for newspaper in newspapers
    }

    # Calculate Jaccard similarity
    for i, newspaper1 in enumerate(newspapers):
        for j, newspaper2 in enumerate(newspapers):
            if i <= j:
                words1 = newspaper_words[newspaper1]
                words2 = newspaper_words[newspaper2]
                similarity = len(words1 & words2) / len(words1 | words2)
                similarity_matrix[i, j] = similarity
                similarity_matrix[j, i] = similarity

    # Create heatmap
    fig = go.Figure(data=go.Heatmap(
        z=similarity_matrix,
        x=newspapers,
        y=newspapers,
        colorscale='Viridis'
    ))

    fig.update_layout(
        title="Lexical Similarity Between Newspapers",
        xaxis_title="Newspaper",
        yaxis_title="Newspaper"
    )

    # Create DataFrame with similarity scores
    similarity_df = pl.DataFrame(
        similarity_matrix,
        schema=newspapers,
    ).with_columns(pl.Series(name='newspaper', values=newspapers))

    return similarity_df, fig


def main():
    # Load data
    filepath = ("D:/PycharmProjects/Thesis/data/to_analyze/"
                "beta_proportional_no_topic_snippet_author_words_fixed_lemmatized.csv")
    df = load_and_prepare_data(filepath)

    # Define output directories
    output_dir = "D:/PycharmProjects/Thesis/data/to_analyze/figures/"
    stats_dir = "D:/PycharmProjects/Thesis/data/to_analyze/statistics/"

    # Basic word frequency analysis
    overall_freq, en_words, it_words = create_word_frequency_analysis(df)
    fig, temporal_fig = create_visualizations(df, overall_freq, en_words, it_words)

    # Save basic visualizations
    fig.write_html(output_dir + "word_analysis.html")
    temporal_fig.write_html(output_dir + "temporal_analysis.html")

    # Save basic statistics
    overall_freq.write_csv(stats_dir + "overall_word_frequency.csv")
    en_words.write_csv(stats_dir + "english_word_frequency.csv")
    it_words.write_csv(stats_dir + "italian_word_frequency.csv")

    # Word length analysis
    avg_length, length_fig = analyze_word_length(df)
    length_fig.write_html(output_dir + "word_length_analysis.html")
    avg_length.write_csv(stats_dir + "word_length_statistics.csv")

    # Co-occurrence analysis
    cooccurrence = analyze_word_cooccurrence(df, min_count=10)
    cooccurrence.write_csv(stats_dir + "word_cooccurrence.csv")

    # Seasonal trends
    seasonal, seasonal_fig = analyze_seasonal_trends(df)
    seasonal_fig.write_html(output_dir + "seasonal_trends.html")
    seasonal.write_csv(stats_dir + "seasonal_statistics.csv")

    # Lexical similarity
    similarity, similarity_fig = calculate_lexical_similarity(df)
    similarity_fig.write_html(output_dir + "lexical_similarity.html")
    similarity.write_csv(stats_dir + "newspaper_similarity.csv")

    # Print all summary statistics
    print("\nSummary Statistics:")
    print(f"Total number of words: {len(df)}")
    print("\nWords per language:")
    print(df.group_by('language').agg(pl.len()))
    print("\nWords per newspaper:")
    print(df.group_by('newspaper').agg(pl.len()))

    print("\nWord Length Statistics:")
    print(avg_length)

    print("\nTop Co-occurring Words:")
    print(cooccurrence.head(10))

    print("\nSeasonal Patterns:")
    print(seasonal)


if __name__ == "__main__":
    main()