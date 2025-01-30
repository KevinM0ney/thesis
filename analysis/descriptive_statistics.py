import polars as pl
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime


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
                    .filter(pl.col('count') >= 10))  # Filter words appearing at least 10 times

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


def main():
    # Load data
    filepath = ("D:/PycharmProjects/Thesis/data/to_analyze/"
                "first_test.csv")
    df = load_and_prepare_data(filepath)

    # Create frequency analysis
    overall_freq, en_words, it_words = create_word_frequency_analysis(df)

    # Create visualizations
    fig, temporal_fig = create_visualizations(df, overall_freq, en_words, it_words)

    # Save figures
    output_dir = "D:/PycharmProjects/Thesis/data/to_analyze/figures/"
    fig.write_html(output_dir + "word_analysis.html")
    temporal_fig.write_html(output_dir + "temporal_analysis.html")

    # Print summary statistics
    print("\nSummary Statistics:")
    print(f"Total number of words: {len(df)}")
    print("\nWords per language:")
    print(df.group_by('language').agg(pl.len()))
    print("\nWords per newspaper:")
    print(df.group_by('newspaper').agg(pl.len()))

    # Save detailed statistics to CSV
    stats_dir = "D:/PycharmProjects/Thesis/data/to_analyze/statistics/"
    overall_freq.write_csv(stats_dir + "overall_word_frequency.csv")
    en_words.write_csv(stats_dir + "english_word_frequency.csv")
    it_words.write_csv(stats_dir + "italian_word_frequency.csv")


if __name__ == "__main__":
    main()