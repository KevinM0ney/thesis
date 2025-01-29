import polars as pl
import numpy as np
from analysis.utils import create_word_contingency_table
import plotly.graph_objects as go
from sklearn.decomposition import TruncatedSVD
import plotly.express as px

path = 'D:/PycharmProjects/Thesis/data/to_analyze/first_test_italiano.csv'

# Read the contingency table
# df = create_word_contingency_table(path)
df = pl.read_csv('top_100_words_frequencies.csv', separator=';')
# Calculate total words per period and relative frequencies
df_with_totals = df.with_columns(
    total_words=pl.fold(
        acc=pl.lit(0),
        function=lambda acc, x: acc + x,
        exprs=[pl.col(col) for col in df.columns if col != 'period']
    )
)

# Calculate relative frequencies
rel_freq_df = df_with_totals.with_columns([
    pl.col(col) / pl.col('total_words')
    for col in df.columns
    if col not in ['period', 'total_words']
])

# Get top 10 words for each period
word_columns = [col for col in rel_freq_df.columns if col not in ['period', 'total_words']]

# Get top words for each period and prepare data for correspondence analysis
top_words_by_period = {}
contingency_matrix = []
periods = []

for row in rel_freq_df.iter_rows(named=True):
    period = row['period']
    periods.append(period)

    # Get frequencies for all words
    word_freqs = {col: row[col] for col in word_columns}

    # Sort by frequency and get top 10
    top_words = sorted(word_freqs.items(), key=lambda x: x[1], reverse=True)[:10]
    top_words_by_period[period] = top_words

    # Add frequencies to contingency matrix
    contingency_matrix.append([freq for word, freq in top_words])

# Convert to numpy array for SVD
contingency_matrix = np.array(contingency_matrix)

# Center the data
row_means = contingency_matrix.mean(axis=1, keepdims=True)
col_means = contingency_matrix.mean(axis=0, keepdims=True)
centered_matrix = contingency_matrix - row_means - col_means + contingency_matrix.mean()

# Perform SVD
n_components = 5  # We'll plot in 2D
svd = TruncatedSVD(n_components=n_components)
coords = svd.fit_transform(centered_matrix)

# Create the plot
fig = go.Figure()

# Plot periods (rows)
fig.add_trace(go.Scatter(
    x=coords[:, 0],
    y=coords[:, 1],
    mode='markers+text',
    marker=dict(size=10, color='blue'),
    text=periods,
    name='Periods',
    textposition="top center"
))

# Get word coordinates (columns)
word_coords = svd.components_.T
scaled_word_coords = word_coords * np.sqrt(svd.singular_values_)

# Plot words
words = [word for period_words in top_words_by_period.values() for word, _ in period_words]
fig.add_trace(go.Scatter(
    x=scaled_word_coords[:, 0],
    y=scaled_word_coords[:, 1],
    mode='markers+text',
    marker=dict(size=8, color='red'),
    text=words,
    name='Words',
    textposition="top center"
))

# Update layout
fig.update_layout(
    title='Correspondence Analysis: Periods and Top Words',
    xaxis_title=f'Dimension 1 ({svd.explained_variance_ratio_[0]:.2%} explained variance)',
    yaxis_title=f'Dimension 2 ({svd.explained_variance_ratio_[1]:.2%} explained variance)',
    showlegend=True,
    width=1000,
    height=800
)

# Show the plot
fig.show()

# Print top words for each period
print("\nTop 10 words by period:")
for period, words in top_words_by_period.items():
    print(f"\n{period}:")
    for word, freq in words:
        print(f"  {word}: {freq:.4f}")