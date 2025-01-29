import pandas as pd

dataset = "beta_proportional"
df = pd.read_csv(f"D:/PycharmProjects/Thesis/data/full_tests/{dataset}.csv")

df.drop(columns=['topic', 'snippet', 'author'], inplace=True)

for column in df.columns:
    print(f"{column}: {df[column].isna().sum()}")

df.to_csv(f"D:/PycharmProjects/Thesis/data/full_tests/cleaned/{dataset}_no_topic_snippet_author.csv")
