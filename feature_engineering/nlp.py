import pandas as pd
import spacy
from tqdm import tqdm
import warnings
from utils import GLOSSARY
from typing import Set, List
import polars as pl

warnings.filterwarnings("ignore")

# Load both English and Italian models
print("Loading language models...")
nlp_en = spacy.load("en_core_web_sm")
nlp_it = spacy.load("it_core_news_sm")

# Define English and Italian newspapers
ENGLISH_NEWSPAPERS = ['guardian', 'nyt']
ITALIAN_NEWSPAPERS = ['ai4business', 'ainews', 'corriere', 'sole24', 'wired']


def get_language(newspaper):
    return 'en' if newspaper in ENGLISH_NEWSPAPERS else 'it'


def process_title(title, language='en'):
    """
    Process title using Spacy to properly handle named entities and tokenization
    """
    # Choose the appropriate language model
    nlp = nlp_en if language == 'en' else nlp_it

    # Process the title
    doc = nlp(title)

    tokens = []
    skip_tokens = set()  # Keep track of tokens that are part of entities

    # First, process named entities
    for ent in doc.ents:
        # Add the entity as a single token
        tokens.append('_'.join([t.text for t in ent]).lower())
        # Mark these tokens to skip in the regular processing
        for token in ent:
            skip_tokens.add(token.i)

    # Then process remaining tokens
    for token in doc:
        if token.i not in skip_tokens:  # Only process if not part of an entity
            if (not token.is_punct and
                    not token.is_space and
                    not token.is_stop and  # Skip punctuation, spaces, and stopwords
                    len(token.text) > 1 and  # Skip single letters
                    not token.text.isdigit()):  # Skip numbers
                tokens.append(token.text.lower())

    return tokens


def parse_dates(df):
    """
    Parse dates in a robust way handling multiple formats
    """
    # First, try to parse with multiple formats in sequence
    date_formats = [
        'ISO8601',
        '%Y-%m-%d %H:%M:%S%z',  # Format with timezone offset
        '%Y-%m-%d %H:%M:%S',  # Format without timezone
        '%Y-%m-%d'  # Just date
    ]

    for fmt in date_formats:
        try:
            if fmt == 'ISO8601':
                return pd.to_datetime(df['date'], format='ISO8601').dt.tz_localize(None)
            else:
                return pd.to_datetime(df['date'], format=fmt)
        except:
            continue

    # If all specific formats fail, try the mixed approach
    try:
        return pd.to_datetime(df['date'], format='mixed').dt.tz_localize(None)
    except:
        # Last resort: force parsing with coerce to handle problematic dates
        return pd.to_datetime(df['date'], errors='coerce')


def expand_titles_to_words(df):
    """
    Expand the dataframe so each word gets its own row
    """
    expanded_data = []

    for _, row in tqdm(df.iterrows(), total=len(df), desc="Processing titles"):
        language = get_language(row['newspaper'])
        try:
            tokens = process_title(row['title'], language)
            for token in tokens:
                new_row = {
                    'date': row['date'],
                    'newspaper': row['newspaper'],
                    'year': row['year'],
                    'quarter': row['quarter'],
                    'word': token,
                    'language': language
                }
                expanded_data.append(new_row)
        except Exception as e:
            print(f"Error processing title: {row['title']}")
            print(f"Error: {str(e)}")
            continue

    return pd.DataFrame(expanded_data)


def merge_consecutive_words(df_words: pd.DataFrame) -> pd.DataFrame:
    """
    Check for consecutive words that should be merged based on the glossary using Polars
    """
    print("\nChecking for consecutive words to merge...")

    # Convert pandas DataFrame to Polars
    print("Converting DataFrame to Polars...")
    df = pl.from_pandas(df_words)

    # Get total number of groups for progress tracking
    total_groups = len(df.group_by(['newspaper', 'date']).count())
    print(f"Found {total_groups} groups to process")

    def process_group(group: pl.DataFrame) -> tuple[List[str], Set[int], Set[str]]:
        words = group['word'].to_list()
        indices_to_drop = set()
        terms_found = set()  # Keep track of found terms for debugging

        # Process each glossary term
        for term in GLOSSARY:
            term_words = term.lower().split()
            if len(term_words) <= 1:
                continue

            # Check for matches in the word sequence
            for i in range(len(words) - len(term_words) + 1):
                if i in indices_to_drop:
                    continue

                # Check if sequence matches
                if all(words[i + j] == term_word
                       for j, term_word in enumerate(term_words)):
                    # Replace first word with merged term
                    words[i] = '_'.join(term_words)
                    # Mark indices for deletion
                    indices_to_drop.update(range(i + 1, i + len(term_words)))
                    terms_found.add(term)

        return words, indices_to_drop, terms_found

    # Process each group of rows with same newspaper and date
    results = []
    total_merged = 0
    found_terms = set()

    # Create progress bar for groups
    groups = df.partition_by(['newspaper', 'date'])
    pbar = tqdm(groups, total=total_groups, desc="Processing groups")

    for group in pbar:
        # Get group info for progress display
        newspaper = group['newspaper'].head(1).item()
        date = group['date'].head(1).item()
        pbar.set_postfix_str(f"Processing {newspaper} - {date}")

        words, indices_to_drop, terms = process_group(group)
        found_terms.update(terms)

        # Create new rows excluding dropped indices
        for i, word in enumerate(words):
            if i not in indices_to_drop:
                results.append({
                    'date': group['date'].head(1).item(),
                    'newspaper': group['newspaper'].head(1).item(),
                    'year': group['year'].head(1).item(),
                    'quarter': group['quarter'].head(1).item(),
                    'word': word,
                    'language': group['language'].head(1).item()
                })

        total_merged += len(indices_to_drop)

        # Update progress bar description periodically
        if total_merged % 100 == 0:
            pbar.set_description(f"Merged {total_merged} words so far")

    # Print summary statistics
    print(f"\nMerged {total_merged} words based on glossary terms")
    if found_terms:
        print(f"Found {len(found_terms)} different terms from glossary:")
        for term in sorted(found_terms):
            print(f"  - {term}")
    else:
        print("No glossary terms were found in the dataset")

    # Convert results back to DataFrame
    print("\nConverting results back to pandas DataFrame...")
    result_df = pl.DataFrame(results).to_pandas()

    print(f"Final dataset has {len(result_df)} rows")
    return result_df


def print_statistics(df_words):
    """
    Print statistics about the processed words
    """
    print("\nTotal number of words:", len(df_words))
    print("\nWords by language:")
    print(df_words.groupby('language').size())
    print("\nSample of processed words:")
    print(df_words.head(10))

    # Print most common words per newspaper, separated by language
    print("\nMost common words in English newspapers:")
    for newspaper in ENGLISH_NEWSPAPERS:
        if newspaper in df_words['newspaper'].unique():
            print(f"\n{newspaper.upper()}:")
            word_counts = df_words[df_words['newspaper'] == newspaper]['word'].value_counts().head(10)
            print(word_counts)

    print("\nMost common words in Italian newspapers:")
    for newspaper in ITALIAN_NEWSPAPERS:
        if newspaper in df_words['newspaper'].unique():
            print(f"\n{newspaper.upper()}:")
            word_counts = df_words[df_words['newspaper'] == newspaper]['word'].value_counts().head(10)
            print(word_counts)


def main():
    dataset = "beta_proportional_no_topic_snippet_author"

    # Load the balanced dataset
    print("Loading dataset...")
    input_path = f"D:/PycharmProjects/Thesis/data/full_tests/cleaned/{dataset}.csv"
    df = pd.read_csv(input_path)

    # Print sample of dates before parsing
    print("\nSample of original dates:")
    print(df['date'].head())

    # Parse dates
    print("\nParsing dates...")
    df['date'] = parse_dates(df)

    # Print sample of parsed dates
    print("\nSample of parsed dates:")
    print(df['date'].head())

    # Check for any NaT (Not a Time) values
    nat_count = df['date'].isna().sum()
    if nat_count > 0:
        print(f"\nWarning: Found {nat_count} invalid dates")

    # Process the dataframe
    print("Processing titles...")
    df_words = expand_titles_to_words(df)
    df_words = merge_consecutive_words(df_words)

    # Save the result
    print("Saving results...")
    output_path = f"D:/PycharmProjects/Thesis/data/full_tests/ready/{dataset}_words_fixed.csv"
    df_words.to_csv(output_path, index=False)

    # Print statistics
    print_statistics(df_words)


if __name__ == "__main__":
    main()