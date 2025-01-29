import polars as pl
import spacy
from tqdm import tqdm
import warnings
from collections import Counter
import nltk
from nltk.corpus import stopwords


warnings.filterwarnings("ignore")

# Load both English and Italian models
print("Loading language models...")
nlp_en = spacy.load("en_core_web_sm")
nlp_it = spacy.load("it_core_news_sm")

# Define English and Italian newspapers
ENGLISH_NEWSPAPERS = ['The Guardian', 'The New York Times']
ITALIAN_NEWSPAPERS = ['ai4business', 'ainews', 'corriere', 'sole24', 'wired']

# Pre-cache stopwords
print("Caching stopwords...")
en_stops = set(stopwords.words('english')).union(nlp_en.Defaults.stop_words)

# Custom Italian stopwords including prepositions and articulated prepositions
ITALIAN_BASIC_STOPWORDS = {
    # Articoli determinativi
    "il", "lo", "la", "i", "gli", "le", "l'",

    # Preposizioni articolate (sia con che senza apostrofo)
    "del", "dello", "della", "dei", "degli", "delle", "dell", "dell'",
    "al", "allo", "alla", "ai", "agli", "alle", "all", "all'",
    "dal", "dallo", "dalla", "dai", "dagli", "dalle", "dall", "dall'",
    "nel", "nello", "nella", "nei", "negli", "nelle", "nell", "nell'",
    "sul", "sullo", "sulla", "sui", "sugli", "sulle", "sull", "sull'",

    # Articoli indeterminativi
    "un", "uno", "una", "un'",

    # Preposizioni semplici
    "di", "a", "da", "in", "con", "su", "per", "tra", "fra",

    # Preposizioni articolate
    "del", "dello", "della", "dei", "degli", "delle", "dell'",
    "al", "allo", "alla", "ai", "agli", "alle", "all'",
    "dal", "dallo", "dalla", "dai", "dagli", "dalle", "dall'",
    "nel", "nello", "nella", "nei", "negli", "nelle", "nell'",
    "sul", "sullo", "sulla", "sui", "sugli", "sulle", "sull'",

    # Congiunzioni comuni
    "e", "ed", "o", "ma", "però", "che", "se",

    # Pronomi e dimostrativi
    "mi", "ti", "si", "ci", "vi", "me", "te", "lui", "lei", "noi", "voi", "loro",
    "questo", "questa", "questi", "queste", "quello", "quella", "quelli", "quelle",
    "quest'", "quell'", "cos'",  # Aggiunti per gestire le forme con apostrofo

    # Altre parole comuni da filtrare
    "è", "sono", "essere", "ha", "hanno", "aveva", "avevano",
    "non", "più", "come", "dove", "quando", "perché", "così",
    "ogni", "tutto", "tutti", "tutta", "tutte",
    "suo", "sua", "suoi", "sue", "mio", "mia", "miei", "mie",
    "tuo", "tua", "tuoi", "tue", "nostro", "nostra", "nostri", "nostre",
    "vostro", "vostra", "vostri", "vostre",

    # Parole aggiuntive basate sui risultati
    "all", "sull", "dell", "nell", "cos",  # forme tronche
    "nuovo", "dato", "usare", "sfida", "cambiare"  # altri termini comuni
}

# Combine custom stopwords with spaCy's stopwords
it_stops = set(nlp_it.Defaults.stop_words).union(ITALIAN_BASIC_STOPWORDS)
it_stops = set(stopwords.words('italian')).union(it_stops)


def get_language(newspaper):
    """Determine language based on newspaper"""
    return 'en' if newspaper in ENGLISH_NEWSPAPERS else 'it'


def print_language_stats(df, stage=""):
    """Print language statistics at any stage"""
    print(f"\nLanguage statistics {stage}:")
    lang_counts = Counter(df['language'].to_list())
    print(f"English words: {lang_counts.get('en', 0)}")
    print(f"Italian words: {lang_counts.get('it', 0)}")


def lemmatize_word(word: str, language: str) -> str:
    """
    Lemmatize a single word based on its language
    """
    # Don't lemmatize already compound words (containing underscore)
    if '_' in word:
        return word

    nlp = nlp_en if language == 'en' else nlp_it
    doc = nlp(word)

    # Return the lemma of the first token
    return doc[0].lemma_.lower()


def process_newspaper_group(newspaper_group: pl.DataFrame, language: str) -> list:
    """
    Process a group of words from a single newspaper
    """
    nlp = nlp_en if language == 'en' else nlp_it
    stops = en_stops if language == 'en' else it_stops
    results = []

    words = newspaper_group['word'].to_list()
    dates = newspaper_group['date'].to_list()
    years = newspaper_group['year'].to_list()
    quarters = newspaper_group['quarter'].to_list()

    # Print sample of words being processed
    print(f"\nProcessing {language} words from {newspaper_group['newspaper'][0]}")
    print(f"Sample words: {words[:5]}")

    for i, word in enumerate(words):
        # Skip if it's already a compound word
        if '_' in word:
            lemmatized = word
        else:
            # Check if it's a stopword using pre-cached sets
            if word.lower() in stops:
                continue

            # Lemmatize
            lemmatized = lemmatize_word(word, language)

        # Skip empty strings or single characters
        if len(lemmatized) <= 1:
            continue

        results.append({
            'date': dates[i],
            'newspaper': newspaper_group['newspaper'][0],
            'year': years[i],
            'quarter': quarters[i],
            'word': lemmatized,
            'language': language
        })

    return results


def process_dataframe(df: pl.DataFrame) -> pl.DataFrame:
    """
    Process the dataframe by lemmatizing words and removing stopwords
    """
    print("\nProcessing words...")
    results = []

    # Print initial language distribution
    print_language_stats(df, "before processing")

    # Group by newspaper for more efficient processing
    newspapers = df['newspaper'].unique()

    for newspaper in tqdm(newspapers, desc="Processing newspapers"):
        language = get_language(newspaper)

        # Get words for current newspaper
        newspaper_group = df.filter(pl.col('newspaper') == newspaper)

        # Process the group
        group_results = process_newspaper_group(newspaper_group, language)
        results.extend(group_results)

        # Print intermediate results
        temp_df = pl.DataFrame(group_results)
        if len(temp_df) > 0:
            print(f"\nSample results for {newspaper}:")
            print(temp_df.sample(n=min(3, len(temp_df))).select(['word', 'language']))

    # Create DataFrame letting Polars infer the schema
    final_df = pl.DataFrame(results, infer_schema_length=None)
    print_language_stats(final_df, "after processing")
    return final_df


def print_statistics(df_original: pl.DataFrame, df_processed: pl.DataFrame):
    """Print statistics about the processing"""
    print("\nProcessing Statistics:")
    print(f"Original number of words: {len(df_original)}")
    print(f"Processed number of words: {len(df_processed)}")
    print(f"Removed {len(df_original) - len(df_processed)} words")

    print("\nWords by newspaper after processing:")
    print(df_processed.group_by('newspaper').agg(pl.len()).sort('newspaper'))

    print("\nSample of processed words by language:")
    for language in ['en', 'it']:
        print(f"\n{language.upper()} sample words:")
        filtered_df = df_processed.filter(pl.col('language') == language)
        num_rows = len(filtered_df)
        if num_rows > 0:
            sample = filtered_df.sample(n=min(5, num_rows))
            print(sample.select(['word', 'newspaper']))
        else:
            print(f"No data available for language: {language}")


def main():
    # Load the dataset
    print("Loading dataset...")
    input_path = ("D:/PycharmProjects/Thesis/data/full_tests/title_to_rows/"
                  "beta_proportional_no_topic_snippet_author_words_fixed.csv")

    # Read with Polars
    df = pl.read_csv(input_path)

    # Convert date to datetime
    df = df.with_columns(pl.col('date').str.strptime(pl.Datetime, format='%Y-%m-%d'))

    # Process the dataframe
    df_processed = process_dataframe(df)

    # Save the processed dataset
    output_path = input_path.replace('.csv', '_lemmatized.csv')
    output_path = output_path.replace('/full_tests/title_to_rows/', '/to_analyze/')
    print(f"\nSaving processed dataset to {output_path}")
    df_processed.write_csv(output_path)

    # Print statistics
    print_statistics(df, df_processed)


if __name__ == "__main__":
    main()