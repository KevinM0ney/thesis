import polars as pl
from deep_translator import GoogleTranslator
import time
from tqdm import tqdm
from typing import Optional
import json
from pathlib import Path


def load_cache(cache_file='translation_cache.json'):
    """Load existing translations from cache"""
    try:
        with open(cache_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_cache(cache, cache_file='translation_cache.json'):
    """Save translations to cache"""
    with open(cache_file, 'w', encoding='utf-8') as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def translate_to_italian(word: str, language: str, translator, cache: dict) -> str:
    """
    Safely translate a word from English to Italian using GoogleTranslator with caching
    """
    # Return early if word is invalid
    if not isinstance(word, str) or not word.strip():
        return word

    # Only translate English words
    if language != 'en':
        return word

    # Check cache first
    if word.lower() in cache:
        return cache[word.lower()]

    # Delay to respect rate limits
    time.sleep(0.5)  # 0.5 secondi tra le richieste

    try:
        translation = translator.translate(text=word.lower())
        if translation:
            # Save to cache
            cache[word.lower()] = translation
            # Periodically save cache to file
            if len(cache) % 100 == 0:
                save_cache(cache)
            return translation

    except Exception as e:
        print(f"Translation error for word '{word}': {str(e)}")
        return word

    return word


# Read the CSV
path = "D:/PycharmProjects/Thesis/data/to_analyze/first_test.csv"
df = pl.read_csv(path)

# Initialize translator and cache
translator = GoogleTranslator(source='en', target='it')
cache = load_cache()

# Count total English words to translate
total_en_words = df.filter(pl.col('language') == 'en').shape[0]

# Create progress bar
pbar = tqdm(total=total_en_words, desc="Translating words")


def translate_with_progress(x):
    result = translate_to_italian(x['word'], x['language'], translator, cache)
    if x['language'] == 'en':
        pbar.update(1)
    return result


# Apply translation using Polars with specified return_dtype and progress bar
df_translated = df.with_columns([
    pl.struct(['word', 'language'])
    .map_elements(
        translate_with_progress,
        return_dtype=pl.Utf8
    )
    .alias('word')
])

# Save final cache
save_cache(cache)

# Close progress bar
pbar.close()

# Save to new file
output_path = path.replace("first_test", "first_test_italiano")
df_translated.write_csv(output_path)

print("\nTranslation completed. Check the output file for results.")