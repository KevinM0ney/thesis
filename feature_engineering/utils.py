import pandas as pd
import numpy as np
import spacy


def convert_italian_month(date_str):
    month_map = {
       'gen': '01', 'feb': '02', 'mar': '03', 'apr': '04',
       'mag': '05', 'giu': '06', 'lug': '07', 'ago': '08',
       'set': '09', 'ott': '10', 'nov': '11', 'dic': '12',
       'gennaio': '01', 'febbraio': '02', 'marzo': '03', 'aprile': '04',
       'maggio': '05', 'giugno': '06', 'luglio': '07', 'agosto': '08',
       'settembre': '09', 'ottobre': '10', 'novembre': '11', 'dicembre': '12'
    }
    if not isinstance(date_str, str):
        print(f"ERROR NOT A STRING: {date_str}")
        return
    day, month, year = date_str.split()
    month = month_map[month.lower()]
    return f"{year}-{month}-{day}"


def downsample_articles(df, method='fixed', n_articles=50, random_state=42):
    """
    Perform downsampling on articles dataset by quarter.

    Parameters:
    -----------
    df : pandas.DataFrame
        Input DataFrame with 'date' column and other article data
    method : str
        'fixed' for fixed number of articles per quarter
        'proportional' for using median as target
    n_articles : int
        Number of articles to sample per quarter when method='fixed'
    random_state : int
        Random seed for reproducibility

    Returns:
    --------
    pandas.DataFrame
        Downsampled DataFrame
    """
    # Ensure we have year and quarter columns
    if 'year' not in df.columns:
        df['year'] = pd.to_datetime(df['date']).dt.year
    if 'quarter' not in df.columns:
        df['quarter'] = pd.to_datetime(df['date']).dt.quarter

    if method == 'fixed':
        # Fixed number downsampling
        def sample_quarter(group):
            if len(group) <= n_articles:
                return group
            return group.sample(n=n_articles, random_state=random_state)

        result = df.groupby(['year', 'quarter']).apply(sample_quarter)

    elif method == 'proportional':
        # Calculate median articles per quarter for this newspaper
        quarterly_counts = df.groupby(['year', 'quarter']).size()
        target_size = int(quarterly_counts.median())

        def sample_proportional(group):
            if len(group) <= target_size:
                return group
            return group.sample(n=target_size, random_state=random_state)

        result = df.groupby(['year', 'quarter']).apply(sample_proportional)

    else:
        raise ValueError("method must be either 'fixed' or 'proportional'")

    # Reset index after groupby
    result = result.reset_index(drop=True)

    return result


def downsample_all_newspapers(dfs_dict, method='fixed', n_articles=50):
    """
    Apply downsampling to multiple newspapers.

    Parameters:
    -----------
    dfs_dict : dict
        Dictionary with newspaper names as keys and DataFrames as values
    method : str
        Downsampling method ('fixed' or 'proportional')
    n_articles : int
        Number of articles for fixed method

    Returns:
    --------
    dict
        Dictionary with downsampled DataFrames
    """
    return {
        name: downsample_articles(df, method=method, n_articles=n_articles)
        for name, df in dfs_dict.items()
    }


def lemming(series, language="Italian"):
    """
    Lemmatize each word in a pandas Series using spaCy model.

    Args:
        series (pd.Series): Input series containing words to lemmatize
        language (str): Language model to use. Options: "Italian" or "English"

    Returns:
        pd.Series: Series containing lemmatized words

    Example:
        >>> import pandas as pd
        >>> # Italian example
        >>> words_it = pd.Series(['cani', 'gatte', 'mangiando'])
        >>> lemming(words_it, "Italian")
        0    cane
        1    gatto
        2    mangiare
        dtype: object
        >>> # English example
        >>> words_en = pd.Series(['dogs', 'cats', 'running'])
        >>> lemming(words_en, "English")
        0    dog
        1    cat
        2    run
        dtype: object
    """
    if language.lower() == "italian":
        nlp = spacy.load('it_core_news_sm')
    elif language.lower() == "english":
        nlp = spacy.load('en_core_web_sm')
    else:
        raise ValueError("Language must be either 'Italian' or 'English'")

    return series.apply(lambda x: nlp(str(x))[0].lemma_)


def normalize(series, glossary=None):
    """
    Normalize text in a pandas Series by converting all characters to lowercase,
    except for words specified in the glossary.

    Text normalization is a crucial preprocessing step in text analysis that aims to
    homogenize different spellings to eliminate potential sources of textual duplication.
    This function specifically handles case normalization while preserving specified words
    that need to maintain their original case (e.g., proper nouns, acronyms, or words
    where case distinction carries semantic meaning).

    Args:
        series (pd.Series): Input series containing text to normalize
        glossary (list, optional): List of words to preserve in their original case.
            Default is None, which means all text will be converted to lowercase.

    Returns:
        pd.Series: Series containing normalized text

    Example:
        >>> import pandas as pd
        >>> # Simple lowercase conversion
        >>> text = pd.Series(['The Church is BEAUTIFUL', 'NASA launched ROCKETS'])
        >>> normalize(text)
        0    the church is beautiful
        1    nasa launched rockets
        dtype: object

        >>> # Using glossary to preserve specific words
        >>> text = pd.Series(['The Church is BEAUTIFUL', 'NASA launched ROCKETS'])
        >>> glossary = ['Church', 'NASA']
        >>> normalize(text, glossary)
        0    the Church is beautiful
        1    NASA launched rockets
        dtype: object
    """
    if glossary is None:
        return series.str.lower()

    def preserve_glossary(text):
        text_lower = text.lower()
        for word in glossary:
            # Replace the lowercase version with the glossary version
            text_lower = text_lower.replace(word.lower(), word)
        return text_lower

    return series.apply(preserve_glossary)


def remove_stopwords(series, language="Italian"):
    if language.lower() == "italian":
        nlp = spacy.load('it_core_news_sm')
    elif language.lower() == "english":
        nlp = spacy.load('en_core_web_sm')
    else:
        raise ValueError("Language must be either 'Italian' or 'English'")
    return series.apply(lambda x: ' '.join(
        [word for word in str(x).split()
         if word.lower() not in nlp.Defaults.stop_words]))


GLOSSARY = [
    # English terms
    "artificial intelligence",
    "machine learning",
    "deep learning",
    "natural language processing",
    "computer vision",
    "neural network",
    "neural networks",
    "generative ai",
    "large language model",
    "large language models",
    "supervised learning",
    "unsupervised learning",
    "reinforcement learning",
    "edge computing",
    "quantum computing",
    "cloud computing",
    "computer science",
    "data science",
    "data mining",
    "big data",
    "decision making",
    "neural nets",
    "autonomous systems",
    "autonomous vehicles",
    "machine translation",
    "facial recognition",
    "image recognition",
    "speech recognition",
    "pattern recognition",
    "predictive analytics",
    "robotic process",
    "expert systems",
    "knowledge base",
    "knowledge graph",
    "semantic web",
    "tech giants",
    "silicon valley",
    "open source",
    "real time",
    "chatgpt plus",

    # Company and Product Names
    "open ai",
    "microsoft azure",
    "google cloud",
    "amazon web",
    "meta platforms",
    "deep mind",
    "anthropic claude",
    "boston dynamics",
    "tesla autopilot",
    "hugging face",

    # Italian terms
    "intelligenza artificiale",
    "apprendimento automatico",
    "apprendimento profondo",
    "elaborazione linguaggio naturale",
    "visione artificiale",
    "rete neurale",
    "reti neurali",
    "intelligenza artificiale generativa",
    "modelli linguistici",
    "apprendimento supervisionato",
    "apprendimento non supervisionato",
    "apprendimento per rinforzo",
    "calcolo quantistico",
    "scienza dei dati",
    "analisi dei dati",
    "riconoscimento facciale",
    "riconoscimento vocale",
    "riconoscimento immagini",
    "processo decisionale",
    "sistemi autonomi",
    "veicoli autonomi",
    "traduzione automatica",
    "analisi predittiva",
    "processo robotico",
    "sistemi esperti",
    "base di conoscenza",
    "grafo della conoscenza",
    "web semantico",
    "giganti tecnologici",
    "valle del silicio",
    "codice aperto",
    "tempo reale",

    # Common Italian phrases in tech context
    "internet delle cose",
    "realtà aumentata",
    "realtà virtuale",
    "metaverso digitale",
    "privacy dei dati",
    "sicurezza informatica",
    "trasformazione digitale",
    "tecnologie emergenti",
    "assistente virtuale",
    "assistenti virtuali",
    "startup innovative",

    # Hybrid/International terms
    "machine learning engineer",
    "data scientist",
    "software engineer",
    "product manager",
    "tech leader",
    "chief technology officer",
    "data analyst",
    "project manager",
    "digital transformation",

    # AI Models and Frameworks
    "gpt four",
    "gpt 4",
    "gpt three",
    "gpt 3",
    "bert model",
    "stable diffusion",
    "dall e",
    "dall e 2",
    "dall e 3",
    "midjourney ai",
    "tensorflow framework",
    "pytorch framework",

    # Emerging Tech Terms
    "web three",
    "web 3",
    "blockchain technology",
    "smart contract",
    "smart contracts",
    "distributed ledger",
    "quantum supremacy",
    "edge computing",
    "fog computing",

    # Regulatory and Ethical Terms
    "ethical ai",
    "responsible ai",
    "ai ethics",
    "ai regulation",
    "data protection",
    "privacy protection",
    "ai governance",
    "algorithmic bias",
    "ai transparency",

    # Business and Industry Terms
    "digital transformation",
    "industry four",
    "industry 4",
    "business intelligence",
    "customer experience",
    "supply chain",
    "market intelligence",
    "predictive maintenance",

    # Research and Academic Terms
    "research paper",
    "peer review",
    "case study",
    "white paper",
    "proof of concept",
    "state of the art",

    # Job Titles and Roles
    "research scientist",
    "lead engineer",
    "senior developer",
    "project lead",
    "team leader",
    "chief executive",
    "vice president"
]