import pandas as pd
import os
import re
from tqdm import tqdm
from collections import Counter
from datetime import datetime

# CSV files paths
csv_files = [
    'D:/PycharmProjects/Thesis/data/ai4business.csv',
    'D:/PycharmProjects/Thesis/data/ainews.csv',
    'D:/PycharmProjects/Thesis/data/guardian_ai_articles.csv',
    'D:/PycharmProjects/Thesis/data/il_corriere_della_sera.csv',
    'D:/PycharmProjects/Thesis/data/ilsole24.csv',
    'D:/PycharmProjects/Thesis/data/nyt_ai_articles.csv',
    'D:/PycharmProjects/Thesis/data/reddit_posts.csv',
    'D:/PycharmProjects/Thesis/data/wired.csv',
    'D:/PycharmProjects/Thesis/data/youtube_videos.csv'
]

# Define AI-related terms to search for
ai_terms = [
    # Core AI terms - English and Italian
    'artificial intelligence', 'intelligenza artificiale',
    'machine learning', 'apprendimento automatico',
    'deep learning', 'apprendimento profondo',
    'neural network', 'rete neurale',
    'neural networks', 'reti neurali',
    'natural language processing', 'elaborazione del linguaggio naturale',
    'nlp', 'elaborazione linguistica',

    # Common acronyms (with spaces for exact matching)
    ' AI ', ' IA ',
    'ML', 'AA',  # Apprendimento Automatico
    'NLP', 'ELN',  # Elaborazione Linguaggio Naturale
    'LLM', 'MLG',  # Modello Linguistico Grande
    'AGI', 'IGA',  # Intelligenza Generale Artificiale
    'GPT',

    # Simple/Common Terms - English and Italian
    'robot', 'robot',  # Same in both languages
    'robots', 'robot',  # Italian uses same form for plural
    'chatbot', 'chatbot',  # Often kept in English in Italian
    'bot', 'bot',
    'automation', 'automazione',
    'automated', 'automatizzato',
    'smart system', 'sistema intelligente',
    'smart systems', 'sistemi intelligenti',
    'intelligent system', 'sistema intelligente',
    'intelligent systems', 'sistemi intelligenti',
    'virtual assistant', 'assistente virtuale',
    'digital assistant', 'assistente digitale',
    'ai-powered', 'basato su ia',
    'ai powered', 'alimentato da ia',
    'ai technology', 'tecnologia ia',
    'ai technologies', 'tecnologie ia',
    'ai solution', 'soluzione ia',
    'ai solutions', 'soluzioni ia',
    'ai tool', 'strumento ia',
    'ai tools', 'strumenti ia',
    'machine intelligence', 'intelligenza delle macchine',
    'artificial mind', 'mente artificiale',
    'computer intelligence', 'intelligenza del computer',

    # Specific models and platforms (mostly kept in English in Italian)
    'chatgpt', 'chatgpt',
    'gpt-4', 'gpt-4',
    'gpt-3', 'gpt-3',
    'gpt4', 'gpt4',
    'gpt3', 'gpt3',
    'gpt-2', 'gpt-2',
    'gpt2', 'gpt2',
    'claude', 'claude',
    'anthropic', 'anthropic',
    'openai', 'openai',
    'google bard', 'google bard',
    'gemini', 'gemini',
    'microsoft copilot', 'microsoft copilot',
    'github copilot', 'github copilot',
    'dall-e', 'dall-e',
    'dalle', 'dalle',
    'midjourney', 'midjourney',
    'stable diffusion', 'stable diffusion',
    'meta ai', 'meta ia',
    'google ai', 'google ia',
    'deepmind', 'deepmind',
    'inflection ai', 'inflection ia',
    'mistral ai', 'mistral ia',
    'cohere', 'cohere',
    'hugging face', 'hugging face',
    'replicate', 'replicate',
    'palm', 'palm',
    'bert', 'bert',
    'galactica', 'galactica',
    'falcon', 'falcon',
    'llama', 'llama',
    'alpaca', 'alpaca',

    # Technical terms - English and Italian
    'large language model', 'modello linguistico grande',
    'language models', 'modelli linguistici',
    'foundation model', 'modello base',
    'transformer model', 'modello transformer',
    'computer vision', 'visione artificiale',
    'reinforcement learning', 'apprendimento per rinforzo',
    'supervised learning', 'apprendimento supervisionato',
    'unsupervised learning', 'apprendimento non supervisionato',
    'sentiment analysis', 'analisi del sentimento',
    'vector database', 'database vettoriale',
    'training data', 'dati di addestramento',
    'prompt engineering', 'ingegneria dei prompt',
    'neural nets', 'reti neurali',
    'deep neural', 'neurale profondo',
    'machine translation', 'traduzione automatica',
    'speech recognition', 'riconoscimento vocale',
    'text-to-speech', 'sintesi vocale',
    'text to speech', 'da testo a voce',
    'speech-to-text', 'riconoscimento del parlato',
    'speech to text', 'da voce a testo',
    'text-to-image', 'da testo a immagine',
    'text to image', 'generazione di immagini da testo',
    'image generation', 'generazione di immagini',
    'semantic analysis', 'analisi semantica',
    'embedding', 'incorporamento',
    'embeddings', 'incorporamenti',
    'tokens', 'token',
    'tokenization', 'tokenizzazione',
    'fine-tuning', 'messa a punto',
    'fine tuning', 'ottimizzazione',
    'fine-tuned', 'ottimizzato',
    'fine tuned', 'messo a punto',
    'dataset', 'set di dati',
    'datasets', 'set di dati',
    'training set', 'set di addestramento',
    'test set', 'set di test',
    'validation set', 'set di validazione',

    # AI Applications - English and Italian
    'autonomous system', 'sistema autonomo',
    'autonomous systems', 'sistemi autonomi',
    'automated decision', 'decisione automatizzata',
    'predictive analytics', 'analisi predittiva',
    'ai assistant', 'assistente ia',
    'smart assistant', 'assistente intelligente',
    'facial recognition', 'riconoscimento facciale',
    'voice recognition', 'riconoscimento vocale',
    'pattern recognition', 'riconoscimento di pattern',
    'image recognition', 'riconoscimento delle immagini',
    'object detection', 'rilevamento oggetti',
    'recommendation system', 'sistema di raccomandazione',
    'predictive model', 'modello predittivo',
    'decision support', 'supporto decisionale',
    'automated process', 'processo automatizzato',
    'process automation', 'automazione dei processi',
    'robotic process', 'processo robotico',
    'intelligent automation', 'automazione intelligente',
    'smart automation', 'automazione intelligente',
    'autonomous vehicle', 'veicolo autonomo',
    'self-driving', 'guida autonoma',
    'machine vision', 'visione artificiale',
    'automated learning', 'apprendimento automatizzato',
    'automated analysis', 'analisi automatizzata',
    'automated detection', 'rilevamento automatico',
    'automated classification', 'classificazione automatica',
    'ai application', 'applicazione ia',
    'ai applications', 'applicazioni ia',
    'ai system', 'sistema ia',
    'ai systems', 'sistemi ia',

    # AI Ethics and Governance - English and Italian
    'ai ethics', 'etica dell\'ia',
    'ai safety', 'sicurezza dell\'ia',
    'ai regulation', 'regolamentazione dell\'ia',
    'ai governance', 'governance dell\'ia',
    'responsible ai', 'ia responsabile',
    'ethical ai', 'ia etica',
    'algorithmic bias', 'bias algoritmico',
    'ai bias', 'pregiudizio dell\'ia',
    'ai risk', 'rischio ia',
    'ai risks', 'rischi ia',
    'ai policy', 'politica ia',
    'ai policies', 'politiche ia',
    'ai guidelines', 'linee guida ia',
    'ai principles', 'principi ia',
    'ai framework', 'framework ia',
    'ai standards', 'standard ia',
    'ai transparency', 'trasparenza dell\'ia',
    'ai accountability', 'responsabilità dell\'ia',
    'ai compliance', 'conformità ia',
    'ai legislation', 'legislazione ia',
    'ai law', 'legge ia',
    'ai laws', 'leggi ia',
    'ai rights', 'diritti ia',
    'ai impact', 'impatto ia',
    'ai effects', 'effetti ia',
    'ai implications', 'implicazioni ia',

    # Future AI Concepts - English and Italian
    'artificial general intelligence', 'intelligenza artificiale generale',
    'artificial superintelligence', 'superintelligenza artificiale',
    'superintelligent ai', 'ia superintelligente',
    'human-level ai', 'ia di livello umano',
    'human level ai', 'intelligenza artificiale di livello umano',
    'strong ai', 'ia forte',
    'weak ai', 'ia debole',
    'narrow ai', 'ia ristretta',
    'general ai', 'ia generale',
    'super ai', 'super ia',
    'conscious ai', 'ia cosciente',
    'ai consciousness', 'coscienza dell\'ia',
    'ai singularity', 'singolarità dell\'ia',
    'technological singularity', 'singolarità tecnologica',
    'future of ai', 'futuro dell\'ia',
    'ai future', 'futuro dell\'ia',
    'next-gen ai', 'ia di nuova generazione',
    'next generation ai', 'ia di prossima generazione',
    'advanced ai', 'ia avanzata',
    'modern ai', 'ia moderna',

    # AI Development/Industry - English and Italian
    'ai development', 'sviluppo ia',
    'ai research', 'ricerca ia',
    'ai researcher', 'ricercatore ia',
    'ai researchers', 'ricercatori ia',
    'ai startup', 'startup ia',
    'ai startups', 'startup ia',
    'ai company', 'azienda ia',
    'ai companies', 'aziende ia',
    'ai industry', 'industria ia',
    'ai sector', 'settore ia',
    'ai market', 'mercato ia',
    'ai business', 'business ia',
    'ai investment', 'investimento ia',
    'ai funding', 'finanziamento ia',
    'ai venture', 'venture ia',
    'ai innovation', 'innovazione ia',
    'ai technology', 'tecnologia ia',
    'ai technologies', 'tecnologie ia',
    'ai solution', 'soluzione ia',
    'ai solutions', 'soluzioni ia',
    'ai platform', 'piattaforma ia',
    'ai platforms', 'piattaforme ia',

    # Additional Technical Terms - English and Italian
    'machine reasoning', 'ragionamento automatico',
    'expert system', 'sistema esperto',
    'expert systems', 'sistemi esperti',
    'knowledge base', 'base di conoscenza',
    'knowledge graph', 'grafo della conoscenza',
    'neural processing', 'elaborazione neurale',
    'cognitive computing', 'computazione cognitiva',
    'computational intelligence', 'intelligenza computazionale',
    'autonomous agent', 'agente autonomo',
    'autonomous agents', 'agenti autonomi',
    'artificial neural network', 'rete neurale artificiale',
    'deep neural network', 'rete neurale profonda',
    'convolutional neural network', 'rete neurale convoluzionale',
    'recurrent neural network', 'rete neurale ricorrente',

    # Industry Applications - English and Italian
    'predictive maintenance', 'manutenzione predittiva',
    'anomaly detection', 'rilevamento anomalie',
    'fraud detection', 'rilevamento frodi',
    'risk assessment', 'valutazione del rischio',
    'process optimization', 'ottimizzazione dei processi',
    'quality control', 'controllo qualità',
    'demand forecasting', 'previsione della domanda',
    'customer analytics', 'analisi dei clienti',
    'market intelligence', 'intelligence di mercato',
    'business intelligence', 'business intelligence',
    'data mining', 'data mining',
    'text mining', 'text mining',
    'process mining', 'process mining'
]

# CSV files and metadata
source_info = {
    'ai4business.csv': {'language': 'italian', 'type': 'business articles'},
    'ainews.csv': {'language': 'english', 'type': 'ai news aggregator'},
    'guardian_ai_articles.csv': {'language': 'english', 'type': 'news articles'},
    'il_corriere_della_sera.csv': {'language': 'italian', 'type': 'news articles'},
    'ilsole24.csv': {'language': 'italian', 'type': 'business news'},
    'nyt_ai_articles.csv': {'language': 'english', 'type': 'news articles'},
    'reddit_posts.csv': {'language': 'english', 'type': 'social media'},
    'wired.csv': {'language': 'english', 'type': 'tech articles'},
    'youtube_videos.csv': {'language': 'english', 'type': 'video content'}
}


def check_ai_terms(text: str, terms: list) -> tuple[bool, list]:
    """
    Check for AI terms in text, using a more flexible matching approach.

    Args:
        text: Text to analyze
        terms: List of AI terms to search for

    Returns:
        Tuple of (bool indicating if AI terms were found, list of found terms)
    """
    if pd.isna(text):
        return False, []

    text = str(text).lower()
    found_terms = []

    # Common special cases that should always be caught
    special_cases = [
        'ai', 'ia',  # Standard form
        'a.i.', 'i.a.',  # With dots
        'a.i', 'i.a',  # With single dot
        'a.i', 'i.a',  # Alternative forms
        'ai.', 'ia.'  # With trailing dot
    ]

    # Check special cases with word boundaries
    for term in special_cases:
        pattern = r'\b' + re.escape(term) + r'\b'
        if re.search(pattern, text):
            found_terms.append(term)

    # Check other terms
    for term in terms:
        term = term.lower()
        if term in text:  # Simple substring match for other terms
            found_terms.append(term)

    return bool(found_terms), list(set(found_terms))


def parse_date(date_str: str) -> pd.Timestamp:
    """
    Parse dates in various formats.

    Args:
        date_str: Date string to parse

    Returns:
        Parsed datetime or None if parsing fails
    """
    try:
        # Try common formats
        formats = [
            '%Y-%m-%d',
            '%d/%m/%Y',
            '%d.%m.%Y',
            '%Y/%m/%d',
            '%m/%d/%Y',
            '%d-%m-%Y'
        ]

        for fmt in formats:
            try:
                return pd.to_datetime(date_str, format=fmt)
            except:
                continue

        # If no format matches, let pandas try to figure it out
        return pd.to_datetime(date_str)
    except:
        return None


def analyze_file(filepath: str, terms: list) -> tuple:
    """
    Analyze a single CSV file for AI terms.

    Args:
        filepath: Path to CSV file
        terms: List of AI terms to search for

    Returns:
        Tuple of (columns, total_rows, ai_rows, term_frequencies, monthly_counts)
    """
    df = pd.read_csv(filepath)
    total_rows = len(df)
    term_frequencies = Counter()
    ai_rows = 0
    monthly_counts = {}

    # Find and parse date column
    date_columns = [col for col in df.columns if 'date' in col.lower()]
    if date_columns:
        date_col = date_columns[0]
        df[date_col] = df[date_col].apply(parse_date)

    # Process rows with progress bar
    with tqdm(total=total_rows, desc=f"Analyzing {os.path.basename(filepath)}") as pbar:
        for _, row in df.iterrows():
            row_text = ' '.join(str(val) for val in row.values)
            has_ai, found_terms = check_ai_terms(row_text, terms)

            if has_ai:
                ai_rows += 1
                term_frequencies.update(found_terms)

                # Track monthly counts if date exists
                if date_columns and pd.notna(row[date_col]):
                    month_key = row[date_col].strftime('%Y-%m')
                    monthly_counts[month_key] = monthly_counts.get(month_key, 0) + 1

            pbar.update(1)

    return df.columns.tolist(), total_rows, ai_rows, term_frequencies, monthly_counts


def create_report():
    """Generate detailed analysis report"""
    total_rows = 0
    total_ai_rows = 0
    overall_term_frequencies = Counter()
    all_monthly_counts = {}

    # Create reports directory
    report_dir = "../reports"
    os.makedirs(report_dir, exist_ok=True)

    report_path = os.path.join(report_dir, f"ai_content_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")

    with open(report_path, 'w', encoding='utf-8') as f:
        # Write header
        f.write("AI CONTENT ANALYSIS REPORT\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Number of AI terms monitored: {len(ai_terms)}\n\n")

        # Process each file
        for filepath in tqdm(csv_files, desc="Analyzing datasets", position=0):
            try:
                filename = os.path.basename(filepath)
                columns, rows, ai_rows, term_freqs, monthly_counts = analyze_file(filepath, ai_terms)

                # Update totals
                total_rows += rows
                total_ai_rows += ai_rows
                overall_term_frequencies.update(term_freqs)

                # Update monthly counts
                for month, count in monthly_counts.items():
                    if month not in all_monthly_counts:
                        all_monthly_counts[month] = {}
                    all_monthly_counts[month][filename] = count

                # Write detailed file summary
                f.write(f"\nDATASET: {filename}\n")
                f.write("-" * 40 + "\n")
                f.write(f"Type: {source_info[filename]['type']}\n")
                f.write(f"Language: {source_info[filename]['language']}\n")
                f.write(f"Total rows: {rows:,}\n")
                f.write(f"AI content identified: {ai_rows:,} ({(ai_rows / rows * 100):.1f}%)\n")
                f.write(f"Available columns: {', '.join(columns)}\n\n")

                # Write top terms for this dataset
                f.write("Most frequent AI terms in this dataset:\n")
                for term, freq in sorted(term_freqs.items(), key=lambda x: x[1], reverse=True)[:10]:
                    f.write(f"- {term}: {freq:,} occurrences\n")

                # Write monthly trend if available
                if monthly_counts:
                    f.write("\nMonthly trend:\n")
                    for month in sorted(monthly_counts.keys()):
                        f.write(f"- {month}: {monthly_counts[month]:,} items\n")

                f.write("\n" + "=" * 80 + "\n")

            except Exception as e:
                f.write(f"\nError analyzing {filename}: {str(e)}\n")
                continue

        # Write overall summary
        f.write("\nOVERALL SUMMARY\n")
        f.write("=" * 80 + "\n")
        f.write(f"Datasets analyzed: {len(csv_files)}\n")
        f.write(f"Total rows analyzed: {total_rows:,}\n")
        f.write(f"Total AI content: {total_ai_rows:,}\n")
        f.write(f"Average AI content percentage: {(total_ai_rows / total_rows * 100):.1f}%\n\n")

        # Write overall top terms
        f.write("TOP 20 MOST FREQUENT AI TERMS (ALL DATASETS)\n")
        f.write("-" * 50 + "\n")
        for term, freq in sorted(overall_term_frequencies.items(), key=lambda x: x[1], reverse=True)[:20]:
            f.write(f"{term}: {freq:,} occurrences\n")

        # Write overall monthly trend
        f.write("\nOVERALL TEMPORAL TREND\n")
        f.write("-" * 50 + "\n")
        if all_monthly_counts:
            for month in sorted(all_monthly_counts.keys()):
                total = sum(all_monthly_counts[month].values())
                f.write(f"{month}: {total:,} total items\n")
                for source, count in sorted(all_monthly_counts[month].items()):
                    f.write(f"  - {source}: {count:,}\n")

        # Write summary by source type
        f.write("\nANALYSIS BY SOURCE TYPE\n")
        f.write("-" * 50 + "\n")
        source_type_stats = {}
        for filename in csv_files:
            basename = os.path.basename(filename)
            source_type = source_info[basename]['type']
            if source_type not in source_type_stats:
                source_type_stats[source_type] = {'total': 0, 'ai': 0}

            # Get stats for this file
            _, rows, ai_rows, _, _ = analyze_file(filename, ai_terms)
            source_type_stats[source_type]['total'] += rows
            source_type_stats[source_type]['ai'] += ai_rows

        for source_type, stats in source_type_stats.items():
            percentage = (stats['ai'] / stats['total'] * 100) if stats['total'] > 0 else 0
            f.write(f"\n{source_type.title()}:\n")
            f.write(f"- Total content: {stats['total']:,}\n")
            f.write(f"- AI content: {stats['ai']:,} ({percentage:.1f}%)\n")

    print(f"\nReport successfully generated: {report_path}")


def split_datasets():
    """
    Split each dataset into two parts: one with AI terms and one without.
    Save both parts in the half_data directory.
    """
    # Create half_data directory if it doesn't exist
    output_dir = "../half_data"
    os.makedirs(output_dir, exist_ok=True)

    for filepath in tqdm(csv_files, desc="Splitting datasets"):
        try:
            # Read the CSV file
            df = pd.read_csv(filepath)
            filename = os.path.basename(filepath)
            basename = os.path.splitext(filename)[0]

            # List to track which rows contain AI terms
            ai_rows = []
            non_ai_rows = []

            # Process each row
            for idx, row in df.iterrows():
                row_text = ' '.join(str(val) for val in row.values)
                has_ai, _ = check_ai_terms(row_text, ai_terms)

                if has_ai:
                    ai_rows.append(idx)
                else:
                    non_ai_rows.append(idx)

            # Create AI and non-AI dataframes
            ai_df = df.loc[ai_rows]
            non_ai_df = df.loc[non_ai_rows]

            # Save the split datasets
            ai_output_path = os.path.join(output_dir, f"{basename}_with_ai.csv")
            non_ai_output_path = os.path.join(output_dir, f"{basename}_without_ai.csv")

            ai_df.to_csv(ai_output_path, index=False)
            non_ai_df.to_csv(non_ai_output_path, index=False)

            # Print summary
            print(f"\nProcessed {filename}:")
            print(f"- Original rows: {len(df)}")
            print(f"- Rows with AI terms: {len(ai_rows)} ({len(ai_rows) / len(df) * 100:.1f}%)")
            print(f"- Rows without AI terms: {len(non_ai_rows)} ({len(non_ai_rows) / len(df) * 100:.1f}%)")
            print(f"- Files saved as: {os.path.basename(ai_output_path)} and {os.path.basename(non_ai_output_path)}")

        except Exception as e:
            print(f"\nError processing {filename}: {str(e)}")
            continue


if __name__ == "__main__":
    split_datasets()
