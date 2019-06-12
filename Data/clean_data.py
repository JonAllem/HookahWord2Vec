"""
Script to clean the raw data.
"""
import functools as ft
import os
import pickle
import re
import string
import sys

import langid
import nltk
import pandas as pd

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(CURRENT_DIR, 'Raw\\')
PICKLE_DIR = os.path.join(CURRENT_DIR, 'Pickles\\')

def cache_as_pickle(save_dir):
    """
    Decorator to use over any processor function to save the result of the function in a pickle.
    Processor function must take an argument 'filename' which is path to the file to process.
    Args:
        save_dir: Directory of the pickle save.
    Result:
        Saves the result of the processor function in save_dir/{filename_base_name}.pickle
    """
    def _load_or_generate(func, force_generate=False, **kwargs):
        filename = os.path.basename(kwargs['filename'])
        save_path = os.path.join(save_dir, f'{filename}.pickle')
        if os.path.exists(save_path) and not force_generate:
            print(f'Found pickle at {save_path}. Loading it.')
            with open(save_path, 'rb') as pickled_file:
                return pickle.load(pickled_file)
        print(f'No pickle found at {save_path}. Creating it.')
        result = func(**kwargs)
        with open(save_path, 'wb') as pickled_file:
            pickle.dump(result, pickled_file)
        return result
    return lambda func: ft.wraps(func)(ft.partial(_load_or_generate, func))

def normalize_tweets(tweets):
    """
    Normalize the tweets.
    Returns:
        [non-lemmatiezed normalized tweets], [lemmatized and normalized tweets]
    """
    url_regex = re.compile(
        r'^(?:http|ftp)s?://' # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
        r'localhost|' #localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
        r'(?::\d+)?' # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    tokenizer = nltk.tokenize.TweetTokenizer()
    lemmatizer = nltk.WordNetLemmatizer()
    printable_chars = set(string.printable)
    punctuations = set(string.punctuation)

    def _normalize_tweet(tweet_text):
        words, lemmatiezed_words, hashtags = [], [], set()
        clean_tweet = ''.join(filter(lambda c: c in printable_chars, tweet_text))
        for token, tag in nltk.pos_tag(tokenizer.tokenize(clean_tweet.lower())):
            if len(token) < 2 or url_regex.search(token):
                continue
            elif all(char in punctuations for char in token):
                continue
            elif token[0] == '@':
                token = '@person' #Replace all friend tags with a common token.
                tag = 'NNP'
            elif token[0] == '#':
                hashtags.add(token)
            words.append(token)
            lemmatiezed_words.append(_lemmatizeToken(token, tag))
        return words, lemmatiezed_words, hashtags

    def _lemmatizeToken(token, tag):
        tag = {
            'N': nltk.corpus.wordnet.NOUN,
            'V': nltk.corpus.wordnet.VERB,
            'R': nltk.corpus.wordnet.ADV,
            'J': nltk.corpus.wordnet.ADJ
        }.get(tag[0], nltk.corpus.wordnet.NOUN)
        return lemmatizer.lemmatize(token, tag)

    normalized_text, normalized_text_lemmatized, hashtags = [], [], []
    for i, x in enumerate(tweets):
        y = _normalize_tweet(x)
        normalized_text.append(y[0])
        normalized_text_lemmatized.append(y[1])
        hashtags.append(y[2])
        if i % 10000 == 0:
            print(f'Finished normalizing {i} tweets.')
    return normalized_text, normalized_text_lemmatized, hashtags

@cache_as_pickle(os.path.join(PICKLE_DIR))
def clean_tweets(filename, filter_keywords):
    """
    Cleans the tweets. Performs the following ops:
        1. Removes retweets.
        2. Normalizes the tweets.
        3. Filters out tweets that don't contain any keyword mentioned in filter_keywords.
        4. Filters out non-english tweets.
    """
    df = pd.read_csv(filename, dtype={'CreatedAt': str, 'Text': str, 'Id': 'int64', 'UserId': 'int64', 'IsRetweet': bool},
                    parse_dates=['CreatedAt'])
    print(f'Loaded file. {df.shape[0]} tweets found.')

    no_retweets = df[~df.IsRetweet]
    print(f'Removed retweets. {no_retweets.shape[0]} tweets remaining.')

    normalized_tweets = normalize_tweets(no_retweets.Text)
    no_retweets['NormalizedText'] = normalized_tweets[0]
    no_retweets['NormalizedTextLemmatized'] = normalized_tweets[1]
    no_retweets['HashTags'] = normalized_tweets[2]
    print('Normalized text.')

    keyword_tweets = no_retweets[no_retweets.apply(
        lambda x: len(filter_keywords.intersection(x.NormalizedText + x.NormalizedTextLemmatized)) > 0 or\
                any(word in hashtag for word in filter_keywords for hashtag in x.HashTags),
        axis=1)]
    print(f'Filtered by keywords. {keyword_tweets.shape[0]} tweets remaining.')

    english_tweets = keyword_tweets[keyword_tweets.apply(lambda x: langid.classify(x.Text)[0] == 'en', axis=1)]
    print(f'Removed non-english tweets. {english_tweets.shape[0]} tweets remaining.')
    return english_tweets

if __name__ == '__main__':
    product_keywords = {
        'swisher', 'swisher sweets', 'swishersweets', 'swisherartistproject', 'swisherartistgrant', 'swisheratl',
        'hookah', 'shees', 'seesh',
        'e-cig', 'ecig', 'e-cigs', 'ecigs', 'e-cigarette', 'ecigarette', 'e-cigarettes', 'ecigarettes',
        'vape', 'vaper', 'vaping', 'vapes', 'vapers',
        'cigarette', 'cigarettes', 'marlboro'
    }
    #pylint: disable=E1123
    file_to_clean = sys.argv[1]
    tweets = clean_tweets(filename=file_to_clean, filter_keywords=product_keywords, force_generate=True)
    print(tweets.head())
