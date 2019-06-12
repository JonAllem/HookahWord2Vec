"""
A set of common utility functions to be used in each product analysis.
"""
import glob
import itertools as it
import os
import pickle

import bokeh.plotting as bplt
import bokeh.models as bmodels
import bokeh.layouts as blayouts
import nltk
import pandas as pd
from wordcloud import WordCloud

CURRENT_DIR = os.path.abspath(os.curdir)
DATA_PICKLE_DIR = os.path.join(CURRENT_DIR, 'Data\\Pickles\\')
DATA_BOTSCORES_DIR = os.path.join(CURRENT_DIR, 'Data\\BotScores\\')
ASSETS_DIR = os.path.join(CURRENT_DIR, 'Assets\\')

def load_product_group(product_group):
    filename = os.path.join(DATA_PICKLE_DIR, f'ProductGroupedDFs-{product_group}.pickle')
    with open(filename, 'rb') as file_handle:
        data = pickle.load(file_handle)

    # Drop 'sheesh', and 'shisha' keywords from hookah dataset.
    if product_group == 'hookah':
        for datum in data:
            drop_list = []
            for x in datum.df.itertuples():
                if 'hookah' not in x.NormalizedText and 'hookah' not in x.NormalizedTextLemmatized:
                    drop_list.append(x.Index)
            datum.df.drop(drop_list, inplace=True)
    return data

def load_botscores(product_group):
    botscore_glob = os.path.join(DATA_BOTSCORES_DIR, f'UserBotScores-{product_group}-*.pickle')
    botscores = {}
    for filename in glob.glob(botscore_glob):
        with open(filename, 'rb') as file_handle:
            scores = pickle.load(file_handle)
            botscores.update(scores)
    return botscores

def botscore_hist(botscore_dict, data, use_cap=False):
    bins = {'user_english': {}, 'tweet_english': {}, 'user_universal': {}, 'tweet_universal': {}}
    max_bin, score_type, score_mult = (101, 'cap', 100) if use_cap else (51, 'scores', 10)
    for key in bins:
        bins[key] = {start: 0 for start in range(0, max_bin)}
    grouped_tweets = pd.concat([datum.df for datum in data]).groupby('UserId').size().to_frame('tweet_count')
    for user_id, scores in botscore_dict.items():
        if user_id not in grouped_tweets.index:
            continue
        score_bin_english = int(scores[score_type]['english'] * score_mult)
        score_bin_universal = int(scores[score_type]['universal'] * score_mult)
        bins['user_english'][score_bin_english] += 1
        bins['user_universal'][score_bin_universal] += 1
        user_tweet_count = grouped_tweets.loc[user_id].tweet_count
        bins['tweet_english'][score_bin_english] -= user_tweet_count
        bins['tweet_universal'][score_bin_universal] -= user_tweet_count

    # Plot bins
    extra_y_start = min(it.chain(bins['tweet_english'].values(), bins['tweet_universal'].values())) - 1000
    extra_y_end = max(it.chain(bins['user_english'].values(), bins['user_universal'].values())) + 5000
    fig_english = bplt.figure(plot_width=500, plot_height=500, title='English Score Distr')
    fig_english.extra_y_ranges = {'tweets': bmodels.Range1d(start=extra_y_start, end=extra_y_end)}
    fig_english.add_layout(bmodels.LinearAxis(y_range_name='tweets'), 'right')
    fig_english.vbar(x=list(bins['user_english'].keys()), width=0.95, bottom=0, top=list(bins['user_english'].values()), legend='Users')
    fig_english.vbar(x=list(bins['tweet_english'].keys()), width=0.95, bottom=0, top=list(bins['tweet_english'].values()),
                     y_range_name='tweets', color='red', legend='Tweets')

    fig_universal = bplt.figure(plot_width=500, plot_height=500, title='Universal Score Distr')
    fig_universal.extra_y_ranges = {'tweets': bmodels.Range1d(start=extra_y_start, end=extra_y_end)}
    fig_universal.add_layout(bmodels.LinearAxis(y_range_name='tweets'), 'right')
    fig_universal.vbar(x=list(bins['user_universal'].keys()), width=0.95, bottom=0, top=list(bins['user_universal'].values()), legend='Users')
    fig_universal.vbar(x=list(bins['tweet_universal'].keys()), width=0.95, bottom=0, top=list(bins['tweet_universal'].values()),
                     y_range_name='tweets', color='red', legend='Tweets')
    bplt.show(blayouts.row(fig_english, fig_universal))

def filter_tweets_by_botscore(data, botscores, botscore_threshold, use_cap=False):
    score_type = 'scores' if not use_cap else 'cap'
    filtered_users = {user_id for user_id, val in botscores.items() if val[score_type]['english'] < botscore_threshold and val[score_type]['universal'] < botscore_threshold}
    for datum in data:
        datum.df = datum.df[datum.df.UserId.isin(filtered_users)]

def tweet_hist_monthly(data):
    ds = bplt.ColumnDataSource(data=dict(
        month=[datum.date_label for datum in data],
        count=[datum.df.shape[0] for datum in data]
    ))
    hover = bmodels.HoverTool(tooltips=[
        ('month', '@month'),
        ('num tweets', '@count')
    ])
    fig = bplt.figure(plot_width=900, plot_height=600, x_range=ds.data['month'],
                      x_axis_label='Month', y_axis_label='Number of Tweets', tools=[hover],
                      title='Num Tweets vs Month')
    fig.vbar(x='month', top='count', bottom=0, width=0.5, source=ds)
    bplt.show(fig)

def user_hist_monthly(data):
    ds = bplt.ColumnDataSource(data=dict(
        month=[datum.date_label for datum in data],
        count=[len(datum.df.UserId.unique()) for datum in data]
    ))
    hover = bmodels.HoverTool(tooltips=[
        ('month', '@month'),
        ('num users', '@count')
    ])
    fig = bplt.figure(plot_width=900, plot_height=600, x_range=ds.data['month'],
                      x_axis_label='Month', y_axis_label='Number of Users', tools=[hover],
                      title='Num Users vs Month')
    fig.vbar(x='month', top='count', bottom=0, width=0.5, source=ds)
    bplt.show(fig)

def process_ngrams(data):
    stopwords = set(nltk.corpus.stopwords.words('english'))
    for datum in data:
        onegrams, bigrams, trigrams = {}, {}, {}
        for row in datum.df.itertuples():
            prev1, prev2 = None, None
            for word in (x for x in row.NormalizedText if x not in stopwords):
                if word not in onegrams:
                    onegrams[word] = 0
                onegrams[word] += 1

                if prev1 is not None:
                    bigram = f'{prev1}-{word}'
                    if bigram not in bigrams:
                        bigrams[bigram] = 0
                    bigrams[bigram] += 1

                if prev2 is not None:
                    trigram = f'{prev2}-{prev1}-{word}'
                    if trigram not in trigrams:
                        trigrams[trigram] = 0
                    trigrams[trigram] += 1

                prev2 = prev1
                prev1 = word
        datum.onegrams = onegrams
        datum.bigrams = bigrams
        datum.trigrams = trigrams
    return data

def create_wordclouds(data, product_name, w=800, h=600):
    onegram_save_folder = os.path.join(ASSETS_DIR, f'WordClouds\\{product_name}\\Onegrams\\')
    if not os.path.exists(onegram_save_folder):
        os.makedirs(onegram_save_folder)
    bigram_save_folder = os.path.join(ASSETS_DIR, f'WordClouds\\{product_name}\\Bigrams\\')
    if not os.path.exists(bigram_save_folder):
        os.makedirs(bigram_save_folder)
    trigram_save_folder = os.path.join(ASSETS_DIR, f'WordClouds\\{product_name}\\Trigrams\\')
    if not os.path.exists(trigram_save_folder):
        os.makedirs(trigram_save_folder)
    for datum in data:
        wc = WordCloud(width=w, height=h).generate_from_frequencies(datum.onegrams)
        wc.to_file(os.path.join(onegram_save_folder, f'{datum.date.strftime("%Y-%m")}.png'))
        wc = WordCloud(width=w, height=h).generate_from_frequencies(datum.bigrams)
        wc.to_file(os.path.join(bigram_save_folder, f'{datum.date.strftime("%Y-%m")}.png'))
        wc = WordCloud(width=w, height=h).generate_from_frequencies(datum.trigrams)
        wc.to_file(os.path.join(trigram_save_folder, f'{datum.date.strftime("%Y-%m")}.png'))
        print(f'Generated wordclouds for {datum.date_label}.')
