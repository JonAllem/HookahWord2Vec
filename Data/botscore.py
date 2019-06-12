"""
Script to score users as bot or not using botometer.
"""
import glob
import os
import pickle
import sys

import botometer

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

class UserScoreCache(object):
    """
    Implements a cache to store the bot scores into a file.
    """
    def __init__(self, product_group, date_label):
        os.makedirs(os.path.join(CURRENT_DIR, 'BotScores\\'), exist_ok=True)

        self._scores = {}
        self._cache_file = os.path.join(CURRENT_DIR, f'BotScores\\UserBotScores-{product_group}-{date_label}.pickle')
        for filename in glob.glob(os.path.join(CURRENT_DIR, 'BotScores\\UserBotScores-*.pickle')):
            with open(filename, 'rb') as file_handle:
                self._scores = {**self._scores, **pickle.load(file_handle)}

        self._bad_users = set()
        self._exception_file = os.path.join(CURRENT_DIR, f'BotScores\\Exception-{product_group}-{date_label}.tsv')
        for filename in glob.glob(os.path.join(CURRENT_DIR, 'BotScores\\Exception-*.pickle')):
            with open(filename, 'r', encoding='utf-8') as file_handle:
                for line in file_handle:
                    user_id = int(line.split()[0])
                    self._bad_users.add(user_id)
        self._exception_file_handle = open(self._exception_file, 'a', encoding='utf-8')
        self._new_scores_count = 0

    def __getitem__(self, user_id):
        return self._scores[user_id]

    def __setitem__(self, user_id, scores):
        self._scores[user_id] = scores
        self._new_scores_count += 1
        if self._new_scores_count >= 1800:
            self._flush()
            self._new_scores_count = 0

    def reject(self, user_id, message):
        """
        Add user id to list of bad users that threw an exception.
        """
        self._bad_users.add(user_id)
        self._exception_file_handle.write(f'{user_id}\t{message}\n')

    def __contains__(self, user_id):
        return user_id in self._scores or user_id in self._bad_users

    def __enter__(self):
        self._exception_file_handle.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._flush()
        self._exception_file_handle.__exit__(exc_type, exc_value, traceback)
        return False

    def _flush(self):
        with open(self._cache_file, 'wb') as file_handle:
            pickle.dump(self._scores, file_handle)


def main(product_group, start_month, end_month):
    """
    Read a data frame list created in `clean_data.py` and score all users in it.
    """
    monthly_tweets_pickle_file = os.path.join(CURRENT_DIR, f'Pickles\\ProductGroupedDFs-{product_group}.pickle')
    with open(monthly_tweets_pickle_file, 'rb') as file_handle:
        data = pickle.load(file_handle)
        for i, datum in enumerate(data):
            datum.users = set(datum.df.UserId.unique())
            for j in range(i):
                datum.users.difference_update(data[j].users)

    bom = botometer.Botometer(
        mashape_key='<mashape_key>',
        consumer_key='<twitter_consumer_key>',
        consumer_secret='<twitter_consumer_secret>',
        botometer_api_url='https://botometer-pro.p.mashape.com',
        wait_on_ratelimit=True
    )
    print(f'Starting to process {monthly_tweets_pickle_file}')
    for datum in data[start_month:end_month]:
        print(f'Processing {datum.date_label}')
        with UserScoreCache(product_group, datum.date_label) as cache:
            for i, user_id in enumerate(datum.users):
                if user_id not in cache:
                    try:
                        score_dict = bom.check_account(user_id)
                        cache[user_id] = {
                            'cap': score_dict['cap'],
                            'scores': {
                                'english': score_dict['display_scores']['english'],
                                'universal': score_dict['display_scores']['universal']
                            }
                        }
                        if i % 50 == 0:
                            print(f'Processed {i} users.')
                    #pylint: disable=W0703
                    except Exception as e:
                        print(f'Exception on user {user_id}')
                        cache.reject(user_id, str(e))
        print(f'Finished {datum.date_label}')


if __name__ == '__main__':
    product_group = sys.argv[1]
    start_month = int(sys.argv[2])
    end_month = int(sys.argv[3])
    main(product_group, start_month, end_month)
