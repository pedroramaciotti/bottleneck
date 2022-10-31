"""accepts a set of RapidAPI keys and calls Botometer overi
   a list of twitter node ids to check for bots """

import os.path

import pandas as pd

import botometer

from tqdm import tqdm

import requests

class BotPredictor(): 

    def check_nodes_for_bots(self, twitter_node_filename = None,
            rapidapi_key_filename = None, twitter_app_auth = None,
            bot_prediction_filename = None):

        if twitter_node_filename is None:
            raise ValueError('Twitter node IDs filename should be provided.')

        if not os.path.isfile(twitter_node_filename):
            raise ValueError('Twitter node IDs file does not exist.')

        if rapidapi_key_filename is None:
            raise ValueError('RapidAPI keys filename should be provided.')

        if not os.path.isfile(rapidapi_key_filename):
            raise ValueError('RapidAPI keys file does not exist.')

        if bot_prediction_filename is None:
            raise ValueError('Output filename should be provided.')

        if twitter_app_auth is None:
            raise ValueError('Twitter account credentials should be provided.')

        # read RapidAPI keys
        key_df = pd.read_csv(rapidapi_key_filename)
        if len(key_df.columns) != 1:
            raise ValueError('RapidAPI keys file should contain a single column.')
        if key_df.columns[0] != 'key_value':
            raise ValueError('RapidAPI keys file should have a single \'key_value\' column.')
        rapid_api_keys_lst = key_df['key_value'].unique().tolist()

        # read twitter node IDs to check for bots
        twitter_node_df = pd.read_csv(twitter_node_filename)
        if len(twitter_node_df.columns) != 1:
            raise ValueError('Twitter node IDs file should contain a single column.')

        if twitter_node_df.columns[0] != 'node_id':
            raise ValueError('Twitter node IDs file should have a single \'node_id\' column.')
        twitter_node_ids_lst = twitter_node_df['node_id'].unique().tolist()

        # initialize result structure
        bot_prediction_df = pd.DataFrame(columns = ['node_id', 'majority_lang', 'cap_english', 'cap_universal',
            'display_scores_english_astroturf', 'display_scores_english_fake_follower',
            'display_scores_english_financial', 'display_scores_english_other',
            'display_scores_english_overall', 'display_scores_english_self_declared',
            'display_scores_english_self_spammer',
            'display_scores_universal_astroturf', 'display_scores_universal_fake_follower',
            'display_scores_universal_financial', 'display_scores_universal_other',
            'display_scores_universal_overall', 'display_scores_universal_self_declared',
            'display_scores_universal_self_spammer',
            'raw_scores_english_astroturf',
            'raw_scores_english_fake_follower', 'raw_scores_english_financial',
            'raw_scores_english_other', 'raw_scores_english_overall',
            'raw_scores_english_self_declared', 'raw_scores_english_self_spammer',
            'raw_scores_universal_astroturf',
            'raw_scores_universal_fake_follower', 'raw_scores_universal_financial',
            'raw_scores_universal_other', 'raw_scores_universal_overall',
            'raw_scores_universal_self_declared', 'raw_scores_universal_self_spammer'])

        # iterate over each node and check if it is bot of not
        # leveraging the provided set of RapidAPI keys 
        bom = None
        rapid_api_key_index = 0
        twitter_node_ids_index = 0
        while twitter_node_ids_index < len(twitter_node_ids_lst):

            if bom is None:
                # no more keys: finish
                if rapid_api_key_index >= len(rapid_api_keys_lst):
                    break

                # invoke botometer with a new key
                rapidapi_key = rapid_api_keys_lst[rapid_api_key_index]
                rapid_api_key_index = rapid_api_key_index + 1
                #print(rapidapi_key, rapid_api_key_index)
                bom = botometer.Botometer(wait_on_ratelimit = True,
                        rapidapi_key = rapidapi_key, **twitter_app_auth)

            try:
                node_id_int = int(twitter_node_ids_lst[twitter_node_ids_index]) # get node ID
                #print(twitter_node_ids_index, node_id_int)
                bot_prediction = bom.check_account(node_id_int)

                twitter_node_ids_index = twitter_node_ids_index + 1 # next node

                # populate result structure for the current node
                node_id = str(node_id_int)

                majority_lang = bot_prediction['user']['majority_lang']

                cap_english = bot_prediction['cap']['english']
                cap_universal = bot_prediction['cap']['universal']

                display_scores_english_astroturf = bot_prediction['display_scores']['english']['astroturf']
                display_scores_english_fake_follower = bot_prediction['display_scores']['english']['fake_follower']
                display_scores_english_financial = bot_prediction['display_scores']['english']['financial']
                display_scores_english_other = bot_prediction['display_scores']['english']['other']
                display_scores_english_overall = bot_prediction['display_scores']['english']['overall']
                display_scores_english_self_declared = bot_prediction['display_scores']['english']['self_declared']
                display_scores_english_self_spammer = bot_prediction['display_scores']['english']['spammer']

                display_scores_universal_astroturf = bot_prediction['display_scores']['universal']['astroturf']
                display_scores_universal_fake_follower = bot_prediction['display_scores']['universal']['fake_follower']
                display_scores_universal_financial = bot_prediction['display_scores']['universal']['financial']
                display_scores_universal_other = bot_prediction['display_scores']['universal']['other']
                display_scores_universal_overall = bot_prediction['display_scores']['universal']['overall']
                display_scores_universal_self_declared = bot_prediction['display_scores']['universal']['self_declared']
                display_scores_universal_self_spammer = bot_prediction['display_scores']['universal']['spammer']

                raw_scores_english_astroturf = bot_prediction['raw_scores']['english']['astroturf']
                raw_scores_english_fake_follower = bot_prediction['raw_scores']['english']['fake_follower']
                raw_scores_english_financial = bot_prediction['raw_scores']['english']['financial']
                raw_scores_english_other = bot_prediction['raw_scores']['english']['other']
                raw_scores_english_overall = bot_prediction['raw_scores']['english']['overall']
                raw_scores_english_self_declared = bot_prediction['raw_scores']['english']['self_declared']
                raw_scores_english_self_spammer = bot_prediction['raw_scores']['english']['spammer']

                raw_scores_universal_astroturf = bot_prediction['raw_scores']['universal']['astroturf']
                raw_scores_universal_fake_follower = bot_prediction['raw_scores']['universal']['fake_follower']
                raw_scores_universal_financial = bot_prediction['raw_scores']['universal']['financial']
                raw_scores_universal_other = bot_prediction['raw_scores']['universal']['other']
                raw_scores_universal_overall = bot_prediction['raw_scores']['universal']['overall']
                raw_scores_universal_self_declared = bot_prediction['raw_scores']['universal']['self_declared']
                raw_scores_universal_self_spammer = bot_prediction['raw_scores']['universal']['spammer']

                bot_prediction_df.loc[len(bot_prediction_df)] = [str(node_id), majority_lang, cap_english, cap_universal,
                        display_scores_english_astroturf, display_scores_english_fake_follower,
                        display_scores_english_financial, display_scores_english_other,
                        display_scores_english_overall, display_scores_english_self_declared,
                        display_scores_english_self_spammer,
                        display_scores_universal_astroturf, display_scores_universal_fake_follower,
                        display_scores_universal_financial, display_scores_universal_other,
                        display_scores_universal_overall, display_scores_universal_self_declared,
                        display_scores_universal_self_spammer, raw_scores_english_astroturf,
                        raw_scores_english_fake_follower, raw_scores_english_financial,
                        raw_scores_english_other, raw_scores_english_overall,
                        raw_scores_english_self_declared, raw_scores_english_self_spammer,
                        raw_scores_universal_astroturf, raw_scores_universal_fake_follower,
                        raw_scores_universal_financial, raw_scores_universal_other,
                        raw_scores_universal_overall, raw_scores_universal_self_declared, raw_scores_universal_self_spammer]
                #print(bot_prediction_df.head())

            except requests.exceptions.HTTPError as he:
                if 'Too Many Requests for url' in str(he):
                    print('change key')
                    # no more keys: finish
                    if rapid_api_key_index >= len(rapid_api_keys_lst):
                        break

                    # invoke botometer with a new key
                    rapidapi_key = rapid_api_keys_lst[rapid_api_key_index]
                    rapid_api_key_index = rapid_api_key_index + 1
                    #print(rapidapi_key, rapid_api_key_index)
                    bom = botometer.Botometer(wait_on_ratelimit = True,
                            rapidapi_key = rapidapi_key, **twitter_app_auth)
                    continue
                else:
                    continue
            except Exception as e:
                print(str(e))
                continue

        # store results to file
        bot_prediction_df.to_csv(bot_prediction_filename, index = False)
