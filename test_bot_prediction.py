# testing BotPredictor class

from bot_predictor import BotPredictor

import configparser

import sys

#import pandas as pd

#from tqdm import tqdm

def main():
    params = configparser.ConfigParser()  # read parameters from file
    params.read(sys.argv[1])

    bp = BotPredictor()

    twitter_app_auth = {  # twitter account credentials
            'consumer_key': params['bot_prediction']['consumer_key'],
            'consumer_secret': params['bot_prediction']['consumer_secret'],
            'access_token': params['bot_prediction']['access_token'],
            'access_token_secret': params['bot_prediction']['access_token_secret'],
            }

    # twitter node ID file format --> single column with header <node_id>
    bp.check_nodes_for_bots(params['bot_prediction']['twitter_node_ids_file'],
            params['bot_prediction']['rapid_api_keys_file'], twitter_app_auth,
            params['bot_prediction']['output_filename'])

if __name__ == "__main__":
    main()
