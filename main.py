from pbpstats.client import Client
from pbpstats.resources.enhanced_pbp import FieldGoal, Foul, FreeThrow, Rebound

import pandas as pd
# This python script converts the pbpstats objects into a csv
# for easier data processing.

output_folder = "data/out"
input_folder = "data/in"

def make_season_dataframe(client, season_games):
    all_data = []
    for game in season_games:
        game_details = client.Game(game.game_id).enhanced_pbp.items
        all_data.append(get_dataframe_from_game(game_details))


    return pd.concat(all_data)



def get_dataframe_from_game(game_details):
    all_data = []
    columns = ["game_id",
               "description",
               "period",
               "seconds_remaining_period",
               "home_score",
               "away_score",
               "new_possession",
               "is_bonus"]
    fg_columns = [
        "fga",
        "distance",
        "made_fga",
        "shot_value",
        "fga_player_id"
    ]
    ft_columns = [
        "fta",
        "made_fta",
        "ft_player_id",
    ]
    reb_columns = [
        "reb",
        "oreb",
        "reb_player_id"
    ]
    for item in game_details:
        try:
            home_score = item.home_score
        except AttributeError:
            home_score = 0
        try:
            away_score = item.away_score
        except AttributeError:
            away_score = 0

        base_data = [
            item.game_id,
            item.description,
            item.period,
            item.seconds_remaining,
            home_score,
            away_score,
            1 if item.count_as_possession else 0,
            1 if item.is_penalty_event() else 0,
        ]
        fg_data = get_fg_data(item)
        ft_data = get_ft_data(item)
        reb_data = get_reb_data(item)
        base_data = base_data + fg_data + ft_data + reb_data
        all_data.append(base_data)
    return pd.DataFrame(all_data, columns=columns+fg_columns+ft_columns+reb_columns)

def get_fg_data(item):
    if isinstance(item,FieldGoal):
        fg_data = [1,
                   item.distance,
                   1 if item.is_made else 0,
                   item.shot_value,
                   item.player1_id]
        return fg_data
    else:
        return [0, None, None, None, None]

def get_ft_data(item):
    if isinstance(item,FreeThrow):
        ft_data = [1, 1 if item.is_made else 0, item.player1_id]
        return ft_data
    else:
        return [0, None, None]

def get_reb_data(item):
    if isinstance(item,Rebound) and item.is_real_rebound:
        reb_data = [1,
                    1 if item.oreb else 0,
                    item.player1_id]
        return reb_data
    else:
        return [0, None, None]



if __name__ == '__main__':
    for year in ["2016-17", "2017-18", "2018-19", "2019-20"]:
        settings = {
            "dir": "data/in/response_data",
            "Games": {"source": "file", "data_provider": "data_nba"},
            "Possessions": {"source": "file", "data_provider": "stats_nba"},
            "EnhancedPbp": {"source": "file", "data_provider": "stats_nba"},
        }
        client = Client(settings)
        season = client.Season("nba", year, "Regular Season")

        df = make_season_dataframe(client, season.games.items)
        df.to_csv("data/out/%s_pbp.csv.gz" % year,  index=False, compression='gzip')
    # Read pbpstats data
    # Produce a dataframe from the data
    # dump the csv of the data

