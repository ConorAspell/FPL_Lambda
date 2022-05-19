import json
import pandas as pd
from build import get_data, get
import requests
import boto3 
import io



def lambda_handler(event, context):
    players_df, fixtures_df, game_week = get_data()
    team_id = '1892941'
    gameweek= players_df.gameweek.iat[0]
    # TODO implement

    if len(team_id) >= 6:
        team = get('https://fantasy.premierleague.com/api/entry/'+str(team_id)+'/event/'+str(gameweek-1) +'/picks/')
        players = [x['element'] for x in team['picks']]

        bank = team['entry_history']['bank']
        my_team = players_df.loc[players_df.id.isin(players)]
        potential_players = players_df.loc[~players_df.id.isin(players)]

        player_out = calc_out_weight(my_team)

        position = player_out.element_type.iat[0]
        out_cost = player_out.now_cost.iat[0]
        budget = bank + out_cost
        dups_team = my_team.pivot_table(index=['team'], aggfunc='size')
        invalid_teams = dups_team.loc[dups_team==3].index.tolist()

        potential_players=potential_players.loc[~potential_players.team.isin(invalid_teams)]
        potential_players=potential_players.loc[potential_players.element_type==position]
        potential_players = potential_players.loc[potential_players.now_cost<=budget]

        player_in = calc_in_weights(potential_players)
        return player_in.to_dict("records")

def calc_out_weight(players):
    players['weight'] = 100
    players['weight']-= players['diff']
    players['weight']-= players['form'].astype("float")*10
    players['weight']+= (100 - players['chance_of_playing_this_round'].astype("float"))*0.2
    players.loc[players['element_type'] ==1, 'weight'] -=10
    players.loc[players['weight'] <0, 'weight'] =0

    return players.sample(1, weights=players.weight)

def calc_in_weights(players):
    players['weight'] = 1
    players['weight'] += players['diff']
    players['weight'] += players['form'].astype("float")*10
    players['weight'] -= (100 - players['chance_of_playing_this_round'].astype("float"))*0.2
    players.loc[players['weight'] <0, 'weight'] =0

    return players.sample(1, weights=players.weight)

def get_df(bucket, key):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))
    return df

def get(url):
    response = requests.get(url)
    return json.loads(response.content)
