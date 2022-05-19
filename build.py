import requests
import pandas  as pd
import json
from datetime import datetime
columns = ['chance_of_playing_next_round', 'chance_of_playing_this_round',
 'element_type', 'ep_next',
       'ep_this',  'first_name', 'form', 'id', 'in_dreamteam',
        'now_cost', 'points_per_game',
       'second_name', 'selected_by_percent', 
        'team', 'team_code', 'total_points', 'transfers_in',
        'transfers_out',
       'value_form', 'value_season', 'web_name',      
        'influence', 'creativity', 'threat',
       'ict_index']


def get_team(ids, players):
    players =  get('https://fantasy.premierleague.com/api/bootstrap-static/')
    players_df = pd.DataFrame(players['elements'])
    teams_df = pd.DataFrame(players['teams'])
    fixtures_df = pd.DataFrame(players['events'])
    today = datetime.now().timestamp()
    fixtures_df = fixtures_df.loc[fixtures_df.deadline_time_epoch>today]
    gameweek =  fixtures_df.iloc[0].id
    players_df = players_df[columns]
    players_df.chance_of_playing_next_round = players_df.chance_of_playing_next_round.fillna(100.0)
    players_df.chance_of_playing_this_round = players_df.chance_of_playing_this_round.fillna(100.0)
    fixtures = get('https://fantasy.premierleague.com/api/fixtures/?event='+str(gameweek))
    fixtures_df = pd.DataFrame(fixtures)

    
    teams=dict(zip(teams_df.id, teams_df.name))
    players_df['team_name'] = players_df['team'].map(teams)
    fixtures_df['team_a_name'] = fixtures_df['team_a'].map(teams)
    fixtures_df['team_h_name'] = fixtures_df['team_h'].map(teams)

    home_strength=dict(zip(teams_df.id, teams_df.strength_overall_home))
    away_strength=dict(zip(teams_df.id, teams_df.strength_overall_away))

    fixtures_df['team_a_strength'] = fixtures_df['team_a'].map(away_strength)
    fixtures_df['team_h_strength'] = fixtures_df['team_h'].map(home_strength)

    fixtures_df=fixtures_df.drop(columns=['id'])
    a_players = pd.merge(players_df, fixtures_df, how="inner", left_on=["team"], right_on=["team_a"])
    h_players = pd.merge(players_df, fixtures_df, how="inner", left_on=["team"], right_on=["team_h"])

    a_players['diff'] = a_players['team_a_strength'] - a_players['team_h_strength']
    h_players['diff'] = h_players['team_h_strength'] - h_players['team_a_strength']

    players_df = a_players.append(h_players)
    return players_df

def make_out_weights(players):
    players['out_weight'] = 100
    players['out_weight']-= players['diff']
    players['out_weight']-= players['form'].astype("float")*10
    players['out_weight']+= (100 - players['chance_of_playing_this_round'].astype("float"))*0.2
    players.loc[players['element_type'] ==1, 'out_weight'] -=10
    players.loc[players['out_weight'] <0, 'out_weight'] =0
    return players

def calc_in_weights(players):
    players['in_weight'] = 1
    players['in_weight'] += players['diff']
    players['in_weight'] += players['form'].astype("float")*10
    players['in_weight'] -= (100 - players['chance_of_playing_this_round'].astype("float"))*0.2
    players.loc[players['in_weight'] <0, 'in_weight'] =0
    return players

def get_data():
    players =  get('https://fantasy.premierleague.com/api/bootstrap-static/')
    players_df = pd.DataFrame(players['elements'])
    teams_df = pd.DataFrame(players['teams'])
    fixtures_df = pd.DataFrame(players['events'])
    today = datetime.now().timestamp()
    fixtures_df = fixtures_df.loc[fixtures_df.deadline_time_epoch>today]
    gameweek =  fixtures_df.iloc[0].id
    players_df = players_df[columns]
    players_df.chance_of_playing_next_round = players_df.chance_of_playing_next_round.fillna(100.0)
    players_df.chance_of_playing_this_round = players_df.chance_of_playing_this_round.fillna(100.0)
    fixtures = get('https://fantasy.premierleague.com/api/fixtures/?event='+str(gameweek))
    fixtures_df = pd.DataFrame(fixtures)

    
    teams=dict(zip(teams_df.id, teams_df.name))
    players_df['team_name'] = players_df['team'].map(teams)
    fixtures_df['team_a_name'] = fixtures_df['team_a'].map(teams)
    fixtures_df['team_h_name'] = fixtures_df['team_h'].map(teams)

    home_strength=dict(zip(teams_df.id, teams_df.strength_overall_home))
    away_strength=dict(zip(teams_df.id, teams_df.strength_overall_away))

    fixtures_df['team_a_strength'] = fixtures_df['team_a'].map(away_strength)
    fixtures_df['team_h_strength'] = fixtures_df['team_h'].map(home_strength)

    fixtures_df=fixtures_df.drop(columns=['id'])
    a_players = pd.merge(players_df, fixtures_df, how="inner", left_on=["team"], right_on=["team_a"])
    h_players = pd.merge(players_df, fixtures_df, how="inner", left_on=["team"], right_on=["team_h"])

    a_players['diff'] = a_players['team_a_strength'] - a_players['team_h_strength']
    h_players['diff'] = h_players['team_h_strength'] - h_players['team_a_strength']

    players_df = a_players.append(h_players)
    players_df['gameweek'] = gameweek

    return players_df, fixtures_df, gameweek


def get(url):
    response = requests.get(url)
    return json.loads(response.content)

