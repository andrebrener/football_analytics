import os

from datetime import date

# Variables

START_DATE = date(2018, 10, 7)
END_DATE = date(2018, 10, 7)

# END_DATE = date.today() - timedelta(1)
# START_DATE = END_DATE - timedelta(3)

# Leagues
LEAGUES = [93]
SEASONS = [22]

# Relative Paths
PYTHON_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = PYTHON_DIR[:PYTHON_DIR.index('python')]
REPO_DIR = PROJECT_DIR[:PROJECT_DIR.index('get_data')]
LIB_COMMON_DIR = os.path.join(REPO_DIR, 'lib_common')
XG_DIR = os.path.join(REPO_DIR, 'expected_goals', 'python')
UD_DIR = os.path.join(REPO_DIR, 'update_data')

# TPLS
GAMES_TPL = 35
EVENTS_TPL = 36
TEAMS_TPL = 42
PLAYERS_TPL = 40
TEAMS_INFO_TPL = 32
PLAYERS_INFO_TPL = 11
PLAYERS_IN_TEAM_TPL = 5

# URLS
GAMES_ID_URL = '&tpl={}&tournament_id={}&season_id={}&date_start={}&date_end={}&lang_id=1&format=json'
GAME_INFO_URL = '&tpl={}&match_id={}&lang_id=1&format=json'
TEAMS_INFO_URL = '&tpl={}&tournament_id={}&season_id={}&lang_id=1&format=json'
PLAYERS_INFO_URL = '&tpl={}&player_id={}&lang_id=1&format=json'
PLAYERS_IN_TEAM_URL = '&tpl={}&team_id={}&lang_id=1&format=json'

LEAGUES_URL = '{}leagues/?accept=json&api_key={}&sig={}'

# Standard tables
STANDARD_TABLES = [
    'actions', 'body_parts', 'standards', 'positions', 'seasons',
    'attack_status', 'attack_types', 'possession_types'
]

STATS_TABLES = [
    'teams_info', 'games_info', 'players_info', 'teams_stats', 'players_stats',
    'game_events'
]

# Stats for views

PLAYERS_INFO_COLS = {
    'id': 'id',
    'firstname': 'firstname',
    'lastname': 'lastname',
    'birthday': 'birthday',
    'country1_id': 'country_id',
    'position2_id': 'position2_id',
    'position3_id': 'position3_id',
    'foot_id': 'foot_id',
    'height': 'height',
    'weight': 'weight',
    'created_on': 'created_on'
}
GAME_INFO_COLS = {
    'id': 'game_id',
    'tournament_id': 'tournament_id',
    'season_id': 'season_id',
    'round_id': 'round_id',
    'team1_id': 'home_team_id',
    'team2_id': 'away_team_id',
    'team1_score': 'home_team_score',
    'team2_score': 'away_team_score',
    'status_id': 'status_id',
    'stadium_id': 'stadium_id',
    'duration': 'duration',
    'match_date': 'date',
}

GAME_EVENTS_COLS = {
    'game_id': 'game_id',
    'action_id': 'action_id',
    'attack_status_id': 'attack_status_id',
    'attack_type_id': 'attack_type_id',
    'body_id': 'body_id',
    'half': 'half',
    'len': 'len',
    'player_id': 'player_id',
    'opponent_id': 'second_player_id',
    'pos_dest_x': 'pos_dest_x',
    'pos_dest_y': 'pos_dest_y',
    'pos_x': 'pos_x',
    'pos_y': 'pos_y',
    'position_id': 'position_id',
    'possession_id': 'possession_id',
    'possession_number': 'possession_number',
    'possession_team_id': 'possession_team_id',
    'second': 'video_seconds',
    'standart_id': 'play_standard_id',
    'team_id': 'team_id',
    'rival_team_id': 'rival_team_id',
    'home_away': 'home_away',
    'game_minutes': 'game_minutes',
    'game_seconds': 'game_seconds',
    'sequence_number': 'sequence_number',
    'formation': 'formation',
    'is_pass': 'is_pass',
    'is_shot': 'is_shot',
    'is_change_formation': 'is_change_formation',
    'zone_id': 'zone_id',
}

TEAMS_STATS_COLS = {
    'game_id': 'game_id',
    'team_id': 'team_id',
    'attacks_with_shots_counter_attacks_%': 'shots_from_counter_attacks_perc',
    'attacks_with_shots_positional_attacks_%': 'shots_from_positional_attacks_perc',
    'attacks_with_shots_set_pieces_attacks_%': 'shots_from_set_pieces_attacks_perc',
    'accurate_passes_%_forward_(capture_angle_180_degrees)': 'passes_forward_perc',
    'ball_possession_%': 'ball_possession',
    'turnovers_in_own_half': 'turnovers_in_own_half',
    'turnovers_in_opponent`s_half': 'turnovers_in_opp_half',
    'ball_recoveries_after_turnovers_within_10_sec': 'ball_recoveries_in_10_sec',
    'ball_recoveries_after_turnovers_within_10_sec_in_opponent`s_half': 'ball_recoveries_in_10_sec_in_opp_half',
    'team_pressing_%_successful': 'team_pressing_successful_perc',
    'attacking_mentality_index': 'attacking_mentality_index',
    'average_length_of_passes_m': 'average_length_of_passes',
    'actions_in_opp_third': 'actions_in_opp_third',
    'average_distance_to_the_goal_at_ball_recoveries': 'average_distance_to_the_goal_at_ball_recoveries',
    'ball_possession_15_45_sec_number_number': 'ball_possession_15_45_sec_number',
    'ball_possession_5_15_sec_number_number': 'ball_possession_5_15_sec_number',
    'ball_possession_>45_sec_number_number': 'ball_possession_45_more_sec_number',
    'ball_possession_<5_sec_number_number': 'ball_possession_0_5_sec_number',
    'ball_possession_time_in_the_final_third_of_the_field': 'possession_time_in_the_final_third',
    "opponent's_ball_possession_sec": 'opp_possession_sec',
    'ball_possession_sec': 'own_possession_sec',
    'shots': 'shots',
    'team_pressing': 'team_pressing',
    'accurate_passes_in_the_opposition_half': 'passes_in_the_opposition_half',
    'goals_conceded': 'goals_conceded',
    'goals_scored_result': 'goals',
    'penalty_attacks_with_goals': 'penalty_goals',
    'other_stats': 'other_stats',
}

PLAYERS_STATS_COLS = {
    'game_id': 'game_id',
    'team_id': 'team_id',
    'player_id': 'player_id',
    'other_stats': 'other_stats',
    '%_of_accurate_passes': 'pass_eff',
    'attacking_passes_accurate': 'attacking_passes_accurate',
    'accurate_passes_%_forward_(capture_angle_180_degrees)': 'passes_forward_perc',
    'air_challenges_%': 'air_challenges_eff',
    'challenges_in_defence_won_%': 'def_challenges_eff',
    'ball_recoveries': 'ball_recoveries',
    'ball_recovery_in_opp_half': 'ball_recovery_in_opp_half',
    'defensive_challenges_successful': 'defensive_challenges_successful',
    'dribbles_successful': 'dribbles',
    'foul': 'fouls_commited',
    'foul_suffered': 'foul_suffered',
    'goals': 'goals',
    'interceptions': 'interceptions',
    'interceptions_in_opp_half': 'interceptions_in_opp_half',
    'lost_balls': 'lost_balls',
    'minutes_played': 'minutes_played',
    'passes_accurate': 'passes',
    'short_passes_accurate': 'short_passes',
    'long_passes_accurate': 'long_passes',
    'medium_passes_accurate': 'medium_passes',
    'accurate_passes_into_the_final_third_of_the_pitch': 'passes_into_final_third',
    'passes_into_the_opp_box_accurate': 'passes_into_opp_box',
    'shots': 'shots',
    'red_cards': 'red_cards',
    'yellow_cards': 'yellow_cards',
    'tackles_successful': 'tackles',
    'was_in_first_11_including_short_data_information': 'in_first_11',
    'was_substituted_in_including_short_data_information': 'sub_in',
    'was_substituted_out_including_short_data_information': 'sub_out'
}

# Action Codes

ACTION_CODES = {
    'shot': [4000, 8010, 4010, 4020, 4030, 4040, 4050],
    'pass': [28010, 28011, 28012, 28030, 1011, 1020, 1031, 1061, 1070]
}

FORMATION_CODES = [
    15020, 15030, 15040, 15050, 15060, 15070, 15080, 15090, 15100, 15110
]

POSITION_CODES = [
    16120, 16130, 16140, 16150, 16160, 16220, 16230, 16240, 16250, 16260,
    16310, 16320, 16330, 16340, 16350, 16360, 16420, 16430, 16440, 16450,
    16460, 16520, 16530, 16540, 16550, 16560
]

# Start 1st & 2nd half
INITIAL_CODES = [18010, 18020]
# Non attacking pass
KICKOFF_CODES = [1021]

# Removing codes:
# tactical points
# average positions
# substitute player

REMOVE_CODES = [29010, 25000, 25010, 25020, 161000]
