from contextlib import contextmanager
import logging
import sqlite3

log = logging.getLogger(__name__)

# Pre-set hardcoded values

# For general use
URL_PREFIX = 'http://www.baseball-reference.com'
BOXES_URL = URL_PREFIX + '/boxes/'


# For reading html format
META_FIELD_ORDER = ['date', 'field', 'attendance']
META_ADDITIONAL_ARGS = ['game_time']
TEAM_ORDER = ['score', 'record', None, None, 'manager']
TEAM_ORDER_ARGS = ['team_shortname']
PITCHER_ORDER = ['winning_pitcher', 'losing_pitcher', 'saving_pitcher']

BOXSCORE_SKIP = ['batting_avg', 'onbase_perc', 'slugging_perc',
                 'onbase_plus_slugging', 'wpa_bat_neg', 'details',
                 'wpa_bat_pos', 'wpa_bat', 're24_bat', 'leverage_index_avg',
                 'earned_run_avg', 'inplay_unk', 'game_score',
                 'inherited_score', 'wpa_def', 'leverage_index_avg',
                 're24_def', 'blank']

SMALL_TEXT_SKIP = ['tb', 'teamrisp']

BOXSCORE_HITTING = {
    # HTML TAG : DATABSE COLUMN
    'a' : 'assists',
    'bb' : 'bases_on_balls',
    'h' : 'hits',
    'pa' : 'plate_appearances',
    'so' : 'hitting_strike_outs',
    'rbi' : 'runs_batted_in',
    'r' : 'runs_scored',
    'po' : 'put_outs',
    'strikes_total' : 'strikes_seen',
    'pitches' : 'pitches_seen',
}

BOXSCORE_PITCHING = {
    # HTML TAG: DATABASE COLUMN
    'ip' : 'innings_pitched',
    'h' : 'hits_allowed',
    'r' : 'runs_allowed',
    'er' : 'earned_runs_allowed',
    'bb' : 'bases_on_balls_allowed',
    'so' : 'pitching_strike_outs',
    'hr' : 'home_runs_allowed',
    'batters_faced' : 'batters_faced',
    'pitches' : 'pitches_thrown',
    'strikes_total' : 'strikes_total',
    'strikes_contact' : 'strikes_contact',
    'strikes_swinging' : 'strikes_swinging',
    'strikes_looking' : 'strikes_looking',
    'inplay_gb_total' : 'ground_balls',
    'inplay_fb_total' : 'fly_balls',
    'inplay_ld' : 'line_drives',
    'inherited_runners' : 'inherited_runners',
}

EXTRA_PLAYER = {
    # HTML TAG: DATABASE COLUMN
    '2b' : 'doubles',
    '3b' : 'triples',
    'hr' : 'home_runs',
    'ibb' : 'intentional_bases_on_balls',
    'hbp' : 'hit_by_pitch',
    'gidp' : 'grounded_into_double_play',
    'dp' : 'double_plays',
    'errors' : 'errors',
}

EXTRA_TEAM = {
    # HTML TAG: DATABASE COLUMN
    'teamlob' : 'left_on_base',
}

MONTH_NAME = {
    'January' : 1,
    'February' : 2,
    'March' : 3,
    'April' : 4,
    'May' : 5,
    'June' : 6,
    'July' : 7,
    'August' : 8,
    'September' : 9,
    'October' : 10,
    'November' : 11,
    'December' : 12,
}

DATABASE_SCHEMA = {
    "tables": [
        {
            "name": "boxscore_meta",
            "columns": [
                "team_name VARCHAR(125)",
                "year INTEGER",
                "schedule_link VARCHAR(1023) PRIMARY KEY"
            ]
        },
        {
            "name": "manager",
            "columns": [
                "first_name VARCHAR(511)",
                "last_name VARCHAR(511)",
                "link VARCHAR(1023) PRIMARY KEY"
            ]
        },
        {
            "name": "player",
            "columns": [
                "first_name VARCHAR(511)",
                "last_name VARCHAR(511)",
                "link VARCHAR(1023) PRIMARY KEY"
            ]
        },
        {
            "name": "boxscore",
            "columns": [
                "team_one_name VARCHAR(125)",
                "team_two_name VARCHAR(125)",
                "date DATETIME",
                "link VARCHAR(1023) PRIMARY KEY",
                "html_path VARCHAR(1023)",
                "attendance INTEGER",
                "game_time INTEGER",
                "field_used VARCHAR(255)",
                "away_team_game_record INTEGER",
                "home_team_game_record INTEGER",
                "winning_pitcher VARCHAR(1023)",
                "losing_pitcher VARCHAR(1023)",
                "saving_pitcher VARCHAR(1023)",
                "weather_description VARCHAR(1023)"
            ]
        },
        {
            "name": "team_game_record",
            "columns": [
                "id INTEGER PRIMARY KEY AUTOINCREMENT",
                "team_name VARCHAR(125)",
                "team_link VARCHAR(1023)",
                "score INTEGER",
                "hits INTEGER",
                "errors INTEGER",
                "left_on_base INTEGER",
                "manager VARCHAR(1023)"
            ]
        },
        {
            "name": "inning_score",
            "columns": [
                "game_record_id INTEGER",
                "inning INTEGER",
                "score INTEGER"
            ]
        },
        {
            "name": "player_game_record",
            "columns": [
                "game_record_id INTEGER",
                "player_link VARCHAR(1023)",
                "fielding_pos VARCHAR(7)",
                "batting_pos INTEGER",
                "at_bats INTEGER",
                "runs_scored INTEGER",
                "hits INTEGER",
                "runs_batted_in INTEGER",
                "bases_on_balls INTEGER",
                "intentional_bases_on_balls INTEGER",
                "hit_by_pitch INTEGER",
                "hitting_strike_outs INTEGER",
                "grounded_into_douple_play INTEGER",
                "plate_appearances INTEGER",
                "doubles INTEGER",
                "triples INTEGER",
                "home_runs INTEGER",
                "strikes_seen INTEGER",
                "pitches_seen INTEGER",
                "innings_pitched_whole INTEGER",
                "innings_pitched_part INTEGER",
                "hits_allowed INTEGER",
                "runs_allowed INTEGER",
                "earned_runs_allowed INTEGER",
                "home_runs_allowed INTEGER",
                "bases_on_balls_allowed INTEGER",
                "pitching_strike_outs INTEGER",
                "batters_faced INTEGER",
                "pitches_thrown INTEGER",
                "strikes_total INTEGER",
                "strikes_contact INTEGER",
                "strikes_swinging INTEGER",
                "strikes_looking INTEGER",
                "ground_balls INTEGER",
                "fly_balls INTEGER",
                "line_drives INTEGER",
                "inherited_runners INTEGER",
                "put_outs INTEGER",
                "assists INTEGER",
                "double_plays INTEGER",
                "errors INTEGER"
            ]
        },
        {
            "name": "manager",
            "columns": [
                "first_name VARCHAR(511)",
                "last_name VARCHAR(511)",
                "link VARCHAR(1023) PRIMARY KEY"
            ]
        },
        {
            "name": "player",
            "columns": [
                "first_name VARCHAR(511)",
                "last_name VARCHAR(511)",
                "link VARCHAR(1023) PRIMARY KEY"
            ]
        },
        {
            "name": "umpire_game_record",
            "columns": [
                "name VARCHAR(511)",
                "boxscore_link VARCHAR(1023)",
                "position VARCHAR(15)"
            ]
        }
    ]
}


# Common functions
def __create_table(cursor, table):
    log.info("Creating table:%s" % table)
    column_list = table['columns']
    table_name = table['name']
    query = 'CREATE TABLE '
    query += table_name + '('
    query += ', '.join(column for column in column_list)
    query += ')'
    try:
        cursor.execute(query)
        log.debug("Created table:%s" % table_name)
    except sqlite3.OperationalError as e:
        # Assume table exists
        log.error("Cannot create table:%s, %s" % (table_name, e))

def create_tables(cursor):
    tables = DATABASE_SCHEMA['tables']
    for table in tables:
        __create_table(cursor, table)

@contextmanager
def connect_sql(database_file):
    with sqlite3.connect(database_file) as conn:
        try:
            yield conn
        finally:
            conn.commit()
