from contextlib import contextmanager
import logging
import os
import sqlite3
import yaml

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

def create_tables(cursor, tables_template):
    with open(os.path.abspath(tables_template)) as f:
        data = yaml.load(f)
        tables = data['tables']
        for table in tables:
            __create_table(cursor, table)

@contextmanager
def connect_sql(database_file):
    with sqlite3.connect(database_file) as conn:
        try:
            yield conn
        finally:
            conn.commit()
