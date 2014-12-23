import argparse
from bs4 import BeautifulSoup
import utils
import sqlite3

MAIN_PREFIX = 'http://www.baseball-reference.com'

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


MONTH_PREFIX = {
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

def __check_blank(stringy):
    if stringy == '':
        return True
    for i in stringy:
        if i == '\n':
            continue
        if str(i) != ' ':
            return False
    return True

def __check_same(stringy1, stringy2):
    lower1 = stringy1.lower().replace(" ", "")
    lower2 = stringy2.lower().replace(" ", "")
    return lower1 == lower2

def __remove_paren_args(stringy):
    #Remove all parenthesis ()
    #And items with those
    while True:
        try:
            x = stringy.index('(')
            y = stringy.index(')')
            stringy = stringy[:x] + stringy[y+1:]
        except ValueError:
            return stringy

def __first_last_name(stringy):
    items = stringy.split(' ')
    first = items[0]
    last = ' '.join(i for i in items[1:])
    return first, last

def next_element(item, repeat=1):
    for _ in range(repeat):
        item = item.next_element
        while item == '\n':
            item = item.next_element
    return item

def next_sibling(item, repeat=1):
    for _ in range(repeat):
        item = item.next_sibling
        while item == '\n':
            item = item.next_sibling
    return item

def __ensure_player(cursor, name, link):
    first_name, last_name = __first_last_name(name)
    query = 'INSERT INTO player(first_name, last_name, link) VALUES' \
            ' ("%s","%s","%s")' %  (first_name, last_name, link)
    try:
        cursor.execute(query)
    except sqlite3.IntegrityError:
        # Player already exists
        return

def __ensure_manager(cursor, name, link):
    first_name, last_name = __first_last_name(name)
    query = 'INSERT INTO manager(first_name, last_name, link)' \
            ' VALUES ("%s", "%s", "%s")' %  (first_name, last_name, link)
    try:
        cursor.execute(query)
    except sqlite3.IntegrityError:
        # Manager already exists
        return

def __ensure_player_game(cursor, player_link, game_record):
    #Check if player_game_record exists
    query = 'SELECT * FROM player_game_record WHERE player_link="%s"' \
            ' AND game_record_id=%d' %  (player_link, game_record)
    cursor.execute(query)
    result = cursor.fetchall()
    if len(result) == 0:
        #If not create
        query = 'INSERT INTO player_game_record(game_record_id, player_link)'\
                ' VALUES(%d, "%s")' % (game_record, player_link)
        cursor.execute(query)

def __update_player_record(cursor, player_link, game_record,
                           key, value):
    db_key = EXTRA_PLAYER[key]
    # Update player record
    query = 'UPDATE player_game_record SET %s=%s' % (db_key, value)
    query += ' WHERE player_link="%s" AND'\
            ' game_record_id=%d' % (player_link, game_record)
    cursor.execute(query)

def __update_team_record(cursor, game_record, key, value):
    db_key = EXTRA_TEAM[key]
    # Update team record
    query = 'UPDATE team_game_record SET %s=%s' % (db_key, value)
    query += ' WHERE id=%d' % game_record
    cursor.execute(query)

def __get_proper_time(time_data):
    date_data = time_data.split(',')
    month_day = date_data[1].split(' ')
    month = MONTH_PREFIX[month_day[1]]
    day = int(month_day[-1])
    year = int(date_data[2])
    date_string = '%d-%d-%d' % (year, month, day)
    # Time format [Hour]:[Minute][AM/PM]
    time_string = date_data[3]
    # Some older boxscores have no time param, check here
    if __check_blank(time_string):
        date_string += ' 00:00:00'
    else:
        time_split = time_string.split(':')
        hour = int(time_split[0])
        m = time_split[1]
        minute = int(m[:2])
        am_pm = m[2:]
        if am_pm == 'PM':
            hour += 12
        date_string += ' %d:%d:00' % (hour, minute)
    return date_string

def __get_proper_attendance_gametime(data):
    gt = None
    ga = None
    #Format of attendance is; Attendance : [attendance], Time of Game: [Time]
    att_split = data.split(',')
    #Get game time
    #format ; Time of Game: [Hour]:[MIN]
    game_time = ':'.join(i for i in att_split[-1:]).replace('Time of Game:', '')
    game_time = game_time.replace(' ', '')
    game_split = game_time.split(':')
    hour = int(game_split[0])
    minute = int(game_split[1])
    total_mins = hour * 60 + minute
    gt = total_mins

    #Get Attendance
    #Format ; Attendance: [Not Given | INT]
    attendance = ','.join(i for i in att_split[:-1])
    num = attendance.split(':')[1]
    if __check_same(num, 'not given'):
        ga = None
    else:
        ga = int(num.replace(',', ''))
    return ga, gt

def __parse_meta(data):
    meta = dict()
    # Find all data in divs
    # match this against META ORDER
    for (count, div) in enumerate(data.find_all('div')):
        try:
            meta[META_FIELD_ORDER[count]] = div.contents[0]
        except IndexError:
            break
    for arg in META_ADDITIONAL_ARGS:
        meta[arg] = None
    # Format of date is;[Day of Week], [Month (3)] [Day (2)], [Year (4)], [Time]
    # So for example := Mon, APR 3, 2014, 1:00 PM
    meta['date'] = __get_proper_time(meta['date'])

    meta['attendance'], meta['game_time'] = \
        __get_proper_attendance_gametime(meta['attendance'])

    #Format of filed is; , [STADIUM NAME]
    field = meta['field'].replace(',', '')
    if field[0] == ' ':
        field = field[1:]
    meta['field'] = field
    return meta

def __team_data(data, cursor):
    team = dict()
    #Team name always a span
    team_shortname = data.find('span').contents[0]
    #Get info from div in TEAM ORDER
    for (count, i) in enumerate(data.find_all('div')):
        if TEAM_ORDER[count]:
            team[TEAM_ORDER[count]] = i.contents[0]
    score = int(team['score'])
    manager = team['manager']
    #Get specific manager information
    manager_url = MAIN_PREFIX + manager.attrs['href']
    manager_name = manager.contents[0]

    #Check for manager before continuing
    __ensure_manager(cursor, manager_name, manager_url)

    #Create a game record
    query = 'INSERT INTO team_game_record(team_name, score, manager) VALUES' \
            ' ("%s", %d, "%s")' % (team_shortname, score, manager_url)
    cursor.execute(query)
    #Return primary id to update boxscore table
    return cursor.lastrowid

def __parse_linescore(data_list, record_id, cursor):
    #Parse specific information from linescore
    #Team url always ref of first object
    team_url = MAIN_PREFIX + next_element(data_list[0]).attrs['href']

    #Remove blanks to only get score numbers
    #Then create one entry for each inning
    innings = data_list[1]
    inn = innings.split(' ')
    best_innings = []
    for i in inn:
        if __check_blank(i):
            continue
        if i == 'X':
            continue
        best_innings.append(int(i))
    for (count, i) in enumerate(best_innings):
        query = 'INSERT INTO inning_score(game_record_id, inning, score) '\
                'VALUES (%d, %d, %d)' % (record_id, count + 1, i)
        cursor.execute(query)

    #Format here [Runs][Hits][Errors]
    #Get only ints and grab hits and errors
    #Runs already collected
    other = data_list[2].next_element
    o = other.split(' ')
    o = [int(i) for i in o if not __check_blank(i)]
    hits = o[1]
    errors = o[2]

    query = 'UPDATE team_game_record SET team_link="%s",hits=%d,errors=%d '\
            'WHERE ID=%d' % (team_url, hits, errors, record_id)
    cursor.execute(query)

def generate_page_meta(page_data, cursor, boxscore_link):
    # Find out of town table
    out_of_town = page_data.find('table', {'class' : 'stats_table'})
    # Current game is always next table after
    current_meta = next_sibling(out_of_town)
    # Then meta data always two elements after
    data = next_element(current_meta, repeat=2)
    # Parse that data
    meta = __parse_meta(data)
    query = 'UPDATE boxscore set date="%s",attendance=%s,game_time=%s,'\
            'field_used="%s" where link="%s"' % (meta['date'], meta['attendance'],\
             meta['game_time'], meta['field'], boxscore_link)
    query = query.replace('None', 'NULL')
    cursor.execute(query)

    #Get all of the team data
    team_meta = current_meta.find_all('td', {'align' : 'center'})[0]
    away_team = next_element(team_meta, repeat=2)
    home_team = next_sibling(away_team, repeat=2)

    #Parse data into team record
    #Update boxscore with both records
    away = __team_data(away_team, cursor)
    #Away returned here is the away team_game_record.id
    query = 'UPDATE boxscore SET away_team_game_record=%d where link="%s"' % \
            (away, boxscore_link)
    cursor.execute(query)

    home = __team_data(home_team, cursor)
    #Home returned here is the home team_game_record.id
    query = 'UPDATE boxscore SET home_team_game_record=%d where link="%s"' % \
            (home, boxscore_link)
    cursor.execute(query)

    #Pitching data ( W/L/S ) will be next td sibling
    pitching_data = next_sibling(home_team)
    #More interested in div on inside
    pitching_data = next_element(pitching_data)
    for (count, i) in enumerate(pitching_data.find_all('div')):
        #href will always be third item in contents
        pitcher_info = i.contents[2]
        #Name in contents, unique link in href
        name = pitcher_info.contents[0]
        url = MAIN_PREFIX + pitcher_info.attrs['href']
        __ensure_player(cursor, name, url)
        query = 'UPDATE boxscore SET %s="%s" WHERE link="%s"' % \
                (PITCHER_ORDER[count], url, boxscore_link)
        cursor.execute(query)

    #Find linescore
    linescore = team_meta.find('pre', id='linescore')
    #Remove blanks
    linescore = [i for i in linescore if not __check_blank(i)]
    #First item in contents is inning listing
    innings = linescore[0].split(' ')
    #Remove all blanks
    innings = [int(i) for i in innings if not __check_blank(i)]

    #Format always ends with [teamlink][score-info][r-h-e] for each
    #So last 6 entries always same
    score_info = linescore[-6:]
    __parse_linescore(score_info[0:3], away, cursor)
    __parse_linescore(score_info[3:6], home, cursor)
    return away, home

def __player_box(cursor, player_data, all_columns, good_columns,
                 record_id, pitcher=False, hitter=False):
    #Player hitting box scores
    #Go through all columns ( td )
    #If data useful ( a good column ), save to player dict
    player_dict = dict()
    for (count, col) in enumerate(player_data.find_all('td')):
        if all_columns[count] in good_columns:
            player_dict[all_columns[count]] = col

    #Parse the dictionary
    #Get position
    if pitcher:
        #If a pitcher box score dont bother
        pos = 'p'
    else:
        try:
            #Always last item in list
            #pos = next_element(player_dict['player'], repeat=3).lower().replace(' ', '')
            pos = player_dict['player'].contents[-1].lower().replace(' ', '')
        except TypeError:
            #TypeError here denotes a "TEAM TOTALS" row
            #Dont care so skip
            return
    #Player URL
    try:
        player_url = MAIN_PREFIX + player_dict['player'].find('a').attrs['href']
    except AttributeError:
        #If attribute error, is a team total row
        #Who cares so return
        return
    #Player name
    player_name = player_dict['player'].find('a').contents[0]
    # Ensure player already exists
    __ensure_player(cursor, player_name, player_url)
    #Check for player game record
    __ensure_player_game(cursor, player_url, record_id)

    #Start to build query
    #First arg always fucking weird
    #For hitters, at bats
    #For pitchers, innings pitched
    if hitter:
        key = 'ab'
        try:
            first = int(next_element(player_dict[key], repeat=2))
        except TypeError:
            #Pitchers/ some hitters wont have this
            first = None
        #Build query
        query = 'UPDATE player_game_record SET fielding_pos="%s",at_bats=%s,' \
                  % (pos, first)
    if pitcher:
        key = 'ip'
        try:
            first = float(next_element(player_dict[key], repeat=2))
            whole = int(first)
            part = int((first - int(first)) * 10)
            query = 'UPDATE player_game_record SET fielding_pos="%s",' \
                    'innings_pitched_whole=%d,innings_pitched_part=%d,' % (pos, whole, part)

        except TypeError:
            #Pitchers/ some hitters wont have this
            first = None
            query = 'UPDATE player_game_record SET fielding_pos="%s",' \
                    % (pos)
    #Build from rest of values
    rest_cols = set(good_columns) - set(['player'])
    if pitcher:
        rest_cols = rest_cols - set(['ip'])
    if hitter:
        rest_cols = rest_cols - set(['ab'])
    #Best dictionary from these values
    for col in rest_cols:
        try:
            player_dict[col] = int(next_element(player_dict[col]))
        #Some values will be blank
        except TypeError:
            player_dict[col] = None
    if hitter:
        query += ','.join('%s=%s' % (BOXSCORE_HITTING[m], player_dict[m]) \
                          for m in rest_cols)
    if pitcher:
        query += ','.join('%s=%s' % (BOXSCORE_PITCHING[m], player_dict[m]) \
                          for m in rest_cols)
    #Add values of which record to update
    query += ' WHERE player_link="%s" AND game_record_id=%d' % \
               (player_url, record_id)
    query = query.replace('None', 'NULL')
    cursor.execute(query)

def __generate_box_data(cursor, box_count, page_data,
                        all_columns, good_columns, record):
    #Generate data for each boxscore
    #Go through ever player record
    for player in page_data.find_all('tr', {'class' : 'normal_text'})[1:]:
        #Get each players box score
        if box_count < 2:
            __player_box(cursor, player, all_columns, good_columns,
                         record, hitter=True)
            continue
        if box_count < 4:
            __player_box(cursor, player, all_columns, good_columns,
                         record, pitcher=True)
            continue

def __parse_lineup(cursor, page_data, away_record, home_record,
                   all_columns, good_columns):
    relevant_page = page_data.find('tbody')
    for line in relevant_page.find_all('tr'):
        #Each line contains two players
        #Match using columns
        line_data = dict()
        for (count, item) in enumerate(line.find_all('td')):
            if all_columns[count] in good_columns:
                line_data[all_columns[count]] = item
        #Generate the good data
        try:
            home_url = MAIN_PREFIX + line_data['player_home'].find('a').attrs['href']
            away_url = MAIN_PREFIX + line_data['player_visitor'].find('a').attrs['href']
        except AttributeError:
            #If cant find url, result is probably blank row
            continue
        #Pitchers wont have this, so have a try catch for that
        try:
            home_bp = int(next_element(line_data['bop_home']))
        except TypeError:
            home_bp = None
        try:
            away_bp = int(next_element(line_data['bop_visitor']))
        except TypeError:
            away_bp = None

        #Update the batting positions
        query = 'UPDATE player_game_record SET batting_pos=%s WHERE player_link="%s" AND '\
                'game_record_id=%d' % (home_bp, home_url, home_record)
        query = query.replace('None', 'NULL')
        cursor.execute(query)

        query = 'UPDATE player_game_record SET batting_pos=%s WHERE player_link="%s" AND '\
                'game_record_id=%d' % (away_bp, away_url, away_record)
        query = query.replace('None', 'NULL')
        cursor.execute(query)

def box_summary(page_data, cursor, away, home):
    #First stats table always out of town scoreboard
    #Rest of box scores
    boxes = page_data.find_all('table', {'class' : 'stats_table'})[1:]
    for (count, box) in enumerate(boxes):
        all_cols = []#All columns
        good_cols = []# Columns with non-blacklisted data
        #Get bad cols to compare since most likely less than god cols
        #Find all table headers to get columns
        for col in box.find_all('th'):
            name = col.attrs['data-stat'].lower().replace(' ', '')
            all_cols.append(name)
            if name not in BOXSCORE_SKIP:
                good_cols.append(name)
        #First is always table headers
        #Rest are player boxscores
        #Go through each block ( all in trs )
        if count % 2 == 0:
            record = away
        else:
            record = home
        #Generate the box data for each player
        __generate_box_data(cursor, count, box, all_cols, good_cols, record)
        #Since lineups are in a tbody value, will not be caught in loop above
        if count == 4:
            __parse_lineup(cursor, box, away, home, all_cols, good_cols)

def __nice_player_string(player):
    # Remove whitespaces from front
    player = player.lstrip(' ')
    # Check if multiple players
    players = player.split('-')
    returned_players = []
    for p in players:
        # Remove trailing whitespaces and periods
        p = p.encode('utf-8').replace('\xc2', '').replace('\xa0', ' ')
        p = p.rstrip('.')
        p = p.strip()
        p = p.lstrip(' .')
        if p and p != 'None':
            returned_players.append(p)
    return returned_players

def __find_numbers(player):
    if 'None' in player:
        return 0
    s = ''.join(x for x in player if x.isdigit())
    if s == '':
        return 1
    return int(s)

def __find_player_url(cursor, player_names, team_record=None,
                      away_record=None, home_record=None):
    if team_record:
        # If you have a team record, query directly from that team
        query = 'select last_name, first_name,player_link  from player_game_record as r ' \
                'join player as p on r.player_link=p.link' \
                ' where game_record_id=%d' % team_record
        cursor.execute(query)
        result = cursor.fetchall()
    elif home_record or away_record:
        # If you don't, try from home and away records
        query = 'select last_name, first_name,player_link  from player_game_record as r ' \
                'join player as p on r.player_link=p.link' \
                ' where game_record_id=%d' % away_record
        cursor.execute(query)
        result = cursor.fetchall()
        team_record = away_record

    # Names will be [Initial] [Last name]
    # Look up by last name, also check initial
    # First split name
    links = []
    for name in player_names:
        n = name.encode('utf-8').replace('\xc2', '').replace('\xa0', ' ')
        split_name = n.split(' ')
        first_initial = split_name[0].lower()
        last_name = split_name[1].lower()

        # Then compare against all players in result
        for player in result:
            # Get args from query
            last = player[0].lower().replace(' ', '')
            first = player[1].lower()
            link = player[2]
            # Compare last names, then first
            if last_name == last:
                if first.startswith(first_initial):
                    links.append(link)
                    break
    return links, team_record

def __parse_subfield(subfield, cursor, away_record, home_record):
    record = None
    team = ''
    try:
        # Key denotes stat to update for a player
        # Data is a a list of all players with that stat
        key = subfield.attrs['id'].lower()
        # Find team record for key
        # 'vistor' or 'home' will be in the key_name
        if record is None:
            if 'visitor' in key:
                record = away_record
                team = 'visitor'
            elif 'home' in key:
                record = home_record
                team = 'home'
        # Remove 'home' or 'visitor' from string
        key = key.replace(team, '')
        # If not a skipable key, get the data
        if key not in SMALL_TEXT_SKIP:
            # Get all data in string
            # Any data in paren () denotes season totals
            # Don't care about those so remove
            raw_data = __remove_paren_args(next_element(subfield,
                                                        repeat=3))
            # Can have multiple entries here seperated by a ';'
            # Split to get a list, then parse
            data = raw_data.split(';')

            for player in data:
                # Find all numbers in string
                # These denote number of times happend in game
                num = __find_numbers(player)
                # Remove numbers from string
                player = player.replace(str(num), '')

                # Strip all uneccesary from player string
                players = __nice_player_string(player)
                # May not need to find player
                # Could just be number in string that was removed
                links = None
                if players:
                    links, record = __find_player_url(cursor,
                                                      players,
                                                      team_record=record,
                                                      away_record=away_record,
                                                      home_record=home_record)
                    for link in links:
                        __update_player_record(cursor, link, record,
                                               key, num)
                else:
                    __update_team_record(cursor, record, key, num)
    except KeyError:
        # If it doesnt have an id its useless
        return

def __parse_boxscore_trailers(field, cursor, boxscore_link):
    field_id = field.attrs['id']
    field_content = next_element(field, repeat=3)
    if field_id.lower() == 'umpires':
        for ump in field_content.split(','):
            ump = ump.lstrip(' ')
            ump = ump.rstrip('.')
            pos = ump.split('-')[0].lstrip(' ').rstrip(' ')
            name = ump.split('-')[1].lstrip(' ').rstrip('.').rstrip(' ')
            query = 'INSERT INTO umpire_game_record(name, position, boxscore_link)'
            query += ' VALUES ("%s", "%s", "%s")' % (name, pos, boxscore_link)
            cursor.execute(query)

    elif field_id.lower() == 'weather':
        stringy = field_content.rstrip('.')
        stringy = stringy.lstrip(' ')
        query = 'UPDATE boxscore SET weather_description="%s" WHERE link="%s"' % (stringy, boxscore_link)
        cursor.execute(query)

def parse_small_text(page_data, cursor, away_record, home_record,
                     boxscore_link):
    # Additional data such as fielding and baserunning info is in small text
    # Go through each div field
    for field in page_data.find_all('div', {'class' : 'small_text'}):
        # Each field will have additional divs
        # These divs denote values to update for players
        all_divs = field.find_all('div')

        # Record will be same for each item in subfield
        # Set to none, then only check if none
        for subfield in all_divs:
            __parse_subfield(subfield,
                             cursor,
                             away_record,
                             home_record)
        # Some fields to not have divs, such as umpires
        # If they have an id, they are still of use
        if len(all_divs) == 0 and 'id' in field.attrs:
            __parse_boxscore_trailers(field, cursor, boxscore_link)

def read_file(file_name, cursor):
    # Find html link from the file_name
    # Use this link to identify box score
    query = 'SELECT link FROM boxscore WHERE html_path LIKE "%'
    query += file_name + '%"'
    cursor.execute(query)
    result_link = cursor.fetchone()[0]
    print 'Reading data from file:%s' % file_name

    with open(file_name, 'r') as f:
        data = f.read()
    soup = BeautifulSoup(data)
    page_data = soup.find('div', id='page_content')

    print 'Getting home, away metadata'
    away, home = generate_page_meta(page_data, cursor, result_link)

    print 'Getting box summarys'
    box_summary(page_data, cursor, away, home)

    print 'Parsing small text'
    parse_small_text(page_data, cursor, away, home, result_link)

def parse_args():
    a = argparse.ArgumentParser(description='Read HTML into JSON')
    a.add_argument('file_name', help='File to read')
    a.add_argument('--database',
                   help='Database file to use',
                   default='boxscores.sql')
    a.add_argument('--table',
                   help='Tables file to use',
                   default='table_metadata.yml')
    return a.parse_args()

def main():
    args = vars(parse_args())
    with utils.connect_sql(args['database']) as sql_connection:
        cursor = sql_connection.cursor()
        utils.create_tables(cursor, args['table'])
        read_file(args['file_name'], cursor)

if __name__ == '__main__':
    main()
