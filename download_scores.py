import argparse
from bs4 import BeautifulSoup
from datetime import date
import requests
import os
import sys
import time
import utils

MAIN_PREFIX = 'http://www.baseball-reference.com'
BOXES = MAIN_PREFIX + '/boxes/'

BOXSCORE_META_TABLE = 'boxscore_meta'

def __check_results(request, msg=None):
    ''' Handle the fact that baseball-reference.com returns a "200" status code
        but an error message in the page content, rather than a legitimate HTTP
        error response. '''
    if not msg:
        msg = 'Unknown error retrieving page.'
    if '404 - File Not Found' in request.text:
        print '#ERROR: %s' % msg
        sys.exit(-1)

def collect_teams_and_schedules(year, cursor):
    url = BOXES +'%d.shtml' % year
    r = requests.get(url)
    __check_results(r, msg='Unable to retrieve year:%s' % year)
    soup = BeautifulSoup(r.text)
    div = soup.find(id='page_content')
    for i in div.find_all('a'):
        info = {'team' : str(i.contents[0]),
                'schedule-link' : i['href']
               }
        print 'Gathering year:%d, Team:%s' % (year, info['team'])
        link = MAIN_PREFIX + info['schedule-link']

        query = 'INSERT INTO boxscore_meta(year, team_name, schedule_link) VALUES (%d, "%s", "%s")' %  (year, info['team'], link)
        cursor.execute(query)

def __insert_boxscore(link, team, html_dir, cursor):
    #Link includes date, strip here
    #Format http://www.baseball-reference.com/boxes/[Team ID (3)]/[Team ID (3)][INFO HERE].shtml
    strip = link.split('/')[5].split('.')[0]

    #User this as save dir
    save_path = html_dir + '/' + strip + '.shtml'
    #Format is [Team Prefix (3)][Year (4)][Month (2)][Day (2)][? (1)]
    s = strip[3:][:-1]
    year = int(s[0:4])
    month = int(s[4:6])
    day = int(s[6:])
    #Say time midnight by default
    #Go back and update with specific time when data extracted
    date_string = '%s 0:00:00' % (str(date(year, month, day)))

    #Get html and save to file
    box = requests.get(link)
    __check_results(box, msg='Unable to retrieve boxscore:%s' % link)
    try:
        with open(save_path, 'w+') as f:
            f.write(box.text.encode('utf-8'))
    except IOError:
        print 'Error saving file:', save_path
        sys.exit(-1)
    query = "INSERT INTO boxscore(team_one_name, team_two_name, link, html_path, date)"\
           " VALUES ('%s', NULL, '%s', '%s', '%s')" % (team, link, save_path, date_string)
    cursor.execute(query)

def __collect_team(url, team, cursor, html_dir):
    r = requests.get(url)
    __check_results(r, msg='Unable to retrieve team:%s' % url)
    soup = BeautifulSoup(r.text)
    for i in soup.find_all('a'):
        href = i['href']
        #All boxscores have a link that starts with /boxes/
        if href.startswith('/boxes/') and href.endswith('.shtml'):
            link = MAIN_PREFIX + href
            cursor.execute('SELECT link,team_one_name,team_two_name FROM boxscore WHERE link="%s"' % link)
            #Check if link exists already
            select_result = cursor.fetchall()
            if select_result == []:
                __insert_boxscore(link, team, html_dir, cursor)
            #If already found, this team may be team_two
            else:
                #First check that team not team one
                if select_result[0][2] == None and select_result[0][1] != team:
                    query = 'UPDATE boxscore SET team_two="%s" WHERE link="%s"' % (team, link)
                    cursor.execute(query)

def collect_team_games(year, cursor, html_dir):
    query = 'SELECT year,team_name,schedule_link from boxscore_meta where year=%d' % year
    cursor.execute(query)
    html_save = os.path.abspath(html_dir)
    #Make dir if needed
    if not os.path.isdir(html_dir):
        os.mkdir(html_dir, 0755)
    result = cursor.fetchall()
    print 'Gathering all boxscores for year:%d' % year
    total = len(result) * 1.0
    start_time = time.time()
    for (count, item) in enumerate(result):
        percent = count / total
        sys.stdout.write("\rProgress: [ %s ] %0.f%% Seconds:%f" % ('#' * int(percent * 50), percent * 100, time.time() - start_time))
        sys.stdout.flush()

        #Tuple returned (year, team, link)
        url = item[2]
        team = item[1]
        __collect_team(url, team, cursor, html_save)

    sys.stdout.write('\n')

def parse_args():
    p = argparse.ArgumentParser(description='Download boxscores')
    p.add_argument('year', type=int, help='Year to download')
    p.add_argument('save_dir',
                   help='Directory to Save Results',
                   nargs='?',
                   default='boxscores/')
    p.add_argument('--database',
                   default='boxscores.sql',
                   help='Database file to use')
    p.add_argument('--tables',
                   default='table_metadata.yml',
                   help='Table meta to follow')
    return p.parse_args()

def main():
    args = vars(parse_args())
    with utils.connect_sql(args['database']) as sql_connection:
        cursor = sql_connection.cursor()
        utils.create_tables(cursor, args['tables'])
        print 'Gathering Teams And Schedules'
        collect_teams_and_schedules(args['year'],
                                    cursor)
        print 'Gathering each teams games'
        collect_team_games(args['year'],
                           cursor,
                           args['save_dir'])

if __name__ == '__main__':
    main()
