from bs4 import BeautifulSoup
from datetime import date
import logging
import requests
import os
import sys
import sqlite3

from client import utils

log = logging.getLogger(__name__)

class DownloadClient(object):

    def __init__(self, year):
        self.year = year

    def __check_results(self, request, msg=None):
        ''' Handle the fact that baseball-reference.com returns a "200" status code
            but an error message in the page content, rather than a legitimate HTTP
            error response. '''
        if not msg:
            msg = 'Unknown error retrieving page.'
        if '404 - File Not Found' in request.text:
            log.error('#ERROR: %s' % msg)
            sys.exit(-1)

    def collect_teams_and_schedules(self, cursor):
        url = utils.BOXES_URL +'%d.shtml' % self.year
        r = requests.get(url)
        self.__check_results(r, msg='Unable to retrieve self.year:%s' % self.year)
        soup = BeautifulSoup(r.text)
        div = soup.find(id='page_content')
        for i in div.find_all('a'):
            info = {'team' : str(i.contents[0]),
                    'schedule-link' : i['href']
                   }
            log.info('Gathering info for team:%s in year:%s' % (info['team'], self.year))
            link = utils.URL_PREFIX + info['schedule-link']

            query = 'INSERT INTO boxscore_meta(year, team_name, schedule_link) VALUES (%d, "%s", "%s")' %  (self.year, info['team'], link)
            try:
                cursor.execute(query)
            except sqlite3.IntegrityError as e:
                log.error("Cannot create table:%s" % str(e))

    def __insert_boxscore(self, link, team, html_dir, cursor):
        # Link includes date, strip here
        # Format http://www.baseball-reference.com/boxes/[Team ID (3)]/[Team ID (3)][INFO HERE].shtml
        strip = link.split('/')[5].split('.')[0]

        # User this as save dir
        save_path = html_dir + '/' + strip + '.shtml'
        # Format is [Team Prefix (3)][Year (4)][Month (2)][Day (2)][? (1)]
        s = strip[3:][:-1]
        self.year = int(s[0:4])
        month = int(s[4:6])
        day = int(s[6:])
        # Say time midnight by default
        # Go back and update with specific time when data extracted
        date_string = '%s 0:00:00' % (str(date(self.year, month, day)))

        # Get html and save to file
        box = requests.get(link)
        self.__check_results(box, msg='Unable to retrieve boxscore:%s' % link)
        try:
            with open(save_path, 'w+') as f:
                f.write(box.text.encode('utf-8'))
        except IOError:
            log.error('Error saving file:%s' % save_path)
            sys.exit(-1)
        try:
            query = "INSERT INTO boxscore(team_one_name, team_two_name, link, html_path, date)"\
                   " VALUES ('%s', NULL, '%s', '%s', '%s')" % (team, link, save_path, date_string)
            cursor.execute(query)
        except sqlite3.IntegrityError as e:
            log.debug("Cannot create record, assume link exists")
            log.error("%s" % str(e))
            query = 'UPDATE boxscore SET team_two_name="%s" WHERE link="%s"' % (team, link)
            cursor.execute(query)

    def __collect_team(self, url, team, cursor, html_dir):
        log.info('Downloading all boxscores for team:%s' % team)
        r = requests.get(url)
        self.__check_results(r, msg='Unable to retrieve team:%s' % url)
        soup = BeautifulSoup(r.text)
        for i in soup.find_all('a'):
            href = i['href']
            # All boxscores have a link that starts with /boxes/
            if href.startswith('/boxes/') and href.endswith('.shtml'):
                link = utils.URL_PREFIX + href
                self.__insert_boxscore(link, team, html_dir, cursor)

    def collect_team_games(self, cursor, html_dir):
        query = 'SELECT year,team_name,schedule_link from boxscore_meta where year=%d' % self.year
        cursor.execute(query)
        html_save = os.path.abspath(html_dir)
        # Make dir if needed
        if not os.path.isdir(html_dir):
            os.mkdir(html_dir, 0755)
        result = cursor.fetchall()
        log.info('Gathering all boxscores for self.year:%d' % self.year)
        for item in result:
            # Tuple returned (self.year, team, link)
            url = item[2]
            team = item[1]
            self.__collect_team(url, team, cursor, html_save)
