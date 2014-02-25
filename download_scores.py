import argparse
from HTMLParser import HTMLParser
import requests
import os
import sys

MAIN_PREFIX = 'http://www.baseball-reference.com'
BOXES = MAIN_PREFIX + '/boxes/'

class list_teams(HTMLParser):
    def __init__(self):
        HTMLParser.__init__(self)
        self.track_data = False
        self.teams = dict()
        self.last_team = None

    def find_teams(self, data):
        self.feed(data)
        if '\n' in self.teams.keys():
            del self.teams['\n']
        return self.teams

    def handle_starttag(self, tag, attrs):
        if tag == 'div':
            if len(attrs) > 0:
                if attrs[0][0] == 'id':
                    if attrs[0][1] == 'page_content':
                        self.track_data = True
        elif self.track_data:
            if tag == 'a':
                self.last_team = attrs[0][1]

    def handle_endtag(self, tag):
        if self.track_data:
            if tag == 'table':
                self.track_data = False

    def handle_data(self, data):
        if self.track_data and self.last_team is not None:
            self.teams[data] = self.last_team

class list_games(HTMLParser):
    def __init__(self):
        HTMLParser.__init__(self)
        self.check_data = False
        self.track_data = False
        self.games = []
    def list_games(self, data):
        self.feed(data)
        return self.games
    def handle_starttag(self, tag, attrs):
        if self.track_data:
            if tag == 'a':
                if attrs[0][1].startswith('/boxes/'):
                    self.games.append(attrs[0][1])
        if tag == 'h2':
            self.check_data = True
        else:
            self.check_data = False
    def handle_endtag(self, tag):
        if tag == 'table':
            self.track_data = False
    def handle_data(self, data):
        if self.check_data:
            if data == 'Team Game-by-Game Schedule and Results':
                self.track_data = True

def get_abbreviation(link):
    things = link.split('/')
    return things[2]

def get_team_info(year):
    url = BOXES + year + '.shtml'
    r = requests.get(url)
    check_results(r, 'Unable to retrieve data for the year requested.')
    parser = list_teams()
    teams = parser.find_teams(r.text)
    team_data = dict()
    for team, link in teams.iteritems():
        abbr = get_abbreviation(link)
        good_link = MAIN_PREFIX + link
        team_data[abbr] = {'name' : team,
                           'link' : good_link}
    return team_data

def get_team_games(team_link):
    r = requests.get(team_link)
    parser = list_games()
    games = parser.list_games(r.text)
    return games

def parse_args():
    p = argparse.ArgumentParser(description='Download boxscores')
    p.add_argument('year', help='Year to download')
    p.add_argument('save_dir', help='Directory to Save Results', nargs='?', default=None)
    return p.parse_args()

def check_results(request, msg=None):
    ''' Handle the fact that baseball-reference.com returns a "200" status code
        but an error message in the page content, rather than a legitimate HTTP
        error response. '''
    if not msg:
        msg = 'Unknown error retrieving page.'
    if '404 - File Not Found' in request.text:
        print '#ERROR: %s' % msg
        sys.exit(-1)

def main():
    args = vars(parse_args())
    output_dir = os.path.abspath('.')
    if args['save_dir']:
        if not os.path.isdir(args['save_dir']):
            os.mkdir(args['save_dir'])
        output_dir = args['save_dir']
    print 'Getting Team List'
    teams = get_team_info(args['year'])
    all_games = set([])
    for abbr in teams:
        print 'Collecting games for team:', teams[abbr]['name']
        games = get_team_games(teams[abbr]['link'])
        all_games = all_games.union(set(games))
    count = 0
    percent = 0.0
    total = len(all_games) * 1.0
    print 'Downloading Boxscores'
    for game in all_games:
        if ( count/total ) >= percent:
            print str(int((percent* 100 ))) + '% Done'
            percent += 0.1
        r = requests.get(MAIN_PREFIX + game)
        check_results(r, 'Unable to retrieve data for the game requested.')
        file_name = game.split('/')[3]
        with open(os.path.join(output_dir, file_name), 'w+') as f:
            f.write(r.text)
        count += 1

if __name__ == '__main__':
    main()
