import argparse
from bs4 import BeautifulSoup
import requests
import os
import sys

MAIN_PREFIX = 'http://www.baseball-reference.com'
BOXES = MAIN_PREFIX + '/boxes/'

def collect_teams(year):
    url = BOXES + year + '.shtml'
    r = requests.get(url)
    check_results(r, msg='Unable to retrieve year:%s' % year)
    soup = BeautifulSoup(r.text)
    div = soup.find(id='page_content')
    div_contents = div.contents[3].contents[0]
    contents = div_contents.contents[0].contents[1]
    teams = contents.find_all('a')
    team_data = dict()
    for team in teams:
        team_name = team.contents[0]
        team_url = team.attrs['href']
        team_data[team_name] = team_url
    return team_data

def collect_games(link):
    url = MAIN_PREFIX + link
    r = requests.get(url)
    check_results(r, msg='Unable to retrieve team:%s' % link)
    soup = BeautifulSoup(r.text)
    references = soup.find_all('a')
    games = set([])
    for ref in references:
        href = ref.attrs['href']
        if href.startswith('/boxes/') and href.endswith('.shtml'):
            games.add(href)
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

    teams = collect_teams(args['year'])
    all_games = set([])
    for team, link in teams.iteritems():
        print 'Collecting games for team:', team
        all_games = all_games.union(collect_games(link))
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
            f.write(r.text.encode('utf-8'))
        count += 1

if __name__ == '__main__':
    main()
