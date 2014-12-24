import argparse
import logging

from client import download
from client import utils

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
    return p.parse_args()

def main():
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
    args = vars(parse_args())
    with utils.connect_sql(args['database']) as sql_connection:
        cursor = sql_connection.cursor()
        utils.create_tables(cursor)
        client = download.DownloadClient(args['year'])
        client.collect_teams_and_schedules(cursor)
        client.collect_team_games(cursor, args['save_dir'])

if __name__ == '__main__':
    main()
