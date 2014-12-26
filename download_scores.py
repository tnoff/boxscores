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
    p.add_argument('--log', default='log', help='Logging file')
    return p.parse_args()

def main():
    args = vars(parse_args())
    logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s",
                        level=logging.DEBUG,
                        filename=args['log'])
    with utils.connect_sql(args['database']) as sql_connection:
        cursor = sql_connection.cursor()
        utils.create_tables(cursor)
        client = download.DownloadClient(args['year'])
        client.collect_teams_and_schedules(cursor)
        client.collect_team_games(cursor, args['save_dir'])

if __name__ == '__main__':
    main()
