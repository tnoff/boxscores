import argparse
from bs4 import BeautifulSoup
import json
import sys

META_ORDER = ['date', 'stadium', 'attendance']
GENERAL_ORDER = ['team', 'runs', 'record', 'links', 'standings', 'manager']
GENERAL_EXTRA = ['winning_pitcher', 'losing_pitcher', 'closing_pitcher']
EXTRA_ORDER = ['umpires', 'gametime', 'attendance', 'field_condition', 'weather', None]

def set_output(file_name):
    f = open(file_name, 'w+')
    sys.stdout = f

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

def __page_meta(page_data):
    meta = dict()
    other_games = page_data.find('table', {'class' : 'stats_table'})
    current_meta = next_sibling(other_games)
    current_meta = next_element(current_meta, repeat=2)
    meta_count = 0
    for item in current_meta:
        if item == '\n':
            continue
        if meta_count == 3:
            break
        meta[META_ORDER[meta_count]] = item.next_element
        meta_count += 1
    return meta

def __general_data(data):
    gen = dict()
    gen_count = 0
    for item in data:
        if item == '\n':
            continue
        gen[GENERAL_ORDER[gen_count]] = next_element(item)
        gen_count += 1
    gen.pop('standings')
    gen.pop('links')
    gen.pop('record')
    gen['runs'] = int(gen.pop('runs')[0])
    manager = gen.pop('manager')
    name = next_element(manager)
    gen['manager'] = { 'name' : name,
                       'link' : manager.attrs['href']
                     }
    return gen

def get_number_innings(text):
    s = text.split(' ')
    max_num = 0
    for i in s:
        try:
            if int(i) > max_num:
                max_num = int(i)
        except ValueError:
            continue
    return max_num

def __nice_list(text):
    split = text.split(' ')
    nice = []
    for i in split:
        if str(i) == '':
            continue
        try:
            nice.append(int(i))
        except ValueError:
            nice.append(None)
    return nice

def __page_general(page_data):
    general = dict()
    meta_data = page_data.find('table', {'class' : 'stats_table'})
    general_data = next_sibling(meta_data)
    gd = general_data.find('td', {'align' : 'center'})
    tds = gd.find_all('td', {'align' : 'center'})
    away = tds[0]
    home = tds[2]
    general['away'] = __general_data(away)
    general['home'] = __general_data(home)
    pitcher_data = gd.find('td', {'align' : 'left'})
    pitcher_data = next_element(pitcher_data)
    gen_count = 0
    general['pitchers'] = dict()
    general['pitchers']['closing_pitcher'] = None
    for p in pitcher_data:
        if p == '\n':
            continue
        a = p.next_element.next_element.next_element.next_element
        general['pitchers'][GENERAL_EXTRA[gen_count]] = \
                    {
                        'name' : a.contents[0],
                        'link' : a.attrs['href'],
                    }
        gen_count += 1
    scores = gd.find_all('td', {'align' : 'left' })[1].find('pre')
    general['innings_played'] = get_number_innings(scores.next_element)
    scores = next_element(scores, repeat=6)
    general['away']['link'] = scores.attrs['href']
    general['away']['runs_by_inning'] = __nice_list(next_element(scores, repeat=2))
    scores = next_element(scores, repeat=4)
    totals = __nice_list(scores)
    general['away']['hits'] = totals[1]
    general['away']['errors'] = totals[2]
    scores = next_element(scores, repeat=2)
    general['home']['link'] = scores.attrs['href']
    general['home']['runs_by_inning'] = __nice_list(next_element(scores, repeat=2))
    scores = next_element(scores, repeat=4)
    totals = __nice_list(scores)
    general['home']['hits'] = totals[1]
    general['home']['errors'] = totals[2]
    return general

def __get_columns(table):
    columns = []
    for header in table.find_all('th'):
        if header.attrs['data-stat'] == 'blank':
            columns.append(None)
            continue
        columns.append(header.attrs['data-stat'])
    return columns

def __generate_stat_table(table):#pylint: disable=R0912
    columns = __get_columns(table)
    players = []
    for row in table.find_all('tr')[1:]:
        data = dict()
        count = -1
        for col in row:
            if col == '\n':
                continue
            count += 1
            if col.attrs['align'] == 'left':
                if col.next_element == 'Team Totals':
                    data[columns[count]] = 'Team Totals'
                    continue
                temp = next_element(col)
                if not hasattr(temp, 'attrs'):
                    temp = next_element(temp)
                data[columns[count]] = dict()
                if 'href' not in temp.attrs.keys():
                    continue
                data[columns[count]]['link'] = temp.attrs['href']
                data[columns[count]]['name'] = next_element(temp)
                continue
            if col.attrs['align'] == '':
                if col.contents == []:
                    data[columns[count]] = None
                    continue
                if len(col.contents) > 1:
                    temp = col.contents[1]
                else:
                    temp = col.contents[0]
                if hasattr(temp, 'attrs'):
                    data[columns[count]] = dict()
                    data[columns[count]]['link'] = temp.attrs['href']
                    data[columns[count]]['name'] = next_element(temp)
                    continue
                data[columns[count]] = temp
                continue
            temp = col.next_element
            if hasattr(temp, 'attrs'):
                temp = temp.contents
                if temp == []:
                    data[columns[count]] = None
                    continue
                temp = temp[0]
            if temp == '\n':
                data[columns[count]] = None
                continue
            data[columns[count]] = temp
        players.append(data)
    return players

def __page_stats(page_data):
    stats = dict()
    stat_tables = page_data.find_all('table', {'class' : 'stats_table'})
    stat_tables.pop(0)
    stats['batting_away'] = __generate_stat_table(stat_tables.pop(0))
    stats['batting_home'] = __generate_stat_table(stat_tables.pop(0))
    stats['pitching_away'] = __generate_stat_table(stat_tables.pop(0))
    stats['pitching_home'] = __generate_stat_table(stat_tables.pop(0))
    stats['starting_lineups'] = __generate_stat_table(stat_tables.pop(0))
    delete = None
    for (count, row) in enumerate(stats['starting_lineups']):
        row.pop(None, None)
        if row['player_home'] == None:
            delete = count
    if delete != None:
        del stats['starting_lineups'][delete]
    return stats

def __page_extras(page_data):
    small_txt = page_data.find_all('div', {'class' : 'small_text'})
    extras = dict()
    extra_count = 0
    for extra_data in small_txt:
        if len(extra_data.attrs['class']) > 1:
            continue
        for data in extra_data:
            if data == '\n':
                continue
            try:
                if 'id' not in data.attrs:
                    continue
            except AttributeError:
                extras[EXTRA_ORDER[extra_count]] = data
                extra_count += 1
                continue
            extras[data.attrs['id']] = next_element(data, repeat=3)
    if None in extras.keys():
        keys = EXTRA_ORDER
        for count in range(len(keys)):
            if keys[count] == None:
                break
            extras[keys[count]] = extras[keys[count+1]]
    extras.pop(None, None)
    extras.pop('gametime')
    extras.pop('attendance')
    return extras

def read_file(file_name):
    with open(file_name, 'r') as f:
        data = f.read().decode('utf-8')
    usable_data = dict()
    soup = BeautifulSoup(data)
    page_data = soup.find(id='page_content')
    usable_data['meta'] = __page_meta(page_data)
    usable_data['general'] = __page_general(page_data)
    usable_data['stats'] = __page_stats(page_data)
    usable_data['general']['starting_lineups'] = \
        usable_data['stats'].pop('starting_lineups')
    usable_data['extras'] = __page_extras(page_data)
    usable_data['meta']['weather'] = usable_data['extras'].pop('weather')
    for i in EXTRA_ORDER:
        if i in usable_data['extras'].keys():
            usable_data['meta'][i] = \
                usable_data['extras'].pop(i)
    return usable_data


def parse_args():
    a = argparse.ArgumentParser(description='Read HTML into JSON')
    a.add_argument('file_name', help='File to read')
    a.add_argument('-o', dest='output', help='Output File')
    return a.parse_args()

def main():
    args = vars(parse_args())
    if args['output'] != None:
        set_output(args['output'])
    text = read_file(args['file_name'])
    print json.dumps(text, indent=4)

if __name__ == '__main__':
    main()
