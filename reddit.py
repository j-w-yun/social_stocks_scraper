# Copyright 2021 Jaewan Yun <jaeyun@ucdavis.edu>
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
# The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.


import datetime
import json
import time
import threading
from queue import Queue

from database import Database
from symbols import Symbols
from tor import Tor
from logger import Logger


L = Logger()
L.set_log_type('OKCYAN')
error_log = Logger()
error_log.set_log_type('HEADER')
bold_log = Logger()
bold_log.set_log_type('BOLD')


class Reddit:
    def __init__(self, tor=None, config_file='config.json'):
        self.tor = tor
        if self.tor is None:
            self.tor = Tor()

        # Read configuration from file
        self.config_file = config_file
        try:
            with open(self.config_file) as f:
                config = json.load(f)
            self.start_date = config['reddit_start_date']
            self.n_threads = config['reddit_n_threads']
        except Exception as e:
            raise Exception('Failed to read {}'.format(self.config_file))

        self.base_url = 'https://api.pushshift.io/reddit/search/comment'
        self.tz_offset = datetime.timedelta(hours=8)
        self.max_body_len= 2000

    def _request(self, config):
        since = config['since'] + self.tz_offset
        until = config['until'] + self.tz_offset
        params = {
            'score': '>0',
            'size': 100,
            'sort': 'asc',
            'sort_type': 'created_utc',
            'after': since.strftime('%Y-%m-%d %H:%M:%S'),
            'before': until.strftime('%Y-%m-%d %H:%M:%S'),
            'q': config['search'],
        }
        while True:
            try:
                res = config['session'].get(self.base_url, params=params)
                break
            except Exception as e:
                error_log.log(config['worker_label'], 'Connection error', e)
                self.tor.renew_connection()
                config['session'] = self.tor.get_session()
                time.sleep(10)
        return res

    def parse_response(self, response, config):
        data = response.json()['data']
        if len(data) == 0:
            return []

        parsed = []
        for t in data:
            # Trim body
            t['body'] = bytes(t['body'], 'utf-8').decode('utf-8', 'ignore')
            t['body'] = ' '.join(t['body'].split())
            if len(t['body']) > self.max_body_len:
                sentences = t['body'].split('. ')
                new_body = ''
                for s in sentences:
                    if len(new_body) > self.max_body_len:
                        break
                    new_body += s.strip() + '. '
                t['body'] = new_body + '[...]'

            # Remove if symbol matched in lower case
            skip = True
            lower_body = t['body'].lower()
            for m in config['matches']:
                if m.lower() in lower_body:
                    skip = False
                    break
            if skip and config['symbol'] not in t['body']:
                continue

            t['datetime'] = datetime.datetime.fromtimestamp(int(t['created_utc']))
            t['day_of_week'] = t['datetime'].strftime('%A')
            t['date'] = t['datetime'].strftime('%Y-%m-%d')
            t['time'] = t['datetime'].strftime('%H:%M:%S')
            t['update_datetime'] = datetime.datetime.now() + datetime.timedelta(hours=8)

            if 'author_created_utc' in t and t['author_created_utc'] is not None:
                t['author_datetime'] = datetime.datetime.fromtimestamp(int(t['author_created_utc']))
            else:
                t['author_datetime'] = datetime.datetime.fromtimestamp(0)

            parsed.append(t)

        n_del = len(data) - len(parsed)
        if n_del != 0:
            bold_log.log(config['worker_label'], '{} removed {} false positive matches'.format(config['symbol'], n_del))

        config['since'] = datetime.datetime.fromtimestamp(int(data[-1]['created_utc']))
        return parsed

    def get_data_chunk(self, config):
        chunk = []
        while True:
            time.sleep(0.2)
            response = self._request(config)

            if response.status_code != self.tor.ok:
                error_log.log(config['worker_label'], '{} Response not OK {}'.format(config['symbol'], response))
                self.tor.renew_connection()
                config['session'] = self.tor.get_session()
                time.sleep(10)
                continue

            data = self.parse_response(response, config)
            if len(data) == 0:
                break
            chunk.extend(data)

        chunk = sorted(chunk, key=lambda t: t['created_utc'], reverse=False)
        return chunk

    def get_data(self, config):
        start = config['since']
        end = config['until']
        current_date = start

        leap = datetime.timedelta(days=3)
        config['since'] = current_date
        config['until'] = current_date + leap

        # Create table if it does not exist
        config['database'].create_table(config['symbol'], type='reddit')

        while True:
            time.sleep(0.2)
            if config['until'] > end:
                config['until'] = end

            chunk = self.get_data_chunk(config)
            if len(chunk) == 0:
                date1 = config['since'].strftime('%Y-%m-%d %H:%M:%S')
                date2 = config['until'].strftime('%Y-%m-%d %H:%M:%S')
            else:
                date1 = chunk[0]['datetime'].strftime('%Y-%m-%d %H:%M:%S')
                date2 = chunk[-1]['datetime'].strftime('%Y-%m-%d %H:%M:%S')
            L.log(config['worker_label'], '{:<8} {} - {} \t ({})'.format(config['symbol'], date1, date2, len(chunk)))
            # L.log(json.dumps(chunk, indent=4, sort_keys=True))
            # return

            # Add to database
            config['database'].add_data(config['symbol'], chunk, type='reddit')

            # Increment time
            current_date += leap
            config['since'] = current_date
            config['until'] = current_date + leap
            if config['since'] >= end:
                break


def _download_query(reddit, symbols, symbol, recency, session, worker_id):
    worker_label = ' (R{}):\t'.format(worker_id)

    # Build query
    name = symbols.company_name(symbol)
    info = symbols.get_info(symbol)
    names = [
        info['shortName'],
        info['longName'],
    ]
    query_list = [*names]
    if name.lower() != symbol.lower():
        q = name
        if symbols.in_dictionary(name):
            q = '({}+(stocks|shares))'.format(q)
        query_list.append(q)
    if len(symbol) >= 3:
        # q = symbol
        # if symbols.in_dictionary(symbol):
        #     q = '({}+(stocks|shares))'.format(symbol)
        # query_list.append(q)
        query_list.append('({}+(stocks|shares))'.format(symbol))
    query = '|'.join(query_list)
    query_log = Logger()
    query_log.set_log_type('OKBLUE')
    query_log.log(worker_label, 'Query {}'.format(query))
    if len(query_list) == 0:
        query_log.log(worker_label, 'Empty query {}'.format(query))
        return

    # Resume from last datetime
    database = Database(id=worker_id)
    since = datetime.datetime.strptime(reddit.start_date, '%Y-%m-%d %H:%M:%S')
    newest = database.newest(symbol, type='reddit')
    if newest is not None:
        since = newest['datetime']

    # Check if it should update based on recency condition
    if recency is not None and newest is not None:
        date_last = newest['datetime']
        date_now = datetime.datetime.now() + datetime.timedelta(hours=8)
        date_check = date_now - recency
        if date_last > date_check:
            last = date_last.strftime('%Y-%m-%d %H:%M:%S')
            check = date_check.strftime('%Y-%m-%d %H:%M:%S')
            skip_log = Logger()
            skip_log.set_log_type('FAIL')
            skip_log.log(worker_label, '{} skipped due to recency condition {} > {}'.format(symbol, last, check))
            return

    config = {
        'worker_label': worker_label,
        'database': database,
        'session': session,
        'symbol': symbol,
        'search': query,
        'since': since,
        'until': datetime.datetime.now() + datetime.timedelta(hours=8),
        'matches': [*names, name, 'stocks', 'shares']
    }
    L.log(worker_label, '{} resuming from {}'.format(symbol, config['since']))
    reddit.get_data(config)
    database.close()


def _work(jobs, session, worker_id):
    while not jobs.empty():
        kwargs = jobs.get()
        _download_query(**kwargs, session=session, worker_id=worker_id)
        jobs.task_done()


def download(recency=None):
    tor = Tor()
    reddit = Reddit(tor)

    # Worker queue
    jobs = Queue()
    symbols = Symbols()
    for symbol_info in symbols.symbols_list:
        symbol = symbol_info['symbol']
        jobs.put({
            'reddit': reddit,
            'symbols': symbols,
            'symbol': symbol,
            'recency': recency,
        })

    # Download
    L.log('Reddit download begin')
    for worker_id in range(reddit.n_threads):
        time.sleep(1)
        tor.renew_connection()
        time.sleep(2)
        session = tor.get_session()
        time.sleep(1)
        worker = threading.Thread(target=_work, args=[jobs, session, worker_id])
        worker.daemon = True
        worker.start()
    jobs.join()
    L.log('Reddit download complete')


if __name__ == '__main__':
    # recency = datetime.timedelta(days=2)
    download(recency=None)

    # config['since'] = datetime.datetime.now() + datetime.timedelta(hours=8) - datetime.timedelta(hours=10)
    # config['until'] = datetime.datetime.now() + datetime.timedelta(hours=8)
    # L.log(config['since'])
    # L.log(config['until'])
    # db.del_reddit_data(symbol, 3)
