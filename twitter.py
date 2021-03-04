# Copyright 2021 Jaewan Yun <jaeyun@ucdavis.edu>
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
# The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.


import datetime
import json
import re
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


BEARER_TOKEN = 'Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA'
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:78.0) Gecko/20100101 Firefox/78.0'


class Token:
    def __init__(self, config, tor):
        config['bearer_token'] = BEARER_TOKEN
        config['guest_token'] = None
        self.config = config
        self.tor = tor
        self.url = 'https://twitter.com'
        self._retries = 100
        self._timeout = 100
        self.config['session'].headers.update({'User-Agent': USER_AGENT})

    def renew(self):
        self.tor.renew_connection()
        self.config['session'] = self.tor.get_session()
        self.config['session'].headers.update({'User-Agent': USER_AGENT})
        time.sleep(10)

    def _request(self):
        for attempt in range(self._retries + 1):
            try:
                return self.config['session'].get(self.url, allow_redirects=True, timeout=self._timeout)
            except Exception as e:
                error_log.log(self.config['worker_label'], 'Could not get guest token', e)
            if attempt < self._retries:
                sleep_time = 2.0 * 2 ** attempt
                time.sleep(sleep_time)
        self.config['guest_token'] = None
        raise Exception('Failed after {} retries'.format(self._retries + 1))

    def refresh(self, config):
        self.config = config
        self.config['session'].headers.update({'User-Agent': USER_AGENT})
        res = self._request()
        match = re.search(r'\("gt=(\d+);', res.text)
        while not match:
            self.renew()
            res = self._request()
            match = re.search(r'\("gt=(\d+);', res.text)
        self.config['guest_token'] = str(match.group(1))


class Twitter:
    def __init__(self, tor=None, config_file='config.json'):
        self.tor = tor
        if self.tor is None:
            self.tor = Tor()

        # Read configuration from file
        self.config_file = config_file
        try:
            with open(self.config_file) as f:
                config = json.load(f)
            self.start_date = config['twitter_start_date']
            self.n_threads = config['twitter_n_threads']
        except Exception as e:
            raise Exception('Failed to read {}'.format(self.config_file))

        self.base_url = 'https://api.twitter.com/2/search/adaptive.json'
        self.tz_offset = datetime.timedelta(hours=8)

    def _request(self, config):
        params = {
            'f': 'tweets',
            'include_can_media_tag': 'true',
            'include_ext_alt_text': 'true',
            'include_quote_count': 'true',
            'include_reply_count': 'true',
            'tweet_mode': 'extended',
            'include_entities': 'true',
            'include_user_entities': 'true',
            'include_ext_media_availability': 'true',
            'send_error_codes': 'true',
            'simple_quoted_tweet': 'true',
            'count': 100,
            'cursor': str(config['cursor']),
            'spelling_corrections': 'true',
            'ext': 'mediaStats%2ChighlightedLabel',
            'tweet_search_mode': 'live',
        }
        if 'lang' in config.keys() and config['lang']:
            params['l'] = config['lang']
            params['lang'] = 'en'

        # Tweet response data uses UTC but API request uses PST
        since = config['since'] - self.tz_offset
        until = config['until'] - self.tz_offset

        q = ''
        q += f" since:{int(since.timestamp())}"
        q += f" until:{int(until.timestamp())}"

        if 'query' in config.keys() and config['query']:
            q += f" from:{config['query']}"
        if 'username' in config.keys() and config['username']:
            q += f" from:{config['username']}"
        if 'geo' in config.keys() and config['geo']:
            config['geo '] = config['geo'].replace(' ', '')
            q += f" geocode:{config['geo']}"
        if 'search' in config.keys() and config['search']:
            q += f" {config['search']}"
        if 'is_verified' in config.keys() and config['is_verified']:
            q += " filter:verified"
        if 'to' in config.keys() and config['to']:
            q += f" to:{config['to']}"
        if 'all' in config.keys() and config['all']:
            q += f" to:{config['all']} OR from:{config['all']} OR @{config['all']}"
        if 'near' in config.keys() and config['near']:
            q += f" near:\"{config['near']}\""
        if 'has_images' in config.keys() and config['has_images']:
            q += " filter:images"
        if 'has_videos' in config.keys() and config['has_videos']:
            q += " filter:videos"
        if 'has_media' in config.keys() and config['has_media']:
            q += " filter:media"
        if 'has_replies' in config.keys() and config['has_replies']:
            q += " filter:replies"
        if 'has_native_retweets' in config.keys() and config['has_native_retweets']:
            q += " filter:nativeretweets"
        if 'min_likes' in config.keys() and config['min_likes']:
            q += f" min_faves:{config['min_likes']}"
        if 'min_retweets' in config.keys() and config['min_retweets']:
            q += f" min_retweets:{config['min_retweets']}"
        if 'min_replies' in config.keys() and config['min_replies']:
            q += f" min_replies:{config['min_replies']}"
        if 'include_links' in config.keys() and config['include_links']:
            q += " filter:links"
        if 'exclude_links' in config.keys() and config['exclude_links']:
            q += " exclude:links"
        if 'source' in config.keys() and config['source']:
            q += f" source:\"{config['source']}\""
        if 'members_list' in config.keys() and config['members_list']:
            q += f" list:{config['members_list']}"
        if 'exclude_retweets' in config.keys() and config['exclude_retweets']:
            q += f" exclude:nativeretweets exclude:retweets"
        params['q'] = q.strip()

        headers = {
            'authorization': config['bearer_token'],
            'x-guest-token': config['guest_token'],
        }
        while True:
            try:
                res = config['session'].get(self.base_url, params=params, headers=headers)
                break
            except Exception as e:
                error_log.log(config['worker_label'], 'Connection error', e)
                self.tor.renew_connection()
                config['session'] = self.tor.get_session()
                time.sleep(10)
        return res

    def parse_response(self, response, config):
        data = response.json()

        if len(data['globalObjects']['tweets']) == 0:
            config['cursor'] = -1
            return []

        feed = []
        for timeline_entry in data['timeline']['instructions'][0]['addEntries']['entries']:
            # Handle cases where timeline entry is a tweet
            if timeline_entry['entryId'].startswith('sq-I-t-') or timeline_entry['entryId'].startswith('tweet-'):
                if 'tweet' in timeline_entry['content']['item']['content']:
                    tid = timeline_entry['content']['item']['content']['tweet']['id']
                    if 'promotedMetadata' in timeline_entry['content']['item']['content']['tweet']:
                        continue
                elif 'tombstone' in timeline_entry['content']['item']['content'] and 'tweet' in timeline_entry['content']['item']['content']['tombstone']:
                    tid = timeline_entry['content']['item']['content']['tombstone']['tweet']['id']
                else:
                    tid = None
                if tid is None:
                    raise ValueError('Unable to find ID of tweet in timeline.')
                try:
                    temp_obj = data['globalObjects']['tweets'][tid]
                except KeyError:
                    continue
                temp_obj['user_data'] = data['globalObjects']['users'][temp_obj['user_id_str']]
                if 'retweeted_status_id_str' in temp_obj:
                    rtid = temp_obj['retweeted_status_id_str']
                    date = datetime.datetime.strptime(data['globalObjects']['tweets'][rtid]['created_at'], '%a %b %d %H:%M:%S %z %Y')
                    date = str(date.strftime('%Y-%m-%d %H:%M:%S %Z'))
                    temp_obj['retweet_data'] = {
                        'user_rtid': data['globalObjects']['tweets'][rtid]['user_id_str'],
                        'user_rt': data['globalObjects']['tweets'][rtid]['full_text'],
                        'retweet_id': rtid,
                        'retweet_date': date,
                    }
                feed.append(temp_obj)

        parsed = []
        for t in feed:
            # Tweet data
            t['datetime'] = datetime.datetime.strptime(t['created_at'], '%a %b %d %H:%M:%S %z %Y')
            t['day_of_week'] = t['datetime'].strftime('%A')
            t['date'] = t['datetime'].strftime('%Y-%m-%d')
            t['time'] = t['datetime'].strftime('%H:%M:%S')
            t['update_datetime'] = datetime.datetime.now() + datetime.timedelta(hours=8)
            t['full_text'] = bytes(t['full_text'], 'utf-8').decode('utf-8', 'ignore')
            t['full_text'] = ' '.join(t['full_text'].split())

            # t['display_text_range'] = json.dumps(t['display_text_range'])
            # if 'coordinates' in t:
            #     t['coordinates'] = json.dumps(t['coordinates'])
            # if 'place' in t:
            #     t['place'] = json.dumps(t['place'])
            # if 'self_thread' in t:
            #     t['self_thread'] = json.dumps(t['self_thread'])

            # Entity data
            t['hashtags'] = json.dumps(t['entities']['hashtags'])
            t['symbols'] = json.dumps(t['entities']['symbols'])
            t['user_mentions'] = json.dumps(t['entities']['user_mentions'])
            t['urls'] = json.dumps(t['entities']['urls'])

            # User data
            # t['user_data_json'] = json.dumps(t['user_data'])
            # t['user_data']['entities'] = json.dumps(t['user_data']['entities'])
            date = datetime.datetime.strptime(t['user_data']['created_at'], '%a %b %d %H:%M:%S %z %Y')
            t['user_data']['datetime'] = date.strftime('%Y-%m-%d %H:%M:%S')

            parsed.append(t)

        try:
            config['cursor'] = data['timeline']['instructions'][0]['addEntries']['entries'][-1]['content']['operation']['cursor']['value']
        except KeyError:
            config['cursor'] = data['timeline']['instructions'][-1]['replaceEntry']['entry']['content']['operation']['cursor']['value']
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
                config['token'].refresh(config)
                time.sleep(10)
                continue

            data = self.parse_response(response, config)
            if len(data) == 0:
                break
            chunk.extend(data)

        chunk = sorted(chunk, key=lambda t: t['id'], reverse=False)
        return chunk

    def get_data(self, config):
        config['token'] = Token(config, self.tor)
        config['token'].refresh(config)

        start = config['since']
        end = config['until']
        current_date = start

        leap = datetime.timedelta(days=3)
        config['since'] = current_date
        config['until'] = current_date + leap

        # Create table if it does not exist
        config['database'].create_table(config['symbol'], type='twitter')

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
            L.log(config['worker_label'], '{:<8} {} - {} ({})'.format(config['symbol'], date1, date2, len(chunk)))
            # L.log(json.dumps(chunk, indent=4, sort_keys=True))
            # return

            # Add to database
            config['database'].add_data(config['symbol'], chunk, type='twitter')

            # Increment time
            current_date += leap
            config['since'] = current_date
            config['until'] = current_date + leap
            if config['since'] >= end:
                break


def _download_query(twitter, symbols, symbol, recency, session, worker_id):
    worker_label = ' (T{}):\t'.format(worker_id)

    # Build query
    name = symbols.company_name(symbol)
    query_list = []
    if name.lower() != symbol.lower():
        query_list.append(name)
    query_list.append('${}'.format(symbol))
    query = ' OR '.join(query_list)
    # query = '({}) lang:en'.format(' OR '.join(query_list))
    query_log = Logger()
    query_log.set_log_type('OKBLUE')
    query_log.log(worker_label, 'Query [{}]'.format(query))

    # Resume from last datetime
    database = Database(id=worker_id)
    since = datetime.datetime.strptime(twitter.start_date, '%Y-%m-%d %H:%M:%S')
    newest = database.newest(symbol, type='twitter')
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
        'cursor': -1,
        'search': query,
        'since': since,
        'until': datetime.datetime.now() + datetime.timedelta(hours=8),
        'exclude_retweets': True,
        # 'min_retweets': 1,
    }
    L.log(worker_label, '{} resuming from {}'.format(symbol, config['since']))
    twitter.get_data(config)
    database.close()


def _work(jobs, session, worker_id):
    while not jobs.empty():
        kwargs = jobs.get()
        _download_query(**kwargs, session=session, worker_id=worker_id)
        jobs.task_done()


def download(recency=None):
    tor = Tor()
    twitter = Twitter(tor)

    # Worker queue
    jobs = Queue()
    symbols = Symbols()
    for symbol_info in symbols.symbols_list:
        symbol = symbol_info['symbol']
        jobs.put({
            'twitter': twitter,
            'symbols': symbols,
            'symbol': symbol,
            'recency': recency,
        })

    # Download
    L.log('Twitter download begin')
    for worker_id in range(twitter.n_threads):
        time.sleep(1)
        tor.renew_connection()
        time.sleep(2)
        session = tor.get_session()
        time.sleep(1)
        worker = threading.Thread(target=_work, args=[jobs, session, worker_id])
        worker.daemon = True
        worker.start()
    jobs.join()
    L.log('Twitter download complete')


if __name__ == '__main__':
    # recency = datetime.timedelta(days=2)
    download(recency=None)

    # config['since'] = datetime.datetime.now() + datetime.timedelta(hours=8) - datetime.timedelta(hours=10)
    # config['until'] = datetime.datetime.now() + datetime.timedelta(hours=8)
    # L.log(config['since'])
    # L.log(config['until'])
    # db.del_twitter_data(symbol, 3)
