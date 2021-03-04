# Copyright 2021 Jaewan Yun <jaeyun@ucdavis.edu>
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
# The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.


import datetime
import json
import time
import mysql.connector

from logger import Logger


L = Logger()
L.set_log_type('WARNING')


class Database:
    def __init__(self, id='N/A', config_file='config.json', verbose=True):
        self.id = id
        self.verbose = verbose
        self.config_file = config_file
        self.config = {
            'autocommit': True,
            'buffered': True,
            'consume_results': True,
            'connection_timeout': 3000000,
        }
        self.db_label = ' (D{}):\t'.format(self.id)
        self.reconnect_tries = 100
        self.reconnect_delay = 10

        # Read configuration from file
        try:
            with open(self.config_file) as f:
                config = json.load(f)
            self.config['host'] = config['host']
            self.config['port'] = config['port']
            self.config['user'] = config['user']
            self.config['password'] = config['password']
            self.config['database'] = config['database']
        except Exception as e:
            if self.verbose:
                L.log(self.db_label, 'Failed to read {}'.format(self.config_file), e)
            raise Exception('Failed to read {}'.format(self.config_file))

        # Establish connection
        self.conn = mysql.connector.connect(**self.config)
        if self.verbose:
            L.log(self.db_label, 'Connected to database')

        self.twitter_format = [
            # Tweet data
            {'key': ['id', ], 'name': 'id', 'type': 'VARCHAR(32) NOT NULL'},
            # {'key': ['contributors', ], 'name': 'contributors', 'type': 'TEXT DEFAULT ""'},
            {'key': ['conversation_id', ], 'name': 'conversation_id', 'type': 'TEXT NOT NULL'},
            # {'key': ['coordinates', ], 'name': 'coordinates', 'type': 'TEXT DEFAULT ""'},
            {'key': ['datetime', ], 'name': 'datetime', 'type': 'DATETIME NOT NULL'},
            {'key': ['day_of_week', ], 'name': 'day_of_week', 'type': 'TEXT NOT NULL'},
            {'key': ['date', ], 'name': 'date', 'type': 'DATE NOT NULL'},
            {'key': ['time', ], 'name': 'time', 'type': 'TIME NOT NULL'},
            # {'key': ['display_text_range', ], 'name': 'display_text_range', 'type': 'TEXT DEFAULT ""'},
            {'key': ['favorite_count', ], 'name': 'favorite_count', 'type': 'INT NOT NULL'},
            {'key': ['favorited', ], 'name': 'favorited', 'type': 'BOOL NOT NULL'},
            {'key': ['full_text', ], 'name': 'full_text', 'type': 'TEXT DEFAULT ""'},
            {'key': ['in_reply_to_screen_name', ], 'name': 'in_reply_to_screen_name', 'type': 'TEXT DEFAULT ""'},
            {'key': ['in_reply_to_status_id', ], 'name': 'in_reply_to_status_id', 'type': 'TEXT DEFAULT ""'},
            {'key': ['in_reply_to_user_id', ], 'name': 'in_reply_to_user_id', 'type': 'TEXT DEFAULT ""'},
            {'key': ['is_quote_status', ], 'name': 'is_quote_status', 'type': 'BOOL NOT NULL'},
            {'key': ['lang', ], 'name': 'lang', 'type': 'TEXT DEFAULT ""'},
            # {'key': ['place', ], 'name': 'place', 'type': 'TEXT DEFAULT ""'},
            {'key': ['possibly_sensitive', ], 'name': 'possibly_sensitive', 'type': 'BOOL DEFAULT 0'},
            # {'key': ['possibly_sensitive_editable', ], 'name': 'possibly_sensitive_editable', 'type': 'BOOL DEFAULT 0'},
            {'key': ['quote_count', ], 'name': 'quote_count', 'type': 'INT NOT NULL'},
            {'key': ['reply_count', ], 'name': 'reply_count', 'type': 'INT NOT NULL'},
            {'key': ['retweet_count', ], 'name': 'retweet_count', 'type': 'INT NOT NULL'},
            {'key': ['retweeted', ], 'name': 'retweeted', 'type': 'BOOL NOT NULL'},
            # {'key': ['self_thread', ], 'name': 'self_thread', 'type': 'TEXT DEFAULT ""'},
            {'key': ['source', ], 'name': 'source', 'type': 'TEXT DEFAULT ""'},
            # {'key': ['supplemental_language', ], 'name': 'supplemental_language', 'type': 'TEXT DEFAULT ""'},
            # {'key': ['truncated', ], 'name': 'truncated', 'type': 'BOOL NOT NULL'},
            {'key': ['update_datetime', ], 'name': 'update_datetime', 'type': 'DATETIME NOT NULL'},
            # {'key': ['user_data_json', ], 'name': 'user_data', 'type': 'TEXT DEFAULT ""'},
            {'key': ['user_id', ], 'name': 'user_id', 'type': 'TEXT NOT NULL'},

            # Entity
            # {'key': ['entities', ], 'name': 'entities', 'type': 'TEXT DEFAULT ""'},
            {'key': ['hashtags', ], 'name': 'hashtags', 'type': 'TEXT NOT NULL'},
            {'key': ['symbols', ], 'name': 'symbols', 'type': 'TEXT NOT NULL'},
            {'key': ['user_mentions', ], 'name': 'user_mentions', 'type': 'TEXT NOT NULL'},
            {'key': ['urls', ], 'name': 'urls', 'type': 'TEXT NOT NULL'},

            # User data
            # {'key': ['user_data', 'advertiser_account_type', ], 'name': 'user_advertiser_account_type', 'type': 'TEXT NOT NULL'},
            {'key': ['user_data', 'datetime', ], 'name': 'user_datetime', 'type': 'DATETIME NOT NULL'},
            # {'key': ['user_data', 'description', ], 'name': 'user_description', 'type': 'TEXT DEFAULT ""'},
            # {'key': ['user_data', 'business_profile_state', ], 'name': 'user_business_profile_state', 'type': 'TEXT NOT NULL'},
            # {'key': ['user_data', 'entities', ], 'name': 'user_entities', 'type': 'TEXT DEFAULT ""'},
            # {'key': ['user_data', 'fast_followers_count', ], 'name': 'user_fast_followers_count', 'type': 'INT NOT NULL'},
            {'key': ['user_data', 'favourites_count', ], 'name': 'user_favourites_count', 'type': 'INT NOT NULL'},
            {'key': ['user_data', 'followers_count', ], 'name': 'user_followers_count', 'type': 'INT NOT NULL'},
            {'key': ['user_data', 'friends_count', ], 'name': 'user_friends_count', 'type': 'INT NOT NULL'},
            # {'key': ['user_data', 'geo_enabled', ], 'name': 'user_geo_enabled', 'type': 'BOOL NOT NULL'},
            # {'key': ['user_data', 'has_custom_timelines', ], 'name': 'user_has_custom_timelines', 'type': 'BOOL NOT NULL'},
            # {'key': ['user_data', 'has_extended_profile', ], 'name': 'user_has_extended_profile', 'type': 'BOOL NOT NULL'},
            # {'key': ['user_data', 'is_translation_enabled', ], 'name': 'user_is_translation_enabled', 'type': 'BOOL NOT NULL'},
            # {'key': ['user_data', 'is_translator', ], 'name': 'user_is_translator', 'type': 'BOOL NOT NULL'},
            {'key': ['user_data', 'lang', ], 'name': 'user_lang', 'type': 'TEXT DEFAULT ""'},
            {'key': ['user_data', 'listed_count', ], 'name': 'user_listed_count', 'type': 'INT NOT NULL'},
            # {'key': ['user_data', 'location', ], 'name': 'user_location', 'type': 'TEXT NOT NULL'},
            # {'key': ['user_data', 'media_count', ], 'name': 'user_media_count', 'type': 'INT NOT NULL'},
            {'key': ['user_data', 'name', ], 'name': 'user_name', 'type': 'TEXT NOT NULL'},
            {'key': ['user_data', 'normal_followers_count', ], 'name': 'user_normal_followers_count', 'type': 'INT NOT NULL'},
            {'key': ['user_data', 'protected', ], 'name': 'user_protected', 'type': 'BOOL NOT NULL'},
            # {'key': ['user_data', 'profile_background_image_url', ], 'name': 'user_profile_background_image_url', 'type': 'TEXT DEFAULT ""'},
            # {'key': ['user_data', 'profile_banner_url', ], 'name': 'user_profile_banner_url', 'type': 'TEXT DEFAULT ""'},
            # {'key': ['user_data', 'profile_image_url', ], 'name': 'user_profile_image_url', 'type': 'TEXT DEFAULT ""'},
            {'key': ['user_data', 'screen_name', ], 'name': 'user_screen_name', 'type': 'TEXT NOT NULL'},
            {'key': ['user_data', 'statuses_count', ], 'name': 'user_statuses_count', 'type': 'INT NOT NULL'},
            {'key': ['user_data', 'verified', ], 'name': 'user_verified', 'type': 'BOOL NOT NULL'},
        ]
        self.reddit_format = [
            {'key': ['id', ], 'name': 'id', 'type': 'VARCHAR(32) NOT NULL'},
            {'key': ['link_id', ], 'name': 'link_id', 'type': 'TEXT DEFAULT ""'},
            {'key': ['parent_id', ], 'name': 'parent_id', 'type': 'TEXT DEFAULT ""'},
            {'key': ['nest_level', ], 'name': 'nest_level', 'type': 'INT DEFAULT 1'},
            {'key': ['reply_delay', ], 'name': 'reply_delay', 'type': 'TEXT DEFAULT ""'},
            {'key': ['controversiality', ], 'name': 'controversiality', 'type': 'BOOL DEFAULT 0'},
            {'key': ['body', ], 'name': 'body', 'type': 'TEXT NOT NULL'},
            {'key': ['datetime', ], 'name': 'datetime', 'type': 'DATETIME NOT NULL'},
            {'key': ['day_of_week', ], 'name': 'day_of_week', 'type': 'TEXT NOT NULL'},
            {'key': ['date', ], 'name': 'date', 'type': 'DATE NOT NULL'},
            {'key': ['time', ], 'name': 'time', 'type': 'TIME NOT NULL'},
            {'key': ['score', ], 'name': 'score', 'type': 'INT NOT NULL'},
            # {'key': ['comments', ], 'name': 'comments', 'type': 'INT NOT NULL'},
            {'key': ['subreddit', ], 'name': 'subreddit', 'type': 'TEXT NOT NULL'},
            {'key': ['subreddit_id', ], 'name': 'subreddit_id', 'type': 'TEXT NOT NULL'},
            # {'key': ['type', ], 'name': 'type', 'type': 'TEXT NOT NULL'},
            {'key': ['update_datetime', ], 'name': 'update_datetime', 'type': 'DATETIME NOT NULL'},
            {'key': ['author', ], 'name': 'author', 'type': 'TEXT NOT NULL'},
            {'key': ['author_fullname', ], 'name': 'author_fullname', 'type': 'TEXT DEFAULT ""'},
            {'key': ['author_datetime', ], 'name': 'author_datetime', 'type': 'DATETIME NOT NULL'},
        ]

    def _exec(self, cmd):
        while True:
            try:
                cur = self.conn.cursor()
                cur.execute(cmd)
                return cur
            except mysql.connector.Error as e:
                if e.errno == -1:
                    # No database connection
                    if self.verbose:
                        L.log(self.db_label, 'No database connection')
                    self.reconnect()
                    continue
                elif e.errno == 1146:
                    # No table
                    if self.verbose:
                        L.log(self.db_label, 'Table does not exist for command {}'.format(cmd))
                    return None
                else:
                    if self.verbose:
                        L.log(self.db_label, 'Error no {}. Error executing command {}'.format(e.errno, cmd), e)
                    return None
            except Exception as e:
                if self.verbose:
                    L.log(self.db_label, 'Error executing command {}'.format(cmd), e)
                return None

    def _call(self, cmd):
        cur = self._exec(cmd)
        if cur is not None:
            cur.close()
            self.conn.commit()
            return True
        return False

    def _fetch(self, cmd):
        try:
            cur = self._exec(cmd)
            if cur is None:
                return None
            res = cur.fetchall()
            cur.close()
            self.conn.commit()
            return res
        except mysql.connector.Error as e:
            L.log(self.db_label, 'Error no {}. Error fetching command {}'.format(e.errno, cmd), e)
        except Exception as e:
            if self.verbose:
                L.log(self.db_label, 'Error fetching command {}'.format(cmd), e)
        return None

    def _to_dict(self, list, type):
        if list is None:
            return None
        table_format = self.table_format(type)
        keys = [k['name'] for k in table_format]
        return {key: val for key, val in zip(keys, list)}

    def reconnect(self):
        if self.verbose:
            L.log(self.db_label, 'Reconnecting to database')
        # self.conn.reconnect(attempts=self.reconnect_tries, delay=self.reconnect_delay)
        time.sleep(1)
        self.conn.close()
        time.sleep(2)
        self.conn = mysql.connector.connect(**self.config)
        time.sleep(1)

    def close(self):
        if self.verbose:
            L.log(self.db_label, 'Closing database connection')
        self.conn.close()
        self.conn = None

    def table_name(self, symbol, type):
        if type == 'twitter':
            return 'Twitter_{}'.format(symbol)
        elif type == 'reddit':
            return 'Reddit_{}'.format(symbol)
        else:
            if self.verbose:
                L.log(self.db_label, 'Error getting table name. Unknown type'.format(type))
            raise Exception('Error getting table name. Unknown type {}'.format(type))

    def table_format(self, type):
        if type == 'twitter':
            return self.twitter_format
        elif type == 'reddit':
            return self.reddit_format
        else:
            if self.verbose:
                L.log(self.db_label, 'Error getting table format. Unknown type'.format(type))
            raise Exception('Error getting table format. Unknown type {}'.format(type))

    def create_table(self, symbol, type):
        table_name = self.table_name(symbol, type)
        table_format = self.table_format(type)

        cmd = 'CREATE TABLE IF NOT EXISTS\n{}(\n'.format(table_name)
        for row in table_format:
            cmd += '\t{} {},\n'.format(row['name'], row['type'])
        cmd += '\tPRIMARY KEY ({})\n);'.format(table_format[0]['name'])
        # L.log(cmd)
        return self._call(cmd)

    def drop_table(self, symbol, type):
        table_name = self.table_name(symbol, type)
        cmd = 'DROP TABLE IF EXISTS {};'.format(table_name)
        # L.log(cmd)
        return self._call(cmd)

    def add_data(self, symbol, data, type):
        table_name = self.table_name(symbol, type)
        table_format = self.table_format(type)

        attributes = ', '.join([row['name'] for row in table_format])
        placeholders = ', '.join(['%s' for _ in table_format])
        cmd = 'INSERT INTO {} ({}) VALUES ({});'.format(table_name, attributes, placeholders)
        values = []
        for datum in data:
            value_row = []
            for row in table_format:
                value = datum
                for key in row['key']:
                    if key not in value:
                        value = None
                        break
                    value = value[key]
                value_row.append(value)
            values.append(value_row)
        # L.log(cmd)
        # L.log(values)

        if len(values) == 0:
            return True

        step = 100
        start = 0
        end = min(start+step, len(values))
        # duplicates = []
        while True:
            vals = values[start:end+1]
            try:
                # cur = self.conn.cursor()
                # cur.execute(cmd, val)
                # cur.close()
                # self.conn.commit()
                cur = self.conn.cursor()
                cur.executemany(cmd, vals)
                cur.close()
                self.conn.commit()
            except mysql.connector.Error as e:
                if e.errno == 1062:
                    # Duplicate entry
                    pass
                    # duplicates.append(data[i]['datetime'])
                elif e.errno == 2006:
                    # Database connection lost
                    self.reconnect()
                    continue
                else:
                    if self.verbose:
                        L.log(self.db_label, 'Error no {}. Error adding data to table {}'.format(e.errno, table_name), e)
                    return False
            except Exception as e:
                if self.verbose:
                    L.log(self.db_label, 'Error while adding data to table {}'.format(table_name), e)
                return False

            if end >= len(values):
                break
            start += step
            end = min(start+step, len(values))

        # if len(duplicates) > 0 and self.verbose:
        #     L.log(self.db_label, '{} duplicate entries for {} {} - {}'.format(len(duplicates), table_name, duplicates[0], duplicates[-1]))
        return True

        # while True:
        #     try:
        #         cur = self.conn.cursor()
        #         cur.executemany(cmd, values)
        #         cur.close()
        #         self.conn.commit()
        #         return True
        #     except mysql.connector.Error as e:
        #         if e.errno == 1062:
        #             # Duplicate entry
        #             if self.verbose:
        #                 L.log(self.db_label, 'Duplicate entry to table {}'.format(table_name))
        #             return True
        #         elif e.errno == 2006:
        #             # Database connection lost
        #             if self.verbose:
        #                 L.log(self.db_label, 'Database connection lost')
        #             self.reconnect()
        #             continue
        #         else:
        #             if self.verbose:
        #                 L.log(self.db_label, 'Error no {}. Error adding data to table {}'.format(e.errno, table_name), e)
        #             break
        #     except Exception as e:
        #         if self.verbose:
        #             L.log(self.db_label, 'Error while adding data to table {}'.format(table_name), e)
        #             break
        # return False

    def del_data(self, symbol, type, hours):
        table_name = self.table_name(symbol, type)
        tz_offset = datetime.timedelta(hours=8)
        since = datetime.datetime.now() + tz_offset - datetime.timedelta(hours=hours)
        cmd = 'DELETE FROM {} WHERE datetime>"{}";'.format(table_name, since.strftime('%Y-%m-%d %H:%M:%S'))
        res = self._call(cmd)
        L.log(res)

        n_rows = 0
        if res is not None:
            n_rows = res.rowcount
        if self.verbose:
            L.log(self.db_label, '{} deleted {} rows'.format(symbol, n_rows))

    def size(self, symbol, type):
        table_name = self.table_name(symbol, type)
        cmd = 'SELECT COUNT(*) FROM {};'.format(table_name)
        res = self._fetch(cmd)
        if res is None or len(res) == 0:
            return None
        return res

    def get_first(self, symbol, type, order_by='datetime', order='DESC'):
        table_name = self.table_name(symbol, type)
        cmd = 'SELECT * FROM {} ORDER BY {} {} LIMIT 1;'.format(table_name, order_by, order)
        res = self._fetch(cmd)
        if res is None or len(res) == 0:
            return None
        return self._to_dict(res[0], type)

    def newest(self, symbol, type):
        return self.get_first(symbol, type, order_by='datetime', order='DESC')

    def oldest(self, symbol, type):
        return self.get_first(symbol, type, order_by='datetime', order='ASC')
