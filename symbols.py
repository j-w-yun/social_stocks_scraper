# Copyright 2021 Jaewan Yun <jaeyun@ucdavis.edu>
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
# The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.


import csv
import nltk


class Dictionary:
    def __init__(self):
        self.lower_words = []
        try:
            self.initialize()
        except LookupError:
            nltk.download('words')
            self.initialize()

    def initialize(self):
        self.lower_words = [w.lower() for w in nltk.corpus.words.words()]

    def is_word(self, word):
        """Check if word exists in the English dictionary.
        """
        return word.lower() in self.lower_words


class Symbols:
    def __init__(self):
        self.filename = 'data/symbol_table.csv'
        self.symbols_list = []
        self.symbols_dict = {}
        self.dictionary = Dictionary()
        self.read_csv()

    def read_csv(self):
        if len(self.symbols_list) > 0 and len(self.symbols_dict) > 0:
            return
        with open(self.filename, 'r', encoding='utf-8') as f:
            dw = csv.DictReader(f, delimiter='|')
            for row in dw:
                symbol = row
                symbol['symbol'] = symbol['symbol'].strip()
                self.symbols_list.append(symbol)
        self.symbols_list = sorted(self.symbols_list, key=lambda k: k['symbol'])
        self.symbols_dict = {k['symbol']: k for k in self.symbols_list}

    def get_list(self):
        if len(self.symbols_list) > 0:
            return self.symbols_list
        self.read_csv()
        return self.symbols_list

    def get_dict(self):
        if len(self.symbols_dict) > 0:
            return self.symbols_dict
        self.read_csv()
        return self.symbols_dict

    def get_info(self, symbol):
        return self.symbols_dict[symbol]

    def in_dictionary(self, word):
        return self.dictionary.is_word(word)

    def _trim_company_name(self, company_name):
        name = company_name.split(',')[0]
        name = name.lower()
        name = name.strip()
        split_words = []

        break_on_word = False
        split_name = name.split()
        for i, word in enumerate(split_name):
            # Skip articles
            if len(split_words) == 0 and word in ['a', 'an', 'the']:
                continue
            # Break on English word after observing non-English word
            if not self.in_dictionary(word):
                if len(split_words) > 0:
                    break_on_word = True
            elif break_on_word:
                break
            split_words.append(word)

        remove_words = [
            'corporation',
            'corp',
            'cor',
            'etf',
            'incorporated',
            'inc',
            'limited',
            'ltd',
        ]
        if len(split_words) > 0:
            last_word = split_words[-1].replace('.', '')
            if last_word in remove_words:
                split_words = split_words[:-1]

        # Join name
        name = ' '.join(split_words)

        # Replace ambiguous name
        if len(name) <= 5 and self.in_dictionary(name):
            if self.in_dictionary(company_name.lower()):
                return None
            name = company_name.lower()
            name = name.replace('.', '')
            name = name.replace(',', '')
        if len(name) <= 3:
            return None
        return name

    def company_name(self, symbol):
        if isinstance(symbol, str):
            symbol = self.symbols_dict[symbol]

        long_name = self._trim_company_name(symbol['longName'])
        short_name = self._trim_company_name(symbol['shortName'])
        name = symbol['symbol']
        if long_name is not None:
            name = long_name
        elif short_name is not None:
            name = short_name
        if short_name is not None and long_name is not None:
            if '(' in short_name:
                name = long_name
            elif '(' in long_name:
                name = short_name

        # print('{}|{} \t\t {}|{}'.format(symbol['symbol'], name, symbol['longName'], symbol['shortName']))
        return name


if __name__ == '__main__':
    symbols = Symbols()

    info = symbols.get_list()
    for i in info:
        symbol = i['symbol']
        symbols.company_name(symbol)

    # info = symbols.get_dict()
    # for k, v in info.items():
    #     print(k, v)
