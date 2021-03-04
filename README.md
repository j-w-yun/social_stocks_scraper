# Social Media Stock Data Scraper
Scrape all of Reddit and Twitter using Tor and MySQL.

![alt tag](https://github.com/Jaewan-Yun/social_stocks_scraper/blob/main/res/screenshot.png)

## About
Scrapes all accessible Tweets and Reddit text data from mid-2005 for Reddit and early-2006 for Twitter up to the present date. The scripts fetch textual content mentioning any one of over 10,000 NASDAQ and NYSE-listed stock symbols or their company names using a unique IP for each thread and stores the data in tables of a MySQL schema.


## Usage
Scrape Reddit
```
python reddit.py
```

Scrape Twitter
```
python twitter.py
```


## Requirements
An example `config.json` to place in the root project directory.
```
{
  "host": "your.mysql.database.rds.amazonaws.com",
  "port": 3306,
  "user": "jae",
  "password": "&$%|@#2130rMm*s0JFiao3-291_=+932j1m@#@rg$<>!",
  "database": "stocks",
  "tor_password": "A238jf@2283#Fm290874i3n4@1!",
  "tor_port": 9050,
  "tor_controller_port": 9051,
  "reddit_start_date": "2010-01-01 00:00:00",
  "twitter_start_date": "2010-01-01 00:00:00",
  "reddit_n_threads": 16,
  "twitter_n_threads": 16
}
```

`tor_port` corresponds to the port on which your Tor service is listening. `tor_controller_port` corresponds to the port on which your Tor controller is listening for a NEWNYM signal. `tor_password` is your authentication credential for the Tor controller.

Your database should be in public mode to allow connections using a database user name and password. In addition, it should be able to handle the number of concurrent connections up to the number of `reddit_n_threads` and `twitter_n_threads`. Each thread uses a unique Tor pathway to access twitter.com and pushshift.io, so be wary of the number of threads you spawn!

Regarding space requirements, the combined disk space used by stock symbols that start with the letter 'A' from 2018 to 2020 takes up approximately 10 gigabytes.


## Legal Disclaimer
This tool is created for the sole purpose of security awareness and education, it should not be used against systems that you do not have permission to test/attack. The author is not responsible for misuse or for any damage that you may cause. You agree that you use this software at your own risk.
