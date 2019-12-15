FROM python:3.7-slim

ADD . reddit_recommender

RUN mkdir ./reddit_recommender/dumps

RUN python3 -m pip install -r ./reddit_recommender/requirements.txt

ENTRYPOINT [ "python3", "-u", "./reddit_recommender/reddit_scraper.py" ]
