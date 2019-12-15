FROM python:3.7-slim

ADD . reddit_recommender

WORKDIR /reddit_recommender

RUN mkdir ./dumps

RUN python3 -m pip install -r ./requirements.txt

ENTRYPOINT [ "python3", "-u", "./reddit_scraper.py" ]
