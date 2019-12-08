try:
	import secrets
except ModuleNotFoundError:
	print('WARNING! No secrets.py file found, assuming enviroment variables are already loaded!')

import os
import sys
import praw
import json
import requests
from flask import jsonify
from bs4 import BeautifulSoup


class RedditClient:
	"""Class to produce connected instance of Reddit.
	"""
	def __init__(self):
		self.reddit = praw.Reddit(
			client_id=os.environ.get('CLIENT_ID'),
			client_secret=os.environ.get('CLIENT_SECRET'),
			user_agent=os.environ.get('USER_AGENT')
			)

	def _reddit_data(self):
		"""Definition of the attributes specific to the PRAW Python library which 
		are to be extracted from a praw.models.Submission instance.
		"""
		return {
				'id': 'reddit thread id',
				'author': self.reddit.redditor(name='none'), #'author name from Redditor.name'
				'created_utc': 'time posted',
				'num_comments': 'total number of comments',
				'score': 'submission score (number of upvotes)',
				'subreddit': self.reddit.subreddit(display_name='none'), #'subreddit in which it was posted from Subreddit.title'
				'title': 'title of the submission',
				'upvote_ratio': 'the ratio of upvotes from all votes',
				'permalink': 'link to submission',
				'url': 'url the submission might be pointing to, or permalink',
				'selftext': 'the submission\'s text content if any',
				'is_self': 'if the submission is a self-post or a URL'
			}
	def _custom_data(self):
		"""Definition of the custom data that are to be extracted. 
		"""
		return {
			'comments': 'list of comments in the reddit thread',
			'external_text': 'external URL scraped text content, if the submission is linking to an external URL'
		}


def external_url_scraper(url: str):
	"""Scrape all text from given URL
	Args:
		url (str): A valid URL
	Returns:
		A list with all the available text from the webpage.
	"""
	output = list()
	response = requests.get(url)

	# Skip non-HTML content
	if 'text/html' in response.headers['content-type']:
		soup = BeautifulSoup(response.content, 'html.parser')

		# Remove all script and style elements
		for script in soup(['script', 'style']):
			script.decompose()
		
		output = [ text for text in soup.find_all(text=True) if text != '\n']
	return output


def main(request=None):
	"""Responds to any HTTP request.
	Args:
		request (flask.Request): HTTP request object.
	Returns:
		The response text or any set of values that can be turned into a
		Response object using
		`make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
	"""
	client = RedditClient()
	multi = client.reddit.multireddit('faaaaaart', 'data')

	record = list()

	for thread in multi.new(limit=30):
		submission_data = client._reddit_data()
		for attr in submission_data.keys():
			submission_data[attr] = getattr(thread, attr)

		submission_data['author'] = submission_data['author'].name
		submission_data['subreddit'] = submission_data['subreddit'].display_name

		submission_data.update(client._custom_data())

		# Replace Comment objects with their text body, if there is any
		thread.comments.replace_more()
		submission_data['comments'] = list()
		for comment in thread.comments.list():
			submission_data['comments'].append(getattr(comment, 'body'))

		if not submission_data['is_self']:
			submission_data['external_text'] = external_url_scraper(submission_data['url'])
		else:
			submission_data['external_text'] = ''
		
		record.append(submission_data)
	json_dump = json.dumps(record)

	try:
		return jsonify(record)
	except RuntimeError:
		with open('reddit_post_dump.json', 'w+') as f:
			f.write(json_dump)
	sys.exit(1)

if __name__ == "__main__":
	main()