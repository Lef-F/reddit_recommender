try:
	import secrets.secrets
except ModuleNotFoundError:
	print('WARNING! No secrets.py file found, assuming enviroment variables are already loaded!')

import os
import sys
import praw
import json
import logging
import requests
import pandas as pd
from bs4 import BeautifulSoup
from google.cloud import bigquery

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
	try:
		response = requests.get(url)

		# Skip non-HTML content
		if 'text/html' in response.headers['content-type']:
			soup = BeautifulSoup(response.content, 'html.parser')

			# Remove all script and style elements
			for script in soup(['script', 'style']):
				script.decompose()

			output = [ text for text in soup.find_all(text=True) if text != '\n']
		return '\n'.join(output)
	except (requests.exceptions.MissingSchema, requests.exceptions.SSLError):
		# In cases where a badly-formed URL is provided or the website got an SSL error skip scraping
		return ''

def upload_to_bq(data):
	# Create bigquery client
	client = bigquery.Client()

	# Get dataset reference
	datasetname = os.environ['DATASET']
	dataset_ref = client.dataset(datasetname)

	# Check if dataset exists, otherwise create
	try:
		client.get_dataset(dataset_ref)
	except Exception as e:
		logging.warn(e)
		logging.warn('Creating dataset: %s' % (datasetname))
		client.create_dataset(dataset_ref)

	# create a bigquery load job config
	job_config = bigquery.LoadJobConfig()

	schema = os.environ['BQ_SCHEMA']
	if schema != '':
		with open(schema, 'r') as f:
			job_config.schema = json.load(f)
	else:
		print('No schema file found for env var BQ_SCHEMA. \nContinuing with schema autodetection on.')
		job_config.autodetect = True

	job_config.create_disposition = 'CREATE_IF_NEEDED'
	job_config.source_format = 'NEWLINE_DELIMITED_JSON'
	job_config.write_disposition = 'WRITE_TRUNCATE'

	# create a bigquery load job
	try:
		load_job = client.load_table_from_file(
			data,
			os.environ['TABLE_PATH'],
			job_config=job_config,
		)
		print('Load job: %s [%s]' % (
			load_job.job_id,
			os.environ['TABLE_PATH']
		))
	except Exception as e:
		logging.error('Failed to create load job: %s' % (e))


def main(limit):
	"""Reddit scraper.
	Fetches the data from X = `limit` latest reddit posts, their comments and possible external text
	and pushes it to a BigQuery table.

	Args:
		limit (int): Number of new posts to scrape.
	Returns:
		None
	"""
	if type(limit) != int:
		try:
			limit = int(limit)
		except ValueError:
			raise ValueError('Limit must be an integer!')
	
	print('Now working to fetch the latest', limit, 
		'new posts from multireddit', os.environ['MULTI'], 
		'and user', os.environ['REDDIT_USER'])

	client = RedditClient()
	multi = client.reddit.multireddit(os.environ['REDDIT_USER'], os.environ['MULTI'])

	record = list()

	for ind, thread in enumerate(multi.new(limit=limit)):
		if not ind % 10:
			print('Scraped', ind, 'out of', limit)

		submission_data = client._reddit_data()
		for attr in submission_data.keys():
			submission_data[attr] = getattr(thread, attr)

		# TODO: Figure out how to fix this elegantly
		submission_data['created_utc'] = int(submission_data['created_utc'])

		submission_data['author'] = submission_data['author'].name
		submission_data['subreddit'] = submission_data['subreddit'].display_name

		submission_data.update(client._custom_data())

		# Replace Comment objects with their text body, if there is any
		thread.comments.replace_more(limit=None)
		submission_data['comments'] = list()
		for comment in thread.comments.list():
			submission_data['comments'].append(getattr(comment, 'body'))

		if not submission_data['is_self']:
			submission_data['external_text'] = external_url_scraper(submission_data['url'])
		else:
			submission_data['external_text'] = ''

		record.append(submission_data)

	print('All data collected. Now processing BigQuery load.')
	df = pd.DataFrame(record)
	df.to_json('./dumps/reddit_post_dump.json', lines=True, orient='records')
	with open('./dumps/reddit_post_dump.json', 'rb') as f:
		upload_to_bq(f)
	print('Data uploaded successfully to', os.environ['TABLE_PATH'])

if __name__ == "__main__":
	args = sys.argv
	if len(args) > 2:
		logging.error('Too many arguments! Please specify number of new posts to fetch.')
		sys.exit(1)
	elif len(args) == 2:
		main(args[1])
		sys.exit(0)
	else:
		print('Welcome to the reddit scraper!')
		print('This script takes the number of new reddit posts as input.')
		print('e.g. python3 reddit_scraper.py 100')
		sys.exit(0)
	