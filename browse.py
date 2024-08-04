import requests
import json
import os
import logging
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo.errors import PyMongoError

try:
    # MongoDB settings
    DB_NAME = os.environ['MONGO_DB_NAME']
    MONGO_DB_USER = os.environ['MONGO_DB_USER']
    RESCRIPT_CLUSTER_PASS = os.environ['RESCRIPT_CLUSTER_PASS']
    VALUESERP_API_KEY = os.environ['VALUESERP_API_KEY']
except KeyError:
    raise KeyError("Environment variables MONGO_DB_NAME, CONGRESS_DATA_API_KEY, CONGRESS, MONGO_DB_USER, and RESCRIPT_CLUSTER_PASS must be set")

logger = logging.getLogger()
console = logging.StreamHandler()
logger.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

# Create a MongoDB client and connect to the server
uri = f"mongodb+srv://{MONGO_DB_USER}:{RESCRIPT_CLUSTER_PASS}@cluster0.uyfwz.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
    db = client[DB_NAME]
    meeting_collection = db['committee-meetings']
    offsets_collection = db['offsets']
    committee_collection = db['committeesAndSubcommittees']
except Exception as e:
    print(e)
    logger.warning("Could not connect to MongoDB or create index. Check your credentials.")

def get_ordinance_links(issue, city_county, state):
    search_query = f"{city_county} {state} {issue} ordinance"
    print(f"Search Query: {search_query}")

    params = {
        'api_key': VALUESERP_API_KEY,
        'q': search_query,
        'location': 'Washington,DC (Hagerstown,MD),United States',
        'google_domain': 'google.com',
        'gl': 'us',
        'hl': 'en',
        'output': 'json'
    }

    api_result = requests.get('https://api.valueserp.com/search', params)
    response = api_result.json()['organic_results']
    print(response)
    return [result['link'] for result in response]