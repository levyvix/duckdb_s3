import dlt
import requests as rq
from dlt.sources.rest_api import rest_api_source
from dlt.sources.rest_api.typing import RESTAPIConfig


@dlt.resource
def get_json_data(process_date):
    response = rq.get(f"https://data.gharchive.org/{process_date}.json.gz")
    
    
