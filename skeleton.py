import json
import requests
from bs4 import BeautifulSoup
import time
import logging
import os
from datetime import datetime
import pytz
from functools import lru_cache
from urllib.parse import urlparse
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from openai import OpenAI
import re
import html
import traceback

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clean_llm_response_dict(text):
    text = text.strip()
    code_block_pattern = r'^```[\w\s]*\n|```$'
    text = re.sub(code_block_pattern, '', text, flags=re.MULTILINE)
    text = text.replace('`', '')
    language_specifiers = ['plaintext', 'json', 'python', 'html', 'javascript']
    for specifier in language_specifiers:
        if text.lower().startswith(specifier + '\n'):
            text = text[len(specifier)+1:]
    text = text.strip()
    text = re.sub(r'^[\'"]|[\'"]$', '', text)
    text = text.replace('\n', '')
    
    start_index = text.find('{')
    end_index = text.rfind('}')
    
    if start_index != -1 and end_index != -1 and start_index < end_index:
        text = text[start_index:end_index+1]
    else:
        return ""
    
    print()
    print(text)
    text = text.replace('\\"', '\\\\"')
    print()
    print(text)
    print()
    return text

def clean_llm_response_list(text):
    text = text.strip()
    code_block_pattern = r'^```[\w\s]*\n|```$'
    text = re.sub(code_block_pattern, '', text, flags=re.MULTILINE)
    text = text.replace('`', '')
    language_specifiers = ['plaintext', 'json', 'python', 'html', 'javascript']
    for specifier in language_specifiers:
        if text.lower().startswith(specifier + '\n'):
            text = text[len(specifier)+1:]
    text = text.strip()
    text = re.sub(r'^[\'"]|[\'"]$', '', text)
    text = text.replace('\n', '')
    
    start_index = text.find('[')
    end_index = text.rfind(']')
    
    if start_index != -1 and end_index != -1 and start_index < end_index:
        text = text[start_index:end_index+1]
    else:
        return ""

    return text

def normalize_netloc(url):
    parsed_url = urlparse(url)
    netloc = parsed_url.netloc
    if netloc.startswith("www."):
        netloc = netloc[4:]
    return netloc

class HearingScraper:
    def __init__(self, retries=3):
        self.retries = retries
        self.client = OpenAI()
        
    def fetch_page_content(self):
        for _ in range(self.retries):
            try:
                response = requests.get(self.url)
                response.raise_for_status()
                return response.text
            except requests.RequestException as e:
                logger.warning(f"Error fetching page content: {e}. Retrying...")
        logger.error(f"Failed to fetch page content after {self.retries} attempts.")
        return None
    
    def remove_polygon_and_path_tags(self, html_content):
        soup = BeautifulSoup(html_content, 'html.parser')
        for tag in soup.find_all(['polygon', 'path']):
            tag.decompose()
        return str(soup)
    
    def get_final_url(self, url):
        response = requests.get(url, allow_redirects=True)
        return response.url

    @lru_cache(maxsize=128)
    def get_committee(self): 
        uri = "mongodb+srv://rescript-user:" + os.environ["RESCRIPT_CLUSTER_PASS"] + "@cluster0.uyfwz.mongodb.net/?retryWrites=true&w=majority"
        client = MongoClient(uri, server_api=ServerApi('1'))
        try:
            client.admin.command('ping')
            db = client['rescript-local']
            committee_collection = db['committeesAndSubcommittees']
        except Exception as e:
            logger.error(f"Error connecting to MongoDB: {e}")
            return {"committee_id": None, "committee": None, "subcommittee_dict": None}

        if "cha.house.gov" in self.url:
            committees = committee_collection.find({"thomas_id": "HSHA"})
        else:
            committees = committee_collection.find({})
        for committee in committees:
            if any(url_key in committee for url_key in ["url", "minority_url"]):
                if any(
                    committee.get(url_key) and (normalize_netloc(committee[url_key]) == normalize_netloc(self.url))
                    for url_key in ["url", "minority_url"]
                ):
                    subcommittee_dict = {
                        subcommittee["name"]: subcommittee["thomas_id"]
                        for subcommittee in committee.get("subcommittees", [])
                    }
                    subcommittee_dict["Full Committee"] = ""
                    return {
                        "committee_id": committee["thomas_id"],
                        "committee": committee["name"],
                        "subcommittee_dict": subcommittee_dict
                    }
                
        return {"committee_id": None, "committee": None, "subcommittee_dict": None}

    def get_consolidated_llm_response(self, html_content, committee_info):
        prompt = f"""
        Extract the following information from the given HTML content of a Senate hearing page. Return the result as a json.loads-readable Python dictionary with the following keys:

        1. title: The title of the hearing as a string.
        2. video_link: The main hearing video link as a string. This may be in the src attribute of an iframe, or it may be an embedding youtube link. For example, it could be the src in an iframe like this: '<iframe allowfullscreen class="embed-responsive-item" src="https://www.senate.gov/isvp/?type=arch&comm=commerce&filename=commerce071223&auto_play=false&amp;wmode=opaque" width="320" height="240" frameborder="0"></iframe>'. If there are multiple, take the first link. If the HTML content is from an hsgac.senate.gov or indian.senate.gov page with an 'archive_stream' variable, simply output 'url'. If the hearing is a "Committee on House Administration," output the HREF link.
        3. subcommittee: The subcommittee name as a string. Use 'Full Committee' if it's a Full Committee meeting or if no subcommittee is mentioned. The possible subcommittees are the keys in this dictionary: {committee_info['subcommittee_dict']}. Output one of {committee_info['subcommittee_dict'].keys()} (with exact spelling and punctuation as in the list) or "Full Committee" if it's a Full Committee meeting. Remember, if it says "Full Committee" or "Executive Session", output "Full Committee". Do not get confused by the title of the hearing. Do not try to infer the committee name based on content in the title or the description. It is never the case that the subcommittee name is absent from the page, so only work with what is explicitly stated in the HTML content. Moreover, the subcommittee name will not be a shortened version of the aforementioned options. If the hearing mentions "Tax", it is not necessarily a "Taxation and IRS Oversight" subcommittee meeting. The same goes for any topic. Rely only on explicit mentions of the subcommittee name.
        4. subcommittee_id: The subcommittee id as a string. Use an empty string if it's a Full Committee meeting or if no subcommittee is mentioned. Map the identified subcommittee to its corresponding id: {committee_info['subcommittee_dict']} and output the id.
        5. location: The location of the hearing as a string. If not available, use an empty string.
        6. witnesses: A list of strings, each containing the witness's name, title, and organization (if available).
        7. date_time: The date and time of the hearing as a string in the format '%m/%d/%y %I:%M%p'.

        Example output format:
        {{
            "title": "Long-Term Economic Benefits and Impacts from Federal Infrastructure and Public Transportation Investment",
            "video_link": "https://www.senate.gov/isvp/?comm=banking&type=live&stt=&filename=banking073124&auto_play=false&wmode=transparent&poster=https%3A%2F%2Fwww%2Ebanking%2Esenate%2Egov%2Fthemes%2Fbanking%2Fimages%2Fvideo%2Dposter%2Dflash%2Dfit%2Epng",
            "subcommittee": "Economic Policy",
            "subcommittee_id": "12",
            "location": "Dirksen Senate Office Building 538",
            "witnesses": [
                "The Honorable Christopher Coes, Acting Under Secretary of Transportation for Policy, United States Department of Transportation",
                "Mr. Michael Knisley, Executive Secretary-Treasurer, Ohio State Building and Construction Trades Council"
            ],
            "date_time": "07/31/24 10:00AM"
        }}


        Be absolutely certain that the subcommittee was explicitly named. If you output a false positive, that is worse than a false negative. The subcommittee name will never require reasoning, it will be present in the HTML content if it is a subcommittee hearing.

        HTML Content:
        {html_content}
        """

        response = self.client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are an assistant that extracts specific information from web page content and formats it as a Python dictionary."},
                {"role": "user", "content": prompt}
            ]
        )

        try:
            string_response = clean_llm_response_dict(response.choices[0].message.content)
            print("string response", string_response)
            string_response = html.unescape(string_response).encode('utf-8').decode('unicode-escape')
            extracted_data = json.loads(string_response)
            return extracted_data
        except json.JSONDecodeError:
            logger.error("Failed to parse LLM response as JSON")
            return {}
        
    def get_witnesses_llm_response(self, html_content):
        prompt = f"""
        Extract the witnesses from the congressional hearing HTML content as a list of strings, each containing the witness's name, title, and organization (if available).

        Example output format:
        ["The Honorable Christopher Coes, Acting Under Secretary of Transportation for Policy, United States Department of Transportation", 
        "Mr. Michael Knisley, Executive Secretary-Treasurer, Ohio State Building and Construction Trades Council"]
    
        HTML Content:
        {html_content}
        """

        response = self.client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are an assistant that extracts specific information from web page content and formats it as a Python dictionary."},
                {"role": "user", "content": prompt}
            ]
        )

        try:
            string_response = clean_llm_response_list(response.choices[0].message.content)
            string_response = html.unescape(string_response).encode('utf-8').decode('unicode-escape')
            extracted_data = json.loads(string_response)
            return extracted_data
        except json.JSONDecodeError:
            logger.error("Failed to parse LLM response as JSON")
            return {}

    def convert_est_to_utc(self, date_string):
        try:
            local_dt = datetime.strptime(date_string, "%m/%d/%y %I:%M%p")
            eastern = pytz.timezone('US/Eastern')
            local_dt = eastern.localize(local_dt)
            utc_dt = local_dt.astimezone(pytz.UTC)
            return utc_dt.strftime("%Y-%m-%d %H:%M:%S+00:00")
        except ValueError as e:
            logger.error(f"Error converting date: {e}")
            return date_string

    def extract_data(self, html_content, committee_info):
        llm_start = time.time()
        data = self.get_consolidated_llm_response(html_content, committee_info)
        llm_end = time.time()
        logger.info(f"LLM response time: {llm_end - llm_start:.2f} seconds")
        print(data)

        if data['subcommittee'] == 'Full Committee':
            data['subcommittee'] = ""
            data['subcommittee_id'] = ""
        elif data['subcommittee'] and data['subcommittee'] != '':
            data['subcommittee_id'] = committee_info['subcommittee_dict'].get(data['subcommittee'], "")

        if data['date_time']:
            data['date_time'] = self.convert_est_to_utc(data['date_time'])

        return data

    def validate_and_set_defaults(self, data, committee_info):
        if not isinstance(data, dict):
            data = {}

        defaults = {
            "title": "",
            "witnesses": [],
            "committee_id": committee_info['committee_id'] or "",
            "committee": committee_info['committee'] or "",
            "date_time": "",
            "location": "",
            "video_link": "",
            "subcommittee": "",
            "subcommittee_id": ""
        }

        for key, default_value in defaults.items():
            data.setdefault(key, default_value)

        data["witnesses"] = [witness if isinstance(witness, str) else "" for witness in data["witnesses"]]
        data["url"] = self.url
        data["thomas_id"] = data["committee_id"] + data["subcommittee_id"]
        data["video_link"] = data["video_link"].lstrip('/')
        if data["video_link"] != "url":
            if data["video_link"][0:4] != "http" or data["video_link"][0:5] != "https":
                data["video_link"] = "https://" + data["video_link"]
            data["video_link"] = self.get_final_url(data["video_link"])

        if "veterans.house.gov" in self.url:
            response = requests.get(self.url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            href = soup.find('a', string='here')['href']
            response2 = requests.get(href)
            data["witnesses"] = self.get_witnesses_llm_response(response2.text)

        return data

    def scrape(self):
        html_content = self.fetch_page_content()
        if html_content:
            html_content = self.remove_polygon_and_path_tags(html_content)
            committee_info = self.get_committee()
            data = self.extract_data(html_content, committee_info)
            validated_data = self.validate_and_set_defaults(data, committee_info)
            validated_data["scraped"] = True
            return validated_data
        return None

    def update_hearing_data(self, existing_data, new_data):
        # Only keep the title and url from the existing data
        preserved_data = {key: existing_data[key] for key in ["title", "url"] if key in existing_data}
        
        # Overwrite all other fields with new data
        existing_data.update(new_data)
        
        # Restore the preserved fields
        existing_data.update(preserved_data)
        
        existing_data["scraped"] = True
        return existing_data

    def process_hearings(self, input_file, output_file):
        with open(input_file, 'r') as f:
            hearings = json.load(f)

        updated_hearings = []
        for index, hearing in enumerate(hearings):
            try:
                if hearing.get("scraped", False) and hearing.get("video_link", "") != "url":
                    logger.info(f"Skipping already scraped hearing {index + 1}/{len(hearings)}: {hearing.get('url', 'No URL')}")
                    updated_hearings.append(hearing)
                    continue

                url = hearing.get("url")
                if url:
                    self.url = url
                    logger.info(f"Processing hearing {index + 1}/{len(hearings)}: {url}")
                    new_data = self.scrape()
                    if new_data:
                        updated_hearing = self.update_hearing_data(hearing, new_data)
                        updated_hearings.append(updated_hearing)
                    else:
                        logger.warning(f"Failed to scrape data for {url}")
                        hearing["scraped"] = False
                        updated_hearings.append(hearing)
                else:
                    logger.warning(f"Skipping hearing without URL: {hearing}")
                    hearing["scraped"] = False
                    updated_hearings.append(hearing)

                # Save progress after each hearing
                if (index + 1) % 5 == 0 or index == len(hearings) - 1:
                    with open(output_file, 'w') as f:
                        json.dump(updated_hearings, f, indent=2)
                    logger.info(f"Progress saved: {index + 1}/{len(hearings)} hearings processed")

            except Exception as e:
                logger.error(f"Error processing hearing {index + 1}/{len(hearings)}: {e}")
                logger.error(f"Error: {e}")
                logger.error(traceback.format_exc())
                hearing["scraped"] = False
                hearing["error"] = str(e)
                updated_hearings.append(hearing)
                
                # Save progress after an error
                with open(output_file, 'w') as f:
                    json.dump(updated_hearings, f, indent=2)
                logger.info(f"Progress saved after error: {index + 1}/{len(hearings)} hearings processed")

        # Final save
        with open(output_file, 'w') as f:
            json.dump(updated_hearings, f, indent=2)

        logger.info(f"All hearings processed. Results saved to {output_file}")

if __name__ == "__main__":
    input_file = 'House/Armed_Scraped.json'  # Replace with your input file name
    output_file = 'House/Armed_Scraped.json'  # Replace with your desired output file name

    scraper = HearingScraper()
    scraper.process_hearings(input_file, output_file)