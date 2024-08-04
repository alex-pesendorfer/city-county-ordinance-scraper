import os
from bs4 import BeautifulSoup
from typing import Callable
import tiktoken
import csv
from openai import OpenAI
import logging
from typing import List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
client = OpenAI(api_key=os.environ['OPENAI_API_KEY'])

# Token counting functions
def count_tokens(text: str) -> int:
    encoding = tiktoken.get_encoding("cl100k_base")
    return len(encoding.encode(text))

# Text extraction functions
def extract_text_from_html(content: str) -> str:
    soup = BeautifulSoup(content, 'html.parser')
    return soup.get_text(separator=' ', strip=True)

def extract_text_from_markdown(content: str) -> str:
    return content  # For markdown, we'll just return the content as is

def extract_text(content: str, file_type: str) -> str:
    if file_type == 'html':
        return extract_text_from_html(content)
    else:  # markdown
        return extract_text_from_markdown(content)

# File processing functions
def process_file(file_path: str, content: str, file_type: str, issue, city_county, state) -> dict:
    text = extract_text(content, file_type)
    token_count = count_tokens(text)
    
    # LLM API call
    impacts_business = call_llm_api(text, issue, city_county, state)
    
    return {
        'file_path': file_path,  # Changed from 'file_name' to 'file_path'
        'impacts_business': int(impacts_business),  # Convert boolean to 0 or 1
        'token_count': token_count
    }

def process_files(directory: str, issue, city_county, state) -> tuple:
    results = []
    total_tokens = 0
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(('.html', '.md')):
                file_path = os.path.join(root, file)
                file_type = 'html' if file.endswith('.html') else 'markdown'
                
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                result = process_file(file_path, content, file_type, issue, city_county, state)
                results.append(result)
                total_tokens += result['token_count']
    
    return results, total_tokens

def chunk_content(content: str, max_tokens: int = 100000) -> List[str]:
    """Split the content into chunks of approximately max_tokens."""
    encoding = tiktoken.encoding_for_model("gpt-4")
    tokens = encoding.encode(content)
    chunks = []
    
    for i in range(0, len(tokens), max_tokens):
        chunk = encoding.decode(tokens[i:i + max_tokens])
        chunks.append(chunk)
    
    return chunks

def call_llm_api(content: str, issue: str, city_county: str, state: str) -> bool:
    chunks = chunk_content(content)
    
    for chunk in chunks:
        prompt = f"""
        Does any of the provided page content discuss the topic of {issue} ordinances in the {city_county}, {state}? Answer with 'True' or 'False' and do not output anything else.

        It is worse to output a false negative than a false positive. If you are unsure, please answer 'True'.

        Page Content:
        {chunk}
        """

        response = client.chat.completions.create(
            model="gpt-4-0125-preview",
            messages=[
                {"role": "system", "content": "You are an assistant that extracts specific information from web page content and formats it as a Python dictionary."},
                {"role": "user", "content": prompt}
            ]
        )

        result = response.choices[0].message.content.strip().lower()
        print(f"Chunk result: {result}")

        if 'true' in result:
            return True

    return False


# LLM API function
def summarize(content, issue, city_county, state):
    prompt = f"""
    Read the following text, which is a concatenation of various files about {issue} ordinances in the {city_county}, {state}.

    Provide a structured summary of the overall content. The summary should use lists to capture the key features of short term rental policy in {city_county}, without dropping any of the policy details.
    Be sure that the summary lists explain who can host, what kind of permit might be needed to host, and what the operating conditions are for a short term rental. Again, do not gloss over any of the fine-grained details,
    as those are what are most important to the user in order to help them remain compliant. A brush-stroke over the details will not be helpful to the user.

    Page Content:
    {content}
    """

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are an assistant that summarizes local ordinance data from a collection of sources."},
            {"role": "user", "content": prompt}
        ]
    )

    print(response.choices[0].message.content)

    return response.choices[0].message.content


# CSV output function
def write_to_csv(results: list, output_file: str):
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['file_path', 'impacts_business', 'token_count']  # Changed 'file_name' to 'file_path'
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for result in results:
            writer.writerow(result)

def main():
    crawled_directory = 'crawled_pages'
    output_file = 'business_impact_assessment.csv'
    
    # Process all files
    results, total_tokens = process_files(crawled_directory)
    
    # Write results to CSV
    write_to_csv(results, output_file)
    
    # Print summary
    impacting_files = sum(result['impacts_business'] for result in results)
    total_files = len(results)
    print(f"Files that impact the business: {impacting_files} out of {total_files}")
    print(f"Total token count across all files: {total_tokens}")
    print(f"Results written to {output_file}")

if __name__ == "__main__":
    main()