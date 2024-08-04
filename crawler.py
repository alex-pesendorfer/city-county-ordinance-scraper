import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import os
import time
import fitz  # PyMuPDF
import io

def crawl_websites(start_urls, max_depth=0, delay=1):
    visited_urls = set()
    pdf_links = set()
    to_visit = [(url, 0) for url in start_urls]

    while to_visit:
        current_url, depth = to_visit.pop(0)
        
        if current_url in visited_urls or depth > max_depth:
            continue
        
        visited_urls.add(current_url)
        
        try:
            # Add delay before each request
            time.sleep(delay)
            
            response = requests.get(current_url)
            
            if current_url.lower().endswith('.pdf'):
                save_pdf_as_markdown(current_url, response.content)
                pdf_links.add(current_url)
            else:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Save page source
                save_page_source(current_url, response.text)
                
                # Find links
                for link in soup.find_all('a'):
                    href = link.get('href')
                    if href:
                        full_url = urljoin(current_url, href)
                        if full_url.lower().endswith('.pdf'):
                            # Download PDF immediately
                            try:
                                pdf_response = requests.get(full_url)
                                save_pdf_as_markdown(full_url, pdf_response.content)
                                pdf_links.add(full_url)
                                print(f"Downloaded and saved PDF: {full_url}")
                            except requests.RequestException as e:
                                print(f"Error downloading PDF {full_url}: {e}")
                        elif depth < max_depth:
                            to_visit.append((full_url, depth + 1))
        
        except requests.RequestException as e:
            print(f"Error crawling {current_url}: {e}")
    
    return list(pdf_links), list(visited_urls)

def save_page_source(url, content):
    parsed_url = urlparse(url)
    domain = parsed_url.netloc
    path = parsed_url.path.strip('/')
    
    if not path:
        path = 'index'
    
    directory = os.path.join('crawled_pages', domain, os.path.dirname(path))
    os.makedirs(directory, exist_ok=True)
    
    filename = os.path.join(directory, f"{os.path.basename(path)}.html")
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(content)
    print(f"Saved HTML: {filename}")

def save_pdf_as_markdown(url, content):
    parsed_url = urlparse(url)
    domain = parsed_url.netloc
    path = parsed_url.path.strip('/')
    
    directory = os.path.join('crawled_pages', domain, os.path.dirname(path))
    os.makedirs(directory, exist_ok=True)
    
    filename = os.path.join(directory, f"{os.path.basename(path)}.md")
    
    try:
        pdf_document = fitz.open(stream=content, filetype="pdf")
        md_content = ""
        
        for page_num in range(len(pdf_document)):
            page = pdf_document.load_page(page_num)
            md_content += page.get_text("markdown")
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(md_content)
        
        print(f"Saved markdown for PDF: {filename}")
    except Exception as e:
        print(f"Error converting PDF to markdown for {url}: {e}")

def main():
    start_urls = [
        'https://www.sandiego.gov/treasurer/short-term-residential-occupancy',
        'https://www.sandiego.gov/city-clerk/officialdocs/municipal-code/chapter-1'
    ]
    delay = 1  # Delay in seconds between requests
    pdf_links, visited_pages = crawl_websites(start_urls, delay=delay)
    
    print(f"Crawled {len(visited_pages)} pages.")
    print(f"Found and processed {len(pdf_links)} PDF links:")
    for link in pdf_links:
        print(link)

if __name__ == "__main__":
    main()