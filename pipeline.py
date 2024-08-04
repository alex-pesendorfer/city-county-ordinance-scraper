from browse import get_ordinance_links
from crawler import crawl_websites
from process import process_files, write_to_csv, summarize, count_tokens
import pandas as pd
import os

def create_efficient_chunks(content, max_tokens=100000):
    chunks = []
    current_chunk = ""
    current_tokens = 0

    for line in content.split('\n'):
        line_tokens = count_tokens(line)
        if current_tokens + line_tokens > max_tokens:
            if current_chunk:
                chunks.append(current_chunk.strip())
            current_chunk = line
            current_tokens = line_tokens
        else:
            current_chunk += '\n' + line
            current_tokens += line_tokens

    if current_chunk:
        chunks.append(current_chunk.strip())

    return chunks

# Get ordinance links
issue = "short term rental"
city_county = "Humboldt County"
state = "CA"

ordinance_links = get_ordinance_links(issue, city_county, state)
print(f"Found {len(ordinance_links)} ordinance links")
for link in ordinance_links:
    print(link)

# Crawl websites from ordinance links
pdf_links, visited_pages = crawl_websites(ordinance_links, delay=1)

print(f"Crawled {len(visited_pages)} pages.")
print(f"Found and processed {len(pdf_links)} PDF links:")
for link in pdf_links:
    print(link)

# Process HTML files and PDFs
crawled_directory = 'crawled_pages'
output_file = 'business_impact_assessment.csv'

results, total_tokens = process_files(crawled_directory, issue, city_county, state)

# Write results to CSV
write_to_csv(results, output_file)

# Print summary
impacting_files = sum(result['impacts_business'] for result in results)
total_files = len(results)
print(f"Files that impact the business: {impacting_files} out of {total_files}")
print(f"Total token count across all files: {total_tokens}")
print(f"Results written to {output_file}")

# Read the CSV file
df = pd.read_csv('business_impact_assessment.csv')

# Take the rows that have 'impacts_business' as True and concatenate those files into a single text file
impacting_files = df[df['impacts_business'] == 1]
output_text_file = 'impacting_files.txt'

with open(output_text_file, 'w', encoding='utf-8') as textfile:
    for file_path in impacting_files['file_path']:
        with open(file_path, 'r', encoding='utf-8') as file:
            textfile.write(file.read())
            textfile.write('\n\n')

print(f"Concatenated text of impacting files written to {output_text_file}")

# Read the concatenated content
output_text_file = 'impacting_files.txt'
with open(output_text_file, 'r', encoding='utf-8') as file:
    content = file.read()

# Create efficient chunks
chunks = create_efficient_chunks(content)

# Summarize each chunk
chunk_summaries = []
for i, chunk in enumerate(chunks):
    print(f"Summarizing chunk {i+1} of {len(chunks)}...")
    chunk_summary = summarize(chunk, issue, city_county, state)
    chunk_summaries.append(chunk_summary)

# Combine chunk summaries
combined_summary = "\n\n".join(chunk_summaries)

# Final summarization of combined summaries
if len(chunks) > 1:
    print("Creating final summary...")
    final_summary = summarize(combined_summary, issue, city_county, state)
else:
    final_summary = combined_summary

# Write the final summary to a text file
output_summary_file = 'business_impact_summary.txt'
with open(output_summary_file, 'w', encoding='utf-8') as file:
    file.write(final_summary)

print(f"Final summary written to {output_summary_file}")