import asyncio
import logging
import boto3
from crawl4ai import AsyncWebCrawler
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import re

# Configuration settings
START_URL = "https://stardewvalleywiki.com/"
BASE_URL = "https://stardewvalleywiki.com/"
S3_BUCKET = "stardew-rag-data"
S3_PREFIX = "stardew_wiki_data/" 

VISITED_PAGES = set()  # Track visited pages

# Initialize logging
logging.basicConfig(level=logging.INFO)

# Set up the AWS S3 client
session = boto3.Session(profile_name="sleepingbeo")
s3_client = session.client('s3')

# Sanitize URL to create valid filename
def sanitize_filename(url):
    return re.sub(r'[<>:"/\\|?*]', '_', url)  # Replace invalid characters

def is_image_url(url):
    """Check if the URL points to an image based on its extension."""
    image_extensions = ['.jpg', '.jpeg', '.png', '.gif']
    return any(url.lower().endswith(ext) for ext in image_extensions)

async def scrape_page(crawler, url):
    """Scrape a single page and return new links."""
    if url in VISITED_PAGES:
        return []

    print(f"Scraping: {url}")
    VISITED_PAGES.add(url)

    try:
        result = await crawler.arun(url=url)
    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return []

    # Skip image URLs or non-HTML content based on file extensions
    if is_image_url(url):
        print(f"Skipping image URL: {url}")
        return []

    # Extract page title for filename
    soup = BeautifulSoup(result.html, "html.parser")
    page_title = soup.find("h1").get_text(strip=True) if soup.find("h1") else "untitled"

    # Sanitize page title for use as a filename
    filename = f"{S3_PREFIX}{sanitize_filename(page_title)}.txt"

    try:
        # Upload the content to S3 as text (markdown or plain text)
        s3_client.put_object(Body=result.markdown, Bucket=S3_BUCKET, Key=filename)
        print(f"Uploaded {filename} to S3.")
    except NoCredentialsError as e:
        print(f"Credentials error: {e}")
    except Exception as e:
        print(f"Error uploading {filename} to S3: {e}")

    # Extract new links
    new_links = []
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        full_url = urljoin(BASE_URL, href)  # Convert relative to absolute URL
        if full_url.startswith(BASE_URL) and full_url not in VISITED_PAGES:
            new_links.append(full_url)

    return new_links

async def crawl_wiki():
    """Crawl the Stardew Valley Wiki starting from the homepage."""
    async with AsyncWebCrawler(verbose=True) as crawler:
        queue = [START_URL]

        while queue:
            url = queue.pop(0)
            new_links = await scrape_page(crawler, url)
            queue.extend(new_links)  # Add new links to the queue

# Run the async crawler
asyncio.run(crawl_wiki())