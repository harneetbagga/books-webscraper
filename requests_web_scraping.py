# https://colab.research.google.com/drive/1HMzK0TWH9gTIwgJf36FlcgCSI72H-bs_?usp=sharing
# https://colab.research.google.com/drive/1XnOWpjuBV-_RHNVWZDAwtcHXHQu8HCXf?usp=sharing

import logging
import random
import re
import time
import requests
from lxml import html
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import pandas as pd
import duckdb
from datetime import datetime
from time import sleep

"""Part 1: Environment Setup
Create a virtual environment and install all required libraries. Verify that you can successfully fetch
the homepage of the target website using Python.
Expected Output
- Virtual environment created
- Dependencies installed successfully
- HTTP response status code 200 logged"""

url = 'https://books.toscrape.com/'
base_url = "https://books.toscrape.com/catalogue/category/books_1/"
current_url = base_url + "page-1.html"

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                  'AppleWebKit/605.1.15 (KHTML, like Gecko) '
                  'Version/16.0 Safari/605.1.15'
}

logging.basicConfig(
    filename='scraper.log',
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

try:
    html_response = requests.get(url=url, headers=headers, timeout=10)
    html_response.raise_for_status()  # raises HTTPError for bad responses

except requests.exceptions.Timeout:
    logging.warning(f"Timeout while fetching {url}")
except requests.exceptions.HTTPError as e:
    logging.error(f"HTTP error for {url}: {e}")
except Exception as e:
    logging.error(f"Parsing error for {url}: {e}")

"""Part 2: Scrape Book Listing Pages
From the book listing pages, extract the following fields:
- Title
- Price (numeric)
- Availability
- Rating (numeric)
- Product page URL
- Category
Tasks
Identify HTML elements using browser DevTools
Extract data from one page
Convert scraped data into a Python dictionary
Append results to a list
Expected Output
- Python list of dictionaries containing book data
- Each record contains all required fields"""

#Extracting data using lmxl and xpath
html_dom = html.fromstring(html_response.text)
parent_object=html_dom.xpath('//ol/li/article')

rating_map = {
    "One": 1,
    "Two": 2,
    "Three": 3,
    "Four": 4,
    "Five": 5
}

list_details=[]

for obj in parent_object:
    title=obj.xpath("string(./h3/a/@title)")
    price=obj.xpath("string(.//p[@class='price_color']/text())").replace('Â£','')
    availability = obj.xpath("normalize-space(.//p[@class='instock availability'])")
    rating_text=obj.xpath("normalize-space(substring-after(.//p[contains(@class, 'star-rating')]/@class, 'star-rating'))")
    rating=rating_map.get(rating_text)
    product_url= url+'/'+obj.xpath("string(./h3/a/@href)")
    list_details.append({
        "title": title,
        "price": price,
        "availability": availability,
        "rating": rating,
        "url":product_url
    })

#Extracting data using BeautifulSoup
"""Part 3: Handle Pagination (Very Important)
Tasks
Detect the “Next” button
Iterate through all pages
Scrape data until no pages remain
Track page number in logs"""

"""Part 4: Scrape Detail Pages (Deep Scraping)
Additional Fields (From Book Detail Page)
product_description
UPC
number_of_reviews
product_type
tax
Tasks
Visit each book's detail page
Extract additional attributes
Merge with listing-level data"""

# Log start time
start_time = datetime.now()
logging.info("Scraping started")

soup_list_details=[]
page_number=1
total_products=0

while True:

    soup = BeautifulSoup(requests.get(current_url).text, 'html.parser')
    logging.info(f"Processing listing page {page_number}: {current_url}")

    for obj in soup.select('article.product_pod'):
        title=obj.find("h3").find("a").get("title") # type: ignore
        price=obj.find("p", class_="price_color").text # type: ignore
        availability = obj.find("p",class_='instock availability').get_text(strip=True) # type: ignore
        rating_tag=obj.find("p", class_= 'star-rating')
        if rating_tag:
            rating_class = next(
                cls for cls in rating_tag["class"] if cls !="star-rating"# type: ignore
            )
        else:
            rating_class=None
        product_url= urljoin(current_url,obj.find("h3").find("a").get("href"))# type: ignore
        #Navigate to product details page and fetch information
        product_description = ""
        product_details={}

        if product_url:
            time.sleep(random.uniform(1, 5))
            logging.info(f"Processing data from URL - {product_url}")
            total_products += 1
            detailed_soup = BeautifulSoup(requests.get(product_url).text, 'html.parser')
            obj1 = detailed_soup.select_one('article.product_page')
            if not obj1:
                logging.error(f"{product_url} is empty or could not be accessed")
            else:
                desc_tag= obj1.select_one("#product_description +p")#type: ignore
                product_description = desc_tag.text if desc_tag else ""

                info_table = obj1.select_one("table.table")#type: ignore
                if not info_table:
                    logging.error(f"No product details available on {product_url}")
                else:
                    for tr in info_table.find_all("tr"):
                        key = tr.find("th").text.strip()
                        value = tr.find("td").text.strip()
                        product_details[key] = value

            logging.info(f"Total products scraped so far: {total_products}")           

        soup_list_details.append({
            "title": title,
            "price": price,
            "availability": availability,
            "rating": rating_class,
            "url":product_url,
            "UPC":product_details.get("UPC",None),
            "number_of_reviews":product_details.get("Number of reviews",None),
            "product_type":product_details.get("Product Type",'Unknown'),
            "tax":product_details.get("Tax",None),
            "product_description":product_description
        })

    next_link = soup.select_one("li.next a")
    if not next_link:
        break

    current_url = urljoin(current_url, str(next_link["href"]))
    page_number+=1

    # Random delay between 1 and 5 seconds
    delay = random.uniform(1, 5)
    time.sleep(delay)

# Log summary at the end
end_time = datetime.now()
logging.info(f"Scraping finished. Pages: {page_number}, Records: {total_products}")
logging.info(f"Total duration: {end_time - start_time}")

"""Part 5: Data Cleaning & Validation
Tasks
Remove currency symbols from prices
Convert ratings (“Three”, “Five”) → integers
Handle missing descriptions gracefully
Validate:
Price > 0
Rating between 1–5"""

def clean_data(data_list):
    for item in data_list:
        # Remove currency symbols from price
        if item.get("price"):
            item["price"] = float(re.sub(r"[^\d.]", "", item["price"]))

        # Remove currency symbols from tax
        if item.get("tax"):
            item["tax"] = float(re.sub(r"[^\d.]", "", item["tax"]))
        
        # Convert rating text to integer
        if item.get("rating"):
            item["rating"]=rating_map.get(item["rating"],None)
        
        #Validate
        if item.get("price") is not None and item.get("price")<=0:
            logging.warning(f'Invalid price for title - {item["title"]}')
        if item.get("rating") is not None and not 1<=item.get("rating")<=5:
            logging.warning(f'Invalid rating for title - {item["title"]}')
        if not item.get("product_description"):
            item["product_description"] = "No description available"
            logging.warning(f'No product description available for title - {item["title"]}')

    return data_list

clean_data_list = clean_data(soup_list_details)

"""Part 6: Store the Data (Data Engineering Angle)
Required Outputs
CSV file
ducdb database table
Tasks
Save scraped data as:
books_raw.csv
books_cleaned.csv
Create duckdb table:
books (
  title TEXT,
  category TEXT,
  price REAL,
  rating INTEGER,
  availability TEXT,
  upc TEXT,
  description TEXT
)"""

#Converting raw data in pandas dataframe and csv
raw_df = pd.DataFrame(soup_list_details)
raw_df.to_csv('ScrapedData/books_raw.csv', index=False)

#Converting clean data in pandas dataframe and csv
clean_df = pd.DataFrame(clean_data_list)
clean_df['price']= pd.to_numeric(clean_df['price'], errors='coerce')
clean_df['tax']= pd.to_numeric(clean_df['tax'], errors='coerce')
clean_df['number_of_reviews']= pd.to_numeric(clean_df['number_of_reviews'], errors='coerce')
clean_df.to_csv('ScrapedData/books_cleaned.csv', index=False)

#Create and insert data in duckdb table
con = duckdb.connect('ScrapedData/books.duckdb')
con.execute("""
DROP TABLE IF EXISTS books
""")
con.execute("""
               CREATE TABLE books AS
               SELECT * FROM clean_df
""")
con.close()

"""Part 7: Logging, Errors & Resilience
Tasks
Log:
Start/end time
Pages scraped
Number of records
Handle:
Network timeouts
Invalid responses
Parsing errors
"""

"""Part 8: Performance & Ethics (Interview Favorite)
Tasks
Add delay between requests
Randomize sleep time
Add custom headers (User-Agent)"""