# 📚 Books to Scrape – End‑to‑End Web Scraping & Data Engineering Project

## Highlights
- End-to-end Python web scraping and data ingestion pipeline
- Full pagination handling across multi-page listings
- Deep detail-page scraping with metadata enrichment
- Data cleaning, validation, and quality checks
- Structured logging and fault-tolerant execution
- CSV and DuckDB storage for analytics-ready datasets
  
## Overview

This project demonstrates a **production‑style web scraping pipeline** built using Python. It scrapes book data from the public practice website **[https://books.toscrape.com](https://books.toscrape.com)**, performs deep pagination and detail‑page scraping, cleans and validates the data, and stores it in **CSV files and a DuckDB database**.

The project is designed as a **portfolio‑ready Data Engineering / Python Web Scraping project**, following interview‑level best practices: logging, error handling, respectful scraping, and data persistence.

---

## Key Features

* Listing‑level and detail‑page scraping
* Pagination handling across all pages
* Data cleaning and validation
* Structured logging
* CSV + DuckDB storage
* Ethical scraping practices (headers, delays, throttling)

---

## Tech Stack

* **Python 3.x**
* **requests** – HTTP requests
* **BeautifulSoup (bs4)** – HTML parsing
* **lxml** – XPath parsing
* **pandas** – Data manipulation
* **duckdb** – Analytical database
* **logging** – Execution logs

---

## Project Structure

```
Webscraper/
│
├── ScrapedData/
│   ├── books_raw.csv          # Raw scraped data
│   ├── books_cleaned.csv      # Cleaned & validated data
│   └── books.duckdb           # DuckDB database
│
├── scraper.py                 # Main scraping pipeline
├── requirements.txt           # Project dependencies
├── scraper.log                # Execution logs
├── README.md                  # Project documentation
└── .gitignore                 # Ignored files
```

---

## Data Collected

### Listing Page Fields

* Title
* Price
* Availability
* Rating
* Product URL

### Detail Page Fields

* Product Description
* UPC
* Number of Reviews
* Product Type
* Tax

---

## Pipeline Breakdown

### 1️⃣ Environment Setup

* Virtual environment creation
* Dependency installation
* Homepage connectivity check (HTTP 200)

### 2️⃣ Listing Page Scraping

* XPath‑based extraction using `lxml`
* Field normalization and structuring

### 3️⃣ Pagination Handling

* Automatic detection of "Next" button
* Iterative scraping until pages exhaust
* Page‑level logging

### 4️⃣ Detail Page Scraping

* Individual product page visits
* Metadata extraction via HTML tables
* Graceful handling of missing data

### 5️⃣ Data Cleaning & Validation

* Currency symbol removal
* Rating normalization (1–5)
* Null handling for descriptions
* Validation rules:

  * Price > 0
  * Rating ∈ [1, 5]

### 6️⃣ Data Storage

* Raw and cleaned datasets saved as CSV
* DuckDB table creation and insertion

### 7️⃣ Logging & Error Handling

* Start/end timestamps
* Page and record counts
* Network, parsing, and data warnings

### 8️⃣ Ethical Scraping

* Custom User‑Agent
* Randomized delays
* Respectful request throttling

---

## How to Run the Project

### 1. Clone the Repository

```bash
git clone https://github.com/<your-username>/books-webscraper.git
cd books-webscraper
```

### 2. Create Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate  # macOS/Linux
venv\Scripts\activate     # Windows
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Run the Scraper

```bash
python scraper.py
```

---

## Outputs

* `ScrapedData/books_raw.csv`
* `ScrapedData/books_cleaned.csv`
* `ScrapedData/books.duckdb`
* `scraper.log`

---

## Interview Discussion Topics

* requests vs urllib
* BeautifulSoup vs Scrapy
* Handling JavaScript‑heavy sites
* Anti‑blocking strategies
* Scaling scrapers
* Scheduling (Airflow / Cron)
* APIs vs scraping

---

## Disclaimer

This project scrapes a **public practice website** intended for learning purposes. No copyrighted or restricted data is used.

---

## Author

**Harneet Bagga**
Senior QA & Data Engineering Professional
