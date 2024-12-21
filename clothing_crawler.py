import os
import requests
import sqlite3
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import json
import re
import yaml
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
import time
from selenium.webdriver.chrome.options import Options

# Configurations
CONFIG_FILE = "config.yaml"
DB_FILE = "products.db"
ASSETS_FOLDER = "assets"

# Load configuration
with open(CONFIG_FILE, 'r') as f:
    config = yaml.safe_load(f)

BASE_URL = config['base_url']
PRODUCTS_PER_SUBCATEGORY = config['products_per_subcategory']

# Initialize SQLite database connection
db_connection = sqlite3.connect(DB_FILE)
cursor = db_connection.cursor()

# Create categories and products tables
cursor.execute('''
    CREATE TABLE IF NOT EXISTS categories (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        main_category TEXT NOT NULL,
        sub_category TEXT NOT NULL,
        url TEXT,
        UNIQUE (main_category, sub_category)
    );
''')

# Drop the products table if it exists and recreate it with the nested_category column
cursor.execute('DROP TABLE IF EXISTS products;')
cursor.execute('''
    CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        main_category TEXT NOT NULL,
        sub_category TEXT NOT NULL,
        nested_category TEXT NOT NULL, -- Added nested_category column
        product_id TEXT NOT NULL,
        title TEXT,
        subtitle TEXT,
        price TEXT,
        old_price TEXT,
        discount TEXT,
        description TEXT,
        additional_details TEXT,
        specs TEXT,
        images TEXT,
        json_path TEXT,
        UNIQUE (main_category, sub_category, nested_category, product_id)
    );
''')

db_connection.commit()

def sanitize_filename(name):
    return re.sub(r'[^\w\-\_\u0600-\u06FF ]', '_', name)

def get_soup(url):
    response = requests.get(url)
    response.raise_for_status()
    response.encoding = 'utf-8'
    return BeautifulSoup(response.text, 'html.parser')

def save_image(url, folder, filename):
    """
    Download an image from the given URL and save it to the specified folder.
    """
    os.makedirs(folder, exist_ok=True)
    file_path = os.path.join(folder, filename)
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(file_path, 'wb') as f:
            for chunk in response.iter_content(1024):
                f.write(chunk)
        print(f"Image saved: {file_path}")
        return file_path
    except requests.exceptions.RequestException as e:
        print(f"Failed to download image: {url}. Error: {e}")
        return None



def extract_product_metadata(product_url, main_category, sub_category, nested_category, product_id):
    """
    Extract detailed metadata and images from the product page.
    """
    soup = get_soup(product_url)

    # Extract product title
    subtitle = soup.find("h3", class_="c-product-page__features-subtitle")
    product_title = subtitle.text.strip() if subtitle else "No Title Found"

    # Extract price
    price_tag = soup.find("div", class_="c-product-page__selling-price js-selling-price")
    product_price = price_tag.text.strip() if price_tag else None

    # Extract old price
    old_price_tag = soup.find("del", class_="c-product-page__rrp-price js-rrp-price")
    product_old_price = old_price_tag.text.strip() if old_price_tag else None

    # Extract discount
    discount_tag = soup.find("span", class_="js-discount-percent-value")
    product_discount = discount_tag.text.strip() if discount_tag else None

    # Extract description
    description_tag = soup.find("div", class_="c-product-page__features-description")
    product_description = description_tag.text.strip() if description_tag else None

    # Extract specifications
    specs = {}
    specs_table = soup.find("ul", class_="c-product__specs-table")
    if specs_table:
        spec_items = specs_table.find_all("li", class_="c-product__specs-table-item")
        for item in spec_items:
            key = item.find("div", class_="c-product__specs-table-item-title")
            values = item.find_all("div", class_="c-product__specs-table-value")
            if key:
                key_text = key.text.strip()
                value_texts = [value.text.strip() for value in values]
                specs[key_text] = ", ".join(value_texts)

    # Additional details
    additional_details_div = soup.find("div", class_="c-product-page__features-content")
    additional_details = additional_details_div.text.strip() if additional_details_div else None

    # Extract images
    images = []
    product_folder = os.path.join(
        ASSETS_FOLDER,
        sanitize_filename(main_category),
        sanitize_filename(sub_category),
        sanitize_filename(nested_category),
        product_id
    )
    os.makedirs(product_folder, exist_ok=True)

    image_tags = soup.find_all("img", class_="c-product-page__gallery-image")
    for idx, img_tag in enumerate(image_tags):
        img_url = img_tag.get("src")
        if img_url:
            img_url = urljoin(BASE_URL, img_url)
            img_filename = f"{product_id}_image_{idx + 1}.jpg"
            saved_path = save_image(img_url, product_folder, img_filename)
            if saved_path:
                images.append(saved_path)

    # Save metadata to JSON
    metadata = {
        "product_id": product_id,
        "main_category": main_category,
        "sub_category": sub_category,
        "nested_category": nested_category,
        "title": product_title,
        "price": product_price,
        "old_price": product_old_price,
        "discount": product_discount,
        "description": product_description,
        "specifications": specs,
        "additional_details": additional_details,
        "images": images,
    }

    json_path = os.path.join(product_folder, f"{product_id}.json")
    with open(json_path, "w", encoding="utf-8") as json_file:
        json.dump(metadata, json_file, ensure_ascii=False, indent=4)

    # Save to database
    cursor.execute('''
        INSERT OR IGNORE INTO products (
            main_category, sub_category, nested_category, product_id, title, price, old_price, discount, description, additional_details, specs, images, json_path
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        main_category, sub_category, nested_category, product_id, product_title, product_price, product_old_price,
        product_discount, product_description, additional_details, json.dumps(specs, ensure_ascii=False),
        json.dumps(images), json_path
    ))
    db_connection.commit()

def crawl_products(subcategory_url, main_category, sub_category, nested_category):
    """
    Crawl products from a nested subcategory page with infinite scrolling in headless mode.
    """
    # Set up Selenium with headless mode
    options = Options()
    options.add_argument("--headless")  # Run Chrome in headless mode
    options.add_argument("--disable-gpu")  # Disable GPU for compatibility
    options.add_argument("--no-sandbox")  # Bypass OS security model
    options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems
    options.add_argument("--window-size=1920x1080")  # Ensure content is fully loaded

    driver = webdriver.Chrome(options=options)  # Ensure you have the correct WebDriver installed
    driver.get(subcategory_url)

    crawled_count = 0
    products_seen = set()  # Keep track of product IDs to avoid duplicates
    last_height = driver.execute_script("return document.body.scrollHeight")

    while crawled_count < PRODUCTS_PER_SUBCATEGORY:
        print(f"Crawling page (scroll iteration) for {main_category} > {sub_category} > {nested_category}...")
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)  # Wait for new products to load

        # Extract product elements
        product_elements = driver.find_elements(By.CLASS_NAME, "cp-card--product-card")
        print(f"Found {len(product_elements)} products so far...")

        for product in product_elements:
            if crawled_count >= PRODUCTS_PER_SUBCATEGORY:
                break

            try:
                product_id = product.get_attribute("data-product-id")
                if product_id and product_id not in products_seen:
                    product_link = product.find_element(By.CLASS_NAME, "c-product-card__image-container").get_attribute("href")
                    products_seen.add(product_id)
                    crawled_count += 1

                    # Crawl product metadata
                    extract_product_metadata(product_link, main_category, sub_category, nested_category, product_id)
            except Exception as e:
                print(f"Error processing product: {e}")

        # Check if we've reached the bottom of the page
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            print("No more products to load.")
            break
        last_height = new_height

    print(f"Finished crawling {crawled_count} products for {nested_category}.")
    driver.quit()

def parse_categories(soup):
    """
    Parse the category structure from the website's main navigation menu.
    """
    categories = {}

    main_categories = soup.find_all("li", class_="c-header__supercat")
    for main_cat in main_categories:
        main_category_name = main_cat.find("a", class_="c-header__supercat-link").text.strip()
        categories[main_category_name] = {}

        subcategories = main_cat.find_all("li", class_="c-mega-menu__tab")
        for sub_cat in subcategories:
            sub_category_name = sub_cat.find("div", class_="c-mega-menu__tab-title").text.strip()
            categories[main_category_name][sub_category_name] = []

            sub_subcategories = sub_cat.find_all("a", class_="c-mega-menu__link")
            for sub_sub in sub_subcategories:
                sub_sub_name = sub_sub.text.strip()
                sub_sub_url = urljoin(BASE_URL, sub_sub["href"])
                categories[main_category_name][sub_category_name].append((sub_sub_name, sub_sub_url))

    return categories


def display_categories(categories):
    """
    Display the categories in a structured format.
    """
    print("Categories:")
    for main_cat, sub_cats in categories.items():
        print(f"- {main_cat}:")
        for sub_cat, sub_sub_cats in sub_cats.items():
            print(f"  - {sub_cat}:")
            for sub_sub in sub_sub_cats:
                print(f"    - {sub_sub[0]} ({sub_sub[1]})")
    print()  # Add spacing for readability


def crawl_categories():
    """Crawl categories and subcategories, display them, and ask for confirmation."""
    soup = get_soup(BASE_URL)
    categories = parse_categories(soup)

    # Display the structured categories
    display_categories(categories)

    confirm = input("Do you want to start crawling these categories? (yes/no): ").strip().lower()
    if confirm != "yes":
        print("Aborting crawler.")
        return

    # Save categories to the database and start crawling
    for main_category, sub_categories in categories.items():
        for sub_category, sub_subcategories in sub_categories.items():
            for nested_category, sub_sub_url in sub_subcategories:
                cursor.execute('''
                    INSERT OR IGNORE INTO categories (main_category, sub_category, url)
                    VALUES (?, ?, ?)
                ''', (main_category, sub_category, sub_sub_url))
                db_connection.commit()
                print(f"\nCrawling products for {main_category} > {sub_category} > {nested_category}...\n")
                crawl_products(sub_sub_url, main_category, sub_category, nested_category)


if __name__ == "__main__":
    crawl_categories()
