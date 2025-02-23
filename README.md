## Talk Outline

- Install DuckDB
- Test Drive
- DuckDB?. Why and Why Not?
- Clickstream data dumping
- SQL Queries
- Python and DuckDB
- Rill Dashboard 
- Things you should know 
- Under the Hood

## Install DuckDB

`curl https://install.duckdb.org | sh`

## Test Drive

## DuckDB?. Why and Why Not?.

### Why?.
  - Embedded DBMS, no external dependencies, No server software required, operates within host process
  - First class Python ecosystem support - e.g. No data copying, can process foreign data directly (e.g., Pandas in Python)
  - Deep integration with Python for interactive analysis
  - Runs everywhere (Linux, macOS, Windows) and CPU architectures (x86, ARM)
  - Can run on edge devices, large servers, web browsers, and mobile phones
  - Comprehensive SQL support, including window functions etc (PostgresSQL Dialect )
  - Designed for OLAP workloads (large, complex queries)
  - Columnar-vectorized execution for optimal performance
  - Efficient handling of data transformations and large-scale updates
  - ACID compliance with Multi-Version Concurrency Control (MVCC)
  - Persistent, single-file databases with support for secondary indexes
  - Allows the addition of new data types, functions, file formats, and SQL syntax
  - Integrates well with current analytics setups thanks to Parquet support, JSON, HTTP(S), S3 support implemented via extensions
  - Open-source under the MIT License

### Why Not?.
  - Extremely Large Datasets: May struggle with out-of-memory or distributed large-scale data.
  - High Concurrency: Not designed for multi-user, concurrent environments. If you're building an application with many concurrent users querying the database, you may want to look into something more robust in handling concurrency.
  - Transactional Workloads (OLTP): Better suited for OLAP, not frequent small transactional queries.
  - Full ACID Compliance: Not designed for multi-node ACID transactions. ( Single Node - Yes )
  - Real-Time Streaming: Not suitable for high-throughput, low-latency streaming data.
  - Cloud-Native Needs: Lacks automatic scaling and cloud service integration. ( Probably Motherduck cloud offering provides this, haven't used it. )
  - Advanced User Management: Lacks robust user roles and permissions features.

### DuckDB excels in the following use cases:

- Ideal for local, fast querying and analysis of large datasets in tools like Jupyter notebooks or Python environments.
- Perfect for embedding within applications where a lightweight, fast analytical database is needed.
- Batch Data Processing: Suitable for running complex queries and aggregations on large datasets without needing a full data warehouse setup.
- Ad-hoc SQL Queries: Great for quick, interactive SQL queries on files like CSVs, Parquet, or other local data formats.
- Prototyping and Experimentation: Ideal for rapid experimentation, data exploration, or small-scale data pipelines in early stages of development.
- OLAP Workloads: Optimized for Online Analytical Processing (OLAP), including aggregations, filtering, and complex analytical queries.

## Clickstream data dumping

`duckdb events.db`

Execute the below SQL on the duckdb console

```sql
CREATE TABLE events (
    event_id VARCHAR,
    event_timestamp TIMESTAMP,
    event_type VARCHAR,
    event_session_id VARCHAR,
    user_id INTEGER,
    user_anonymous_id VARCHAR,
    user_properties_account_type VARCHAR,
    user_properties_registration_date DATE,
    user_properties_last_login TIMESTAMP,
    platform_type VARCHAR,
    platform_details_app_version VARCHAR,
    platform_details_build_number VARCHAR,
    platform_details_screen_name VARCHAR,
    platform_details_view_controller VARCHAR,
    platform_details_deeplink VARCHAR,
    platform_details_app_installation_source VARCHAR,
    platform_details_url VARCHAR,
    platform_details_referrer VARCHAR,
    platform_details_page_title VARCHAR,
    platform_details_utm_source VARCHAR,
    platform_details_utm_medium VARCHAR,
    platform_details_utm_campaign VARCHAR,
    platform_details_page_path VARCHAR,
    platform_details_activity VARCHAR,
    platform_details_fragment VARCHAR,
    platform_details_navigation_path VARCHAR,
    device_type VARCHAR,
    device_brand VARCHAR,
    device_model VARCHAR,
    device_screen_width INTEGER,
    device_screen_height INTEGER,
    device_screen_orientation VARCHAR,
    device_screen_scale_factor INTEGER,
    device_os_name VARCHAR,
    device_os_version VARCHAR,
    device_browser_name VARCHAR,
    device_browser_version VARCHAR,
    device_network_connection_type VARCHAR,
    device_network_carrier VARCHAR,
    device_network_effective_connection_type VARCHAR,
    device_hardware_ram_gb INTEGER,
    device_hardware_storage_type VARCHAR,
    device_hardware_battery_level FLOAT,
    device_hardware_low_power_mode BOOLEAN,
    location_country VARCHAR,
    location_region VARCHAR,
    location_city VARCHAR,
    location_timezone TIMESTAMP,
    location_locale VARCHAR,
    location_coordinates_latitude DOUBLE,
    location_coordinates_longitude DOUBLE,
    location_coordinates_accuracy INTEGER,
    location_ip_address VARCHAR,
    interaction_element_id VARCHAR,
    interaction_element_tag VARCHAR,
    interaction_element_text VARCHAR,
    interaction_element_class VARCHAR,
    interaction_element_path VARCHAR,
    interaction_element_x_position INTEGER,
    interaction_element_y_position INTEGER,
    interaction_element_is_visible BOOLEAN,
    interaction_element_was_scrolled_to BOOLEAN,
    interaction_action_type VARCHAR,
    interaction_action_duration_ms INTEGER,
    interaction_action_force FLOAT,
    interaction_action_velocity FLOAT,
    interaction_action_direction VARCHAR,
    interaction_target_product_id VARCHAR,
    interaction_target_category VARCHAR,
    interaction_target_section VARCHAR,
    interaction_target_position INTEGER,
    interaction_target_context VARCHAR,
    performance_page_load_time_ms INTEGER,
    performance_first_contentful_paint_ms INTEGER,
    performance_time_to_interactive_ms INTEGER,
    performance_memory_usage_mb INTEGER,
    performance_cpu_utilization FLOAT,
    performance_battery_impact VARCHAR,
    performance_network_response_time_ms INTEGER,
    performance_network_download_size_bytes INTEGER,
    performance_network_latency_ms INTEGER,
    context_feature_flags_dark_mode BOOLEAN,
    context_feature_flags_beta_features BOOLEAN,
    context_feature_flags_new_product_cards BOOLEAN,
    context_cart_item_count INTEGER,
    context_cart_total_value FLOAT,
    context_search_query VARCHAR,
    context_content_category VARCHAR,
    context_previous_screen VARCHAR,
    context_view_duration_ms INTEGER,
    context_session_duration_ms INTEGER,
    context_funnel_step VARCHAR,
    context_user_flow VARCHAR,
    custom_properties_promotion_viewed VARCHAR,
    custom_properties_recommendation_source VARCHAR,
    custom_properties_content_impression_id VARCHAR,
    custom_properties_experiment_id VARCHAR
);
```

```sql
CREATE INDEX idx_user_id ON events (user_id);
CREATE INDEX idx_event_timestamp ON events (event_timestamp);
CREATE INDEX idx_event_type ON events (event_type);
CREATE INDEX idx_event_session_id ON events (event_session_id);
CREATE INDEX idx_user_anonymous_id ON events (user_anonymous_id);
CREATE INDEX idx_platform_type ON events (platform_type);
CREATE INDEX idx_location_country ON events (location_country);
CREATE INDEX idx_location_region ON events (location_region);
CREATE INDEX idx_location_city ON events (location_city);
CREATE INDEX idx_device_os_name ON events (device_os_name);
CREATE INDEX idx_device_browser_name ON events (device_browser_name);
CREATE INDEX idx_interaction_element_id ON events (interaction_element_id);
CREATE INDEX idx_interaction_target_product_id ON events (interaction_target_product_id);
CREATE INDEX idx_interaction_target_category ON events (interaction_target_category);
CREATE INDEX idx_context_search_query ON events (context_search_query);
CREATE INDEX idx_context_funnel_step ON events (context_funnel_step);
```

```python
import json
import random
import uuid
import argparse
from datetime import datetime, timedelta
from faker import Faker
from faker.providers import internet, user_agent, date_time
from tqdm import tqdm

def flatten_json(nested_json, prefix=''):
    flat_dict = {}
    def recursive_flatten(obj, parent_key=''):
        if isinstance(obj, dict):
            for k, v in obj.items():
                new_key = f"{parent_key}_{k}" if parent_key else k
                recursive_flatten(v, new_key)
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                new_key = f"{parent_key}_{i}"
                recursive_flatten(item, new_key)
        else:
            flat_dict[parent_key] = obj
    recursive_flatten(nested_json)
    return flat_dict


def generate_ecommerce_clickstream_data(num_records=10):
    fake = Faker()
    fake.add_provider(internet)
    fake.add_provider(user_agent)
    fake.add_provider(date_time)
    
    records = []
    
    product_categories = ["electronics", "clothing", "home_goods", "beauty", "books", "sports", "toys"]
    product_actions = ["view", "add_to_cart", "add_to_wishlist", "purchase", "remove_from_cart", "review"]
    page_types = ["home", "category_listing", "product_detail", "cart", "checkout", "account", "search_results"]
    
    app_screens = {
        "android": ["MainActivity", "HomeActivity", "ProductDetailActivity", "CartActivity", "CheckoutActivity", "SearchActivity"],
        "ios": ["HomeViewController", "ProductDetailViewController", "CartViewController", "CheckoutViewController", "SearchViewController"]
    }
    
    element_ids = ["add-to-cart-btn", "buy-now-btn", "product-image", "size-selector", "color-picker", 
                  "view-details", "checkout-btn", "apply-coupon-btn", "continue-shopping", "sign-in-btn"]
    
    element_tags = ["button", "a", "div", "img", "select", "input"]
    element_classes = ["btn-primary", "product-tile", "nav-item", "carousel-item", "form-control", "card-img"]
    
    user_segments = ["new_visitor", "returning_customer", "high_value", "price_sensitive", "deal_seeker", 
                     "cart_abandoner", "frequent_browser", "mobile_first", "desktop_preferred"]
    
    device_types = {
        "web": ["desktop", "tablet", "smartphone"],
        "android": ["smartphone", "tablet"],
        "ios": ["iPhone", "iPad"]
    }
    
    user_registration_dates = {}
    
    for _ in tqdm(range(num_records), desc="Generating records", unit="record"):
        platform_type = random.choice(["web", "android", "ios"])
        
        user_id = random.randint(1, num_records)
        if user_id in user_registration_dates:
            registration_date = user_registration_dates[user_id]
        else:
            registration_date = fake.date_between(start_date='-2y', end_date='-1d')
            user_registration_dates[user_id] = registration_date
        
        anon_id = f"anon_{uuid.uuid4().hex[:8]}"
        
        device_type = random.choice(device_types[platform_type])
        
        event_timestamp = fake.date_time_between(start_date='-1d', end_date='now').isoformat() + 'Z'
        last_login = fake.date_time_between(start_date='-12h', end_date='now').isoformat() + 'Z'
        
        session_id = f"sess_{uuid.uuid4().hex[:12]}"
        session_duration = random.randint(60000, 1800000)
        
        page_load_time = random.randint(200, 5000)
        first_contentful_paint = random.randint(100, 1000)
        time_to_interactive = page_load_time + random.randint(100, 800)
        
        category = random.choice(product_categories)
        product_id = f"p_{random.randint(10000, 99999)}"
        action_type = random.choice(product_actions)
        
        if device_type == "desktop":
            os_name = random.choice(["Windows", "macOS", "Linux"])
            if os_name == "Windows":
                os_version = random.choice(["10", "11"])
            elif os_name == "macOS":
                os_version = random.choice(["10.15", "11.0", "12.0", "13.0"])
            else:
                os_version = random.choice(["Ubuntu 20.04", "Fedora 35", "Debian 11"])
            
            browser = random.choice(["Chrome", "Firefox", "Safari", "Edge"])
            browser_version = f"{random.randint(70, 110)}.{random.randint(0, 9)}"
            
            screen_width = random.choice([1366, 1440, 1920, 2560, 3440])
            screen_height = random.choice([768, 900, 1080, 1440, 2160])
            brand = random.choice(["Dell", "HP", "Apple", "Lenovo", "ASUS", "Custom Build"])
            model = f"{brand} {random.choice(['Inspiron', 'XPS', 'Pavilion', 'ThinkPad', 'MacBook Pro', 'iMac'])}"
            
        elif device_type == "smartphone":
            if platform_type == "ios":
                os_name = "iOS"
                os_version = f"{random.randint(14, 18)}.{random.randint(0, 5)}.{random.randint(0, 2)}"
                brand = "Apple"
                model = f"iPhone {random.choice(['11', '12', '13', '14', '15'])} {random.choice(['', 'Pro', 'Pro Max'])}"
                screen_width = random.choice([1170, 1284, 1125])
                screen_height = random.choice([2532, 2778, 2436])
            else:
                os_name = "Android"
                os_version = f"{random.randint(9, 14)}.{random.randint(0, 5)}"
                brand = random.choice(["Samsung", "Google", "OnePlus", "Xiaomi", "Motorola", "Sony"])
                model = f"{brand} {random.choice(['Galaxy S23', 'Pixel 7', 'OnePlus 10', 'Redmi Note 11', 'Edge 30'])}"
                screen_width = random.choice([1080, 1440, 1600])
                screen_height = random.choice([1920, 2400, 3200])
            
            browser = random.choice(["Chrome", "Safari", "Samsung Internet"])
            browser_version = f"{random.randint(70, 110)}.{random.randint(0, 9)}"
        
        else:
            if platform_type == "ios":
                os_name = "iOS"
                os_version = f"{random.randint(15, 17)}.{random.randint(0, 5)}"
                brand = "Apple"
                model = f"iPad {random.choice(['Air', 'Pro', 'Mini'])} {random.randint(4, 6)}"
                screen_width = random.choice([1620, 1668, 2048])
                screen_height = random.choice([2160, 2224, 2732])
            else:
                os_name = "Android"
                os_version = f"{random.randint(10, 13)}.{random.randint(0, 5)}"
                brand = random.choice(["Samsung", "Lenovo", "Huawei", "Amazon"])
                model = f"{brand} {random.choice(['Galaxy Tab S8', 'Tab M10', 'MatePad Pro', 'Fire HD 10'])}"
                screen_width = random.choice([1200, 1600, 2000])
                screen_height = random.choice([1920, 2560, 3000])
            
            browser = random.choice(["Chrome", "Safari", "Firefox"])
            browser_version = f"{random.randint(70, 110)}.{random.randint(0, 9)}"
        
        record = {
            "event": {
                "id": f"ev_{uuid.uuid4().hex[:12]}",
                "timestamp": event_timestamp,
                "type": random.choice(["click", "scroll", "view", "pageview", "app_open", "add_to_cart"]),
                "session_id": session_id
            },
            "user": {
                "id": user_id,
                "anonymous_id": anon_id,
                "properties": {
                    "account_type": random.choice(["guest", "registered", "premium"]),
                    "registration_date": registration_date.strftime("%Y-%m-%d"),
                    "last_login": last_login,
                }
            },
            "platform": {
                "type": platform_type,
                "details": {}
            },
            "device": {
                "type": device_type,
                "brand": brand,
                "model": model,
                "screen": {
                    "width": screen_width,
                    "height": screen_height,
                    "orientation": random.choice(["portrait", "landscape"]),
                    "scale_factor": random.choice([1, 2, 3])
                },
                "os": {
                    "name": os_name,
                    "version": os_version
                },
                "browser": {
                    "name": browser,
                    "version": browser_version
                },
                "network": {
                    "connection_type": random.choice(["wifi", "cellular", "ethernet"]),
                    "carrier": random.choice(["Verizon", "AT&T", "T-Mobile", "Vodafone", "Orange", None]),
                    "effective_connection_type": random.choice(["3g", "4g", "5g", "wifi"])
                },
                "hardware": {
                    "ram_gb": random.choice([4, 8, 16, 32]),
                    "storage_type": random.choice(["flash", "ssd", "hdd"]),
                    "battery_level": round(random.uniform(0.1, 1.0), 2),
                    "low_power_mode": random.choice([True, False])
                }
            },
            "location": {
                "country": fake.country_code(),
                "region": fake.state_abbr(),
                "city": fake.city(),
                "timezone": str(fake.date_time_this_year()),
                "locale": f"{fake.language_code()}-{fake.country_code()}",
                "coordinates": {
                    "latitude": float(fake.latitude()),
                    "longitude": float(fake.longitude()),
                    "accuracy": random.randint(5, 50)
                },
                "ip_address": fake.ipv4()
            },
            "interaction": {
                "element": {
                    "id": random.choice(element_ids),
                    "tag": random.choice(element_tags),
                    "text": random.choice(["Add to Cart", "Buy Now", "View Details", "Apply Coupon", "Continue"]),
                    "class": random.choice(element_classes),
                    "path": f"div#product-container > div.actions > {random.choice(element_tags)}.{random.choice(element_classes)}",
                    "x_position": random.randint(10, 1000),
                    "y_position": random.randint(10, 2000),
                    "is_visible": random.choice([True, False, True, True]),
                    "was_scrolled_to": random.choice([True, False])
                },
                "action": {
                    "type": random.choice(["tap", "click", "scroll", "swipe"]),
                    "duration_ms": random.randint(50, 500),
                    "force": round(random.uniform(0.1, 1.0), 2),
                    "velocity": round(random.uniform(0.1, 1.0), 2),
                    "direction": random.choice(["vertical", "horizontal", None])
                },
                "target": {
                    "product_id": product_id,
                    "category": category,
                    "section": random.choice(["featured", "recommended", "recently_viewed", "bestsellers", "on_sale"]),
                    "position": random.randint(1, 24),
                    "context": random.choice(page_types)
                }
            },
            "performance": {
                "page_load_time_ms": page_load_time,
                "first_contentful_paint_ms": first_contentful_paint,
                "time_to_interactive_ms": time_to_interactive,
                "memory_usage_mb": random.randint(50, 500),
                "cpu_utilization": round(random.uniform(0.05, 0.9), 2),
                "battery_impact": random.choice(["low", "medium", "high"]),
                "network": {
                    "response_time_ms": random.randint(50, 1000),
                    "download_size_bytes": random.randint(10000, 5000000),
                    "latency_ms": random.randint(20, 500)
                }
            },
            "context": {
                "feature_flags": {
                    "dark_mode": random.choice([True, False]),
                    "beta_features": random.choice([True, False, False]),
                    "new_product_cards": random.choice([True, False])
                },
                "cart": {
                    "item_count": random.randint(0, 10),
                    "total_value": round(random.uniform(0, 499.99), 2)
                },
                "search_query": random.choice([None, None, "shoes", "t-shirt", "phone case", "headphones"]),
                "content_category": category,
                "previous_screen": "CategoryListView" if platform_type != "web" else "category_page",
                "view_duration_ms": random.randint(5000, 180000),
                "session_duration_ms": session_duration,
                "funnel_step": random.choice(["browse", "product_view", "add_to_cart", "checkout", "purchase_complete"]),
                "user_flow": random.choice(["direct_navigation", "search_to_purchase", "category_browse", "recommendation_click"])
            },
            "custom_properties": {
                "promotion_viewed": random.choice([None, "spring_sale", "holiday_special", "flash_deal"]),
                "recommendation_source": random.choice(["collaborative_filtering", "recently_viewed", "trending", "bestsellers"]),
                "content_impression_id": f"imp_{uuid.uuid4().hex[:6]}",
                "experiment_id": f"exp_{uuid.uuid4().hex[:6]}"
            }
        }
        
        if platform_type == "web":
            record["platform"]["details"] = {
                "app_version": None,
                "build_number": str(random.randint(100, 999)),
                "screen_name": None,
                "activity": None,
                "fragment": None,
                "deeplink": None,
                "app_installation_source": None,
                "view_controller": None,
                "navigation_path": None,
                "url": f"https://mystore.com/{random.choice(['', 'category/', 'product/', 'checkout/'])}{fake.slug()}",
                "referrer": random.choice([
                    "https://google.com/search?q=best+online+shopping",
                    "https://facebook.com/",
                    "https://instagram.com/stories/",
                    None,
                    "https://mystore.com/category/electronics"
                ]),
                "page_title": f"{fake.word().capitalize()} {random.choice(['Store', 'Shop', 'Outlet'])} - {random.choice(['Best Deals', 'Free Shipping', 'Shop Now'])}",
                "page_path": f"/{random.choice(['', 'products/', 'categories/', 'cart/', 'account/'])}",
                "utm_source": random.choice([None, "email", "social", "search", "affiliate"]),
                "utm_medium": random.choice([None, "newsletter", "banner", "cpc", "organic"]),
                "utm_campaign": random.choice([None, "spring_sale_2025", "new_arrivals", "clearance"])
            }
        elif platform_type == "android":
            app_version = f"{random.randint(1, 5)}.{random.randint(0, 9)}.{random.randint(0, 9)}"
            screen = random.choice(app_screens["android"])
            record["platform"]["details"] = {
                "url":None,
                "referrer":None,
                "page_title": None,
                "page_path": None,
                "utm_source": None,
                "utm_medium": None,
                "utm_campaign": None,
                "app_version": app_version,
                "build_number": str(random.randint(100, 999)),
                "screen_name": screen,
                "activity": f"com.example.store.{screen}",
                "fragment": f"{screen.replace('Activity', 'Fragment')}",
                "deeplink": random.choice([None, f"examplestore://product/{product_id}", "examplestore://cart"]),
                "app_installation_source": "play_store",
                "view_controller":screen,
                "navigation_path": random.sample(app_screens["ios"], k=random.randint(1, 3))[0],
            }
        else:
            app_version = f"{random.randint(1, 5)}.{random.randint(0, 9)}.{random.randint(0, 9)}"
            screen = random.choice(app_screens["ios"])
            record["platform"]["details"] = {
                "url":None,
                "referrer":None,
                "page_title": None,
                "page_path": None,
                "utm_source": None,
                "utm_medium": None,
                "utm_campaign": None,
                "app_version": app_version,
                "build_number": str(random.randint(100, 999)),
                "screen_name": screen.replace("ViewController", "View"),
                "activity": f"com.example.store.{screen}",
                "fragment": f"{screen.replace('Activity', 'Fragment')}",
                "view_controller": screen,
                "navigation_path": random.sample(app_screens["ios"], k=random.randint(1, 3))[0],
                "deeplink": random.choice([None, f"examplestore://product/{product_id}", "examplestore://cart"]),
                "app_installation_source": "app_store"
            }
        record = flatten_json(record)   
        records.append(record)
    
    return records

def main():
    parser = argparse.ArgumentParser(description='Generate synthetic e-commerce clickstream data')
    parser.add_argument('-n', '--num-records', type=int, default=10,
                        help='Number of clickstream records to generate (default: 10)')
    parser.add_argument('-o', '--output', type=str, default='ecommerce_clickstream_data.json',
                        help='Output file name (default: ecommerce_clickstream_data.json)')
    
    args = parser.parse_args()
    
    print(f"Generating {args.num_records} e-commerce clickstream records...")
    data = generate_ecommerce_clickstream_data(args.num_records)
    
    print(f"Saving data to '{args.output}'...")
    with open(args.output, 'w') as f:
        for record in data:
            f.write(json.dumps(record))
            f.write("\n")
    
    print(f"Generated {len(data)} clickstream records and saved to '{args.output}'")

if __name__ == "__main__":
    main()
```

Save above file as `generate_synthetic_ecommerce_data.py` and execute it.

`python generate_synthetic_ecommerce_data.py -n 10000 -o events.json`

`COPY events FROM 'ecommerce_clickstream_data.json' (FORMAT JSON);`

### SQL Refresher

`https://duckdb.org/docs/sql/data_types/overview`

- 1. Total Events Recorded

`SELECT COUNT(*) AS total_events FROM events;`

- 2. Unique Users

`SELECT COUNT(DISTINCT user_id) AS unique_users FROM events;`

- 3. Active Users (Users with events in the last 7 days)

`SELECT COUNT(DISTINCT user_id) AS active_users FROM events  WHERE event_timestamp >= NOW() - INTERVAL '7 days';`

- 4. New Users (Users whose registration date is within last 7 days)

`SELECT COUNT(DISTINCT user_id) AS new_users FROM events  WHERE user_properties_registration_date >= NOW() - INTERVAL '7 days';`
- 5. Avg. Session Duration

`SELECT AVG(context_session_duration_ms) / 1000 AS avg_session_duration_sec FROM events;`

- 6. Page Views per Session

`SELECT AVG(event_count) AS avg_page_views_per_session FROM (SELECT event_session_id, COUNT(*) AS event_count FROM events GROUP BY event_session_id);`

- 7. Top N Common Event Types

`SELECT event_type, COUNT(*) AS event_count FROM events GROUP BY event_type ORDER BY event_count DESC LIMIT 10;`

- 8. Top 5 Clicked Elements

`SELECT interaction_element_id, COUNT(*) AS click_count FROM events WHERE event_type = 'click' GROUP BY interaction_element_id ORDER BY click_count DESC LIMIT 5;`

- 9. Device Distribution

`SELECT device_type, COUNT(*) AS device_count FROM events GROUP BY device_type;`

- 10. App vs. Web Users

`SELECT platform_type, COUNT(DISTINCT user_id) AS user_count FROM events GROUP BY platform_type;`

- 11. Most Popular App Version

`SELECT platform_details_app_version, COUNT(*) AS version_count FROM events  WHERE platform_details_app_version IS NOT NULL AND platform_details_app_version <> '' GROUP BY platform_details_app_version ORDER BY version_count DESC LIMIT 1;`

- 12. Avg. Time Spent on Screen

`SELECT platform_details_screen_name, AVG(context_view_duration_ms) / 1000 AS avg_time_sec FROM events GROUP BY platform_details_screen_name ORDER BY avg_time_sec DESC;`

- 13. Top Referrers

`SELECT platform_details_referrer, COUNT(*) AS referrer_count FROM events GROUP BY platform_details_referrer ORDER BY referrer_count DESC LIMIT 5;`

- 14. Users by Country

`SELECT location_country, COUNT(DISTINCT user_id) AS user_count FROM events GROUP BY location_country ORDER BY user_count DESC;`

- 15. Most Common Screen Resolutions

`SELECT device_screen_width, device_screen_height, COUNT(*) AS resolution_count FROM events GROUP BY device_screen_width, device_screen_height ORDER BY resolution_count DESC LIMIT 5;`

- 16. Avg. Page Load Time

`SELECT AVG(performance_page_load_time_ms) AS avg_page_load_time_ms FROM events;`

- 17. Avg. Time to First Contentful Paint

`SELECT AVG(performance_first_contentful_paint_ms) AS avg_fcp_time_ms FROM events;`

- 18. Avg. Time to Interactive

`SELECT AVG(performance_time_to_interactive_ms) AS avg_tti_ms FROM events;`

- 19. Network Response Time by Connection Type

`SELECT device_network_effective_connection_type, AVG(performance_network_response_time_ms) AS avg_response_time_ms FROM events GROUP BY device_network_effective_connection_type;`

- 20. Avg. Cart Value

`SELECT AVG(context_cart_total_value) AS avg_cart_value FROM events;`

- 21. Avg. Items in Cart

`SELECT AVG(context_cart_item_count) AS avg_cart_items FROM events;`

- 22. Search Query Popularity

`SELECT context_search_query, COUNT(*) AS search_count FROM events  WHERE context_search_query IS NOT NULL GROUP BY context_search_query ORDER BY search_count DESC LIMIT 5;`

- 23. UTM Campaign Effectiveness

`SELECT platform_details_utm_campaign, COUNT(*) AS campaign_count FROM events  WHERE platform_details_utm_campaign IS NOT NULL GROUP BY platform_details_utm_campaign ORDER BY campaign_count DESC;`

- 24. Dark Mode Adoption

`SELECT COUNT(*) * 100.0 / (SELECT COUNT(*) FROM events) AS dark_mode_percentage FROM events WHERE context_feature_flags_dark_mode = TRUE;`

- 25. Conversion Rate (Users who performed a purchase event)

`SELECT COUNT(DISTINCT user_id) * 100.0 / COUNT(DISTINCT event_session_id) AS conversion_rate FROM events WHERE event_type = 'purchase';`

- 26. Most Common Funnel Step Drop-off

`SELECT context_funnel_step, COUNT(*) AS drop_off_count FROM events GROUP BY context_funnel_step ORDER BY drop_off_count DESC LIMIT 1;`

- 27. Most Common Experiment ID

`SELECT custom_properties_experiment_id, COUNT(*) AS experiment_count FROM events WHERE custom_properties_experiment_id IS NOT NULL GROUP BY custom_properties_experiment_id ORDER BY experiment_count DESC LIMIT 1;`

- 28. Avg. User Flow Duration

`SELECT context_user_flow, AVG(context_view_duration_ms) / 1000 AS avg_duration_sec FROM events GROUP BY context_user_flow ORDER BY avg_duration_sec DESC;`

- 29. Rolling Active Users Over Last 30 Days

`SELECT event_timestamp::DATE, COUNT(DISTINCT user_id) OVER (ORDER BY event_timestamp::DATE ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS rolling_active_users FROM events;`

- 30. Session Retention Rate (Users Returning in 7 Days)

`SELECT user_id, COUNT(*) AS session_count FROM (SELECT user_id, event_session_id, MIN(event_timestamp) OVER (PARTITION BY user_id ORDER BY event_timestamp) AS first_session FROM events) GROUP BY user_id HAVING session_count > 1;`

- 31. Peak Event Hours (Hourly Distribution)

`SELECT EXTRACT(HOUR FROM event_timestamp) AS event_hour, COUNT(*) AS event_count FROM events GROUP BY event_hour ORDER BY event_count DESC;`

- 32. Average Scroll Depth per Session

`SELECT event_session_id, AVG(interaction_element_y_position) OVER (PARTITION BY event_session_id) AS avg_scroll_depth FROM events WHERE interaction_element_is_visible = TRUE;`

- 33. Top Performing UTM Campaigns (Last 30 Days)

`SELECT platform_details_utm_campaign, COUNT(*) AS campaign_count FROM events WHERE event_timestamp >= NOW() - INTERVAL '30 days' GROUP BY platform_details_utm_campaign ORDER BY campaign_count DESC LIMIT 5;`

- 34. Cumulative Event Count Over Time

`SELECT event_timestamp::DATE, COUNT(*) OVER (ORDER BY event_timestamp::DATE ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_event_count FROM events;`

## Python and DuckDB

`https://duckdb.org/docs/clients/python/overview.html`

## RillData - Dashboard on DuckDB

Rill is the fastest path from data lake to dashboard. Unlike most BI tools, Rill comes with its own embedded in-memory database ( DuckDB ). 
Data and compute are co-located, and queries return in milliseconds.

`curl https://rill.sh | sh` 

https://github.com/rilldata/rill

## Things you should know 

### Toy example for micro batching with redis and writing into duckdb

#### Producer
 
```python
import json
import random
import time
import redis

redis_client = redis.Redis(host='localhost', port=6379, db=0)

STREAM_NAME = "event_stream"

def read_events():
    f = open("events.json", "r")
    events = []
    for line in f:
        events.append(json.loads(line))
    chunk_size = random.randint(1000, 2000)
    sampled_events = random.sample(events, min(chunk_size, len(events)))
    return sampled_events

def send_events_to_stream(events):
    for event in events:
        redis_client.xadd(STREAM_NAME, {"event": json.dumps(event)})

def main():
    while True:
        events = read_events()
        send_events_to_stream(events)
        print(f"Produced {len(events)} events. Sleeping for 5 minutes...")
        time.sleep(300)

if __name__ == "__main__":
    main()
```

#### Consumer

```python
import json
import redis
import duckdb

# Redis connection
redis_client = redis.Redis(host='localhost', port=6379, db=0)

STREAM_NAME = "event_stream"
CONSUMER_GROUP = "event_group"
CONSUMER_NAME = "event_consumer"

# Initialize DuckDB connection
db_connection = duckdb.connect("event_stream.db")

# Create table if not exists
db_connection.execute("""
CREATE TABLE IF NOT EXISTS events (
    event_data JSON
)
""")

def consume_events():
    """Reads 1000 events from Redis stream and writes to DuckDB"""
    while True:
        response = redis_client.xread({STREAM_NAME: "0"}, count=1000, block=0)

        if response:
            _, events = response[0]  # Extract events from response
            batch = [(json.loads(event[1][b'event']),) for event in events]
            
            # Insert batch into DuckDB
            db_connection.executemany("INSERT INTO events (event_data) VALUES (?)", batch)
            
            # Acknowledge processed messages (deleting for simplicity)
            for event in events:
                redis_client.xdel(STREAM_NAME, event[0])

            print(f"Inserted {len(batch)} events into DuckDB.")

if __name__ == "__main__":
    consume_events()
```

### Cookbook style snippets
https://duckdb.org/docs/guides/overview

## Under the Hood
DuckDB's speed comes from several key architectural decisions: 

- OLAP Core Concepts - Images [https://www.erp-information.com/online-analytical-processing.html]

- Vectorized Query Execution DuckDB processes data in batches (vectors) rather than row by row, which makes better use of modern CPU features like SIMD instructions and cache lines. This means operations like filtering and aggregation can be performed on multiple values simultaneously. [https://15721.courses.cs.cmu.edu/spring2024/notes/06-vectorization.pdf]

- Column-Oriented Storage Unlike traditional row-oriented databases like SQLite, DuckDB stores data by column rather than by row. This is crucial for analytical queries because: 
  - Only relevant columns need to be read from disk 
  - Data compression is more effective since similar values are stored together 
  - Better CPU cache utilization when scanning columns 
  [https://pbs.twimg.com/media/GC0Ir4naAAAICpb?format=jpg&name=4096x4096]

- In-Memory Processing DuckDB is designed to work primarily in memory, minimizing disk I/O. While it can handle data larger than available RAM through smart buffering, its performance really shines when working with data that fits in memory. 

- Optimized Query Planning DuckDB includes modern query optimization techniques like: 
  - Adaptive filtering (pushing down predicates) [https://airbyte.com/data-engineering-resources/predicate-pushdown]
  - Parallel query execution 
  - Intelligent join ordering
  - Advanced statistics usage for better plan selection 

- Zero-Copy Data Sharing When working with tools like Pandas or R, DuckDB can often operate directly on the data without making copies, reducing memory usage and improving performance.
