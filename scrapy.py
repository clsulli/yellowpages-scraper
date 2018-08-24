#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
from lxml import html
import database.driver as query
from collections import deque
import pickle
import warnings
import json
import math
from settings.config import Config
warnings.filterwarnings("ignore")


def go(keyword, place):
    ok, listing_amt = get_total_listings(keyword=keyword, place=place)
    if listing_amt > 5:
        listing_amt = 5
    if ok and listing_amt is not 0:
        for page in range(1, listing_amt):
            listings = parse_listing(keyword=keyword, place=place, page=page)
            print(listings)


def get_total_listings(keyword, place):
    url = "https://www.yellowpages.com/search?search_terms={0}&geo_location_terms={1}".format(keyword, place)
    headers = Config.HEADERS
    print(url)
    try:
        response = requests.get(url, verify=False, headers=headers, proxies={"http": Config.PROXY_ROTATOR, "https": Config.PROXY_ROTATOR})
        if response.status_code == 200:
            parser = html.fromstring(response.text)
            base_url = "https://www.yellowpages.com"
            parser.make_links_absolute(base_url)

            XPATH_TOTAL_LISTINGS = "//div[contains(@class, 'pagination')]//text()"

            total_listings = parser.xpath(XPATH_TOTAL_LISTINGS)
            total_listings = int(total_listings[1])
            total_pages = math.ceil(total_listings/30)
            return True, total_pages

        elif response.status_code == 404:
            print("Could not find a location matching", place)
            return True, 0
        else:
            print("Failed to process page")
            return False, 0

    except Exception as e:
        print("Failed to process page: {}".format(e))
        return False, 0


def parse_listing(keyword, place, page):
    url = "https://www.yellowpages.com/search?search_terms={0}&geo_location_terms={1}&page={2}".format(keyword, place, page)
    headers = Config.HEADERS

    try:
        response = requests.get(url, verify=False, headers=headers, proxies={"http": Config.PROXY_ROTATOR, "https": Config.PROXY_ROTATOR})
        if response.status_code == 200:
            parser = html.fromstring(response.text)
            base_url = "https://www.yellowpages.com"
            parser.make_links_absolute(base_url)

            XPATH_LISTINGS = "//div[@class='search-results organic']//div[@class='v-card']"

            listings = parser.xpath(XPATH_LISTINGS)
            scraped_results = []

            for results in listings:
                XPATH_BUSINESS_NAME = ".//a[@class='business-name']//text()"
                XPATH_BUSSINESS_PAGE = ".//a[@class='business-name']//@href"
                XPATH_TELEPHONE = ".//div[@itemprop='telephone']//text()"
                XPATH_WEBSITE = ".//div[@class='info']//div[contains(@class,'info-section')]//div[@class='links']//a[contains(@class,'website')]/@href"

                raw_business_name = results.xpath(XPATH_BUSINESS_NAME)
                raw_business_telephone = results.xpath(XPATH_TELEPHONE)
                raw_business_page = results.xpath(XPATH_BUSSINESS_PAGE)
                raw_website = results.xpath(XPATH_WEBSITE)

                business_name = ''.join(raw_business_name).strip() if raw_business_name else None
                telephone = ''.join(raw_business_telephone).strip() if raw_business_telephone else None
                business_page = ''.join(raw_business_page).strip() if raw_business_page else None
                website = ''.join(raw_website).strip() if raw_website else None

                business_details = {
                    'name': business_name,
                    'phone': telephone,
                    'website': website,
                    'detail_page': business_page
                }
                scraped_results.append(business_details)
            return True, scraped_results

        elif response.status_code == 404:
            print("Could not find a location matching", place)
            return True, []
        else:
            print("Failed to process page")
            return False, []

    except Exception as e:
        print("Failed to process page: {}".format(e))
        return False, []


def find_email(listing):
    url = listing
    headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
               'Accept-Encoding': 'gzip, deflate, br',
               'Accept-Language': 'en-GB,en;q=0.9,en-US;q=0.8,ml;q=0.7',
               'Cache-Control': 'max-age=0',
               'Connection': 'keep-alive',
               'Host': 'www.yellowpages.com',
               'Upgrade-Insecure-Requests': '1',
               'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36'
               }

    for retry in range(10):
        try:
            response = requests.get(url, verify=False, headers=headers, proxies={"http": Config.PROXY_ROTATOR, "https": Config.PROXY_ROTATOR})
            if response.status_code == 200:
                parser = html.fromstring(response.text)
                base_url = "https://www.yellowpages.com"
                parser.make_links_absolute(base_url)

                XPATH_EMAIL = "//a[contains(@href, 'mailto')]/@href"
                raw_email = parser.xpath(XPATH_EMAIL)
                email = ' '.join(raw_email).strip() if raw_email else None
                return email
        except:
            return None


def find_popular_cities(state):
    url = "https://www.yellowpages.com/state-{}".format(state)
    print("retrieving ", url)

    headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
               'Accept-Encoding': 'gzip, deflate, br',
               'Accept-Language': 'en-GB,en;q=0.9,en-US;q=0.8,ml;q=0.7',
               'Cache-Control': 'max-age=0',
               'Connection': 'keep-alive',
               'Host': 'www.yellowpages.com',
               'Upgrade-Insecure-Requests': '1',
               'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36'
               }

    for retry in range(1):
        try:
            response = requests.get(url, verify=False, headers=headers)
            print("parsing page")
            if response.status_code == 200:
                parser = html.fromstring(response.text)
                base_url = "https://www.yellowpages.com"
                parser.make_links_absolute(base_url)

                XPATH_POPULAR_CITIES = "/html/body/div/div/div/div/div[1]//text()"
                raw_cities = parser.xpath(XPATH_POPULAR_CITIES)
                popular_cities = raw_cities[1::]
                for city in popular_cities:
                    query.insert_location(city=city, state=state)

                # email = ''.join(raw_email).strip() if raw_email else None

        except Exception as e:
            print('error: {}'.format(e))


def get_categories():
    session = query.load_session()
    category_list = []
    categories = query.get_categories(session)
    for category in categories:
        category_list.append(category.category)
    session.close()
    return category_list


def get_locations():
    session = query.load_session()
    location_list = []
    locations = query.get_locations(session)
    for location in locations:
        location_str = "{}, {}".format(location.city, location.state)
        location_list.append(location_str)
    session.close()
    return location_list


def create_search_terms():
    categories = get_categories()
    locations = get_locations()

    with open('queue.txt', 'w') as f:

        for location in locations:
            for category in categories:
                f.write('{} in {}\n'.format(category, location))
        f.close()


def load_queue():
    Q = deque()
    with open('queue.txt', 'r') as f:
        queue_list = f.readlines()
        for item in queue_list:
            Q.appendleft(item.strip('\n'))
    pickle.dump(Q, open('queue.pkl', 'wb'))
    return Q


def get_proxy():
    url = 'http://falcon.proxyrotator.com:51337/'

    params = dict(
        apiKey='t4SVLNeP8w2pghMd59AXyBaKnDc3ZbEj'
    )

    resp = requests.get(url=url, params=params)
    resp = json.loads(resp.text)
    proxy = resp['proxy']
    return proxy


def load_proxy_list():
    proxy_list = pickle.load(open('proxies.pkl', 'rb'))
    proxy_list = proxy_list.split('\r\n')
    return proxy_list


def perform_search():
    q = load_queue()
    search = q.pop()
    search_sep = search.split(' in ')
    category, location = search_sep[0], search_sep[1]
    listings = parse_listing(keyword=category, place=location)
    entry = listings[1]
    session = query.load_session()
    query.insert_business_entry(session=session, name=entry['name'], phone=entry['phone'], website=entry['website'],
                                email=entry['email'], category=category, location=location)
    session.close()


def process_queue():
    q = pickle.load(open('queue.pkl', 'rb'))
    session = query.load_session()

    while q:
        search = q.pop()
        search_sep = search.split(' in ')
        category, location = search_sep[0], search_sep[1]
        listings = parse_listing(keyword=category, place=location)
        for entry in listings:
            try:
                query.insert_business_entry(session=session, name=entry['name'], phone=entry['phone'],
                                            website=entry['website'],
                                            email=entry['email'], category=category, location=location)
                with open('logs.txt', 'a') as f:
                    f.write('SUCCESS:\n    SEARCH: {}\n    BUSINESS: {}\n'.format(search, entry['name']))
                f.close()
            except Exception as e:
                with open('logs.txt', 'a') as f:
                    f.write('FAILURE:\n    SEARCH: {}\n    BUSINESS: {}\n        Exception: {}\n'.format(search,
                                                                                                         entry['name'],
                                                                                                         e))
                f.close()
        pickle.dump(q, open('queue.pkl', 'wb'))


if __name__ == "__main__":
    go('Doctors', 'Edwardsville, IL')
