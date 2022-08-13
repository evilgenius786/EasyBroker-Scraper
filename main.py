# -*- coding: utf8 -*-
import csv
import os.path
import threading
import time
import traceback
from datetime import datetime
from urllib.parse import urlparse

import openpyxl
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

import json

timeout = 10

debug = True
headless = False
images = False
maximize = False
incognito = False

if os.path.isfile('proxy.txt'):
    with open('proxy.txt', 'r') as pfile:
        p = pfile.read()
        proxies = {'http': p, 'https': p}
else:
    proxies = {}
test = False
lock = threading.Lock()
thread_count = 10
semaphore = threading.Semaphore(thread_count)
encoding = 'utf8'

fieldnames = ["ID", "fecha_publicacion", "nombre", "precio", "recamaras", "banos", "cant_estacionamiento",
              "m2_construccion", "m2_terreno", "largo", "ancho", "antiguedad", "pisos_numero_piso", "penthouse",
              "planta_Baja", "unaplanta", "amueblado", "cocina", "cocina_equipada", "cocina_integral", "mtto",
              "direccion", "coordenadas", "descripcion", "estacionamiento_techado", "facilidad_estacionamiento",
              "garaje", "balcon", "bodega", "terraza", "jardin", "parrilla", "patio", "alberca", "area_juegos_infantil",
              "cancha_tenis", "gimnasio", "salon_usos_multiples", "adultos", "discapacitados", "alarma", "hvac",
              "calefacción", "circuito_cerrado", "portero", "seguridad_24_hora", "fraccionamiento", "mascotas",
              "foto1", "foto2", "foto3", "foto4", "foto5", "foto6", "foto7", "foto8", "foto9", "foto10", "foto11",
              "foto12", "foto13", "foto14", "foto15", "foto16", "foto17", "foto18", "foto19", "foto20",
              "publicacion_url"]


def getListings():
    driver = getChromeDriver()
    url = getElement(driver, '//a[@rel="next"]').get_attribute('href')
    start_page = "1"
    if os.path.isfile('last_page.txt'):
        with open('last_page.txt', 'r') as f:
            start_page = f.read()
        print(f'Resuming from page {start_page}')
    driver.get(url.replace('page=2', start_page))
    info = getElement(driver, '//div[@class="py-2"]').text
    print(info)
    total = int(info.split()[-2])
    page_count = int(total / 18) + 1
    print(f"Properties count: {total}")
    print(f"Page count: {page_count}")
    for i in range(int(start_page), page_count):
        print(f"Working on page {i}")
        driver.get(url.replace('page=2', str(i)))
        print(getElement(driver, '//div[@class="py-2"]').text)
        urls = [a.get_attribute('href') for a in getElements(driver, '//a[@class="d-block text-truncate"]')]
        with open('last_page.txt', 'w') as f:
            f.write(str(i))
        with open('urls.txt', 'a') as f:
            f.write("\n".join(urls) + "\n")
    print("Removing duplicates..")
    with open('urls.txt', 'r') as f:
        all_urls = f.read().splitlines()
    with open('urls.txt', 'w') as f:
        f.write("\n".join(set(all_urls)))


def getDetails(url):
    with semaphore:
        try:
            print(f"Working on {url}")
            if os.path.isfile('index.html') and test:
                with open('index.html', 'r', encoding=encoding) as f:
                    soup = BeautifulSoup(f.read(), 'lxml')
            else:
                soup = getSoup(url)
                with open('index.html', 'w', encoding=encoding) as f:
                    f.write(soup.prettify())
            if "Lo sentimos, la propiedad que buscas ya no se encuentra disponible." in soup.text:
                print(f"Property not available {url}")
                with open('NotAvailable.txt', 'a') as f:
                    f.write(url + "\n")
                return
            # print(soup)
            data = {
                "URL": url,
                "Price": soup.find('h3', {"class": "price"}).text.split()[0] if soup.find('h3',
                                                                                          {"class": "price"}) else "",
                "Description": soup.find('div', {"class": "description-text"}).text.strip() if soup.find('div', {
                    "class": "description"}) else "",
                "Title": soup.find('h5', {"class": "property-title"}).text.strip() if soup.find('h5', {
                    "class": "property-title"}) else "",
                "Characteristics": [li.text.strip() for li in
                                    soup.find('h6', string="Características").parent.find_all('li')] if soup.find_all(
                    'h6',
                    string="Características") else [],
                'Images': [img.get('src') for img in soup.find_all('img', {'class': 'rsImg'})],
                "location": soup.find('div', {"class": "icon-content py-2 location"}).text.strip() if soup.find('div', {
                    "class": "icon-content py-2 location"}) else "",
                "Coordinates": soup.find('a', {"class": "btn-link"}).get('href', "") if soup.find('a', {
                    "class": "btn-link"}) else ""
            }
            features = soup.find('div', {"class": "main-features"})
            # if not features:
            #     with open('index.html', 'w', encoding=encoding) as f:
            #         f.write(soup.prettify())
            #     input("No features found. Press Enter to continue..")
            #     raise Exception("Features not found")
            for div in features.find_all('div'):
                txt = div.text.strip()
                if ":" in txt:
                    key, value = txt.split(":", 1)
                    data[key.strip()] = value.strip()
                else:
                    tmp = txt.split(' ')
                    data[tmp[-1]] = " ".join(tmp[:-1])
            print(json.dumps(data, indent=4, ensure_ascii=False))
            filename = os.path.basename(urlparse(url).path)
            with open(f"./json/{filename}.json", 'a') as f:
                f.write(json.dumps(data, indent=4) + "\n")
            processSoup(data)
        except:
            traceback.print_exc()
            print(f"Error on {url}")
            with open('errors.txt', 'a') as f:
                f.write(f"{url}\n")


def processSoup(data):
    row = {}
    with open('translate.json', 'r', encoding=encoding) as f:
        translate = json.load(f)
        for key in translate.keys():
            if key in data.keys() and type(data[key]) is not list:
                row[translate[key]] = data[key]
    with open('features.json', encoding=encoding) as f:
        features = json.load(f)
        for key in features.keys():
            if key in data['Characteristics']:
                row[features[key]] = key
    for i, img in enumerate(data['Images']):
        if i < 20:
            row[f"foto{i + 1}"] = img
    print(json.dumps(row, indent=4, ensure_ascii=False))
    append(row)


def convert(filename):
    wb = openpyxl.Workbook()
    ws = wb.active
    with open(filename, encoding=encoding) as f:
        reader = csv.reader(f, delimiter=',')
        for row in reader:
            ws.append(row)
    wb.save(filename.replace(".csv", ".xlsx"))


def append(data):
    with lock:
        with open('EasyBroker.csv', 'a', encoding=encoding, newline='') as f:
            csv.DictWriter(f, fieldnames=fieldnames).writerow(data)


def main():
    logo()
    if test:
        choice = "2"
    else:
        choice = input("1. Get listings\n2. Get details\n3. Convert CSV to XLSX\n4. Exit\n")
    # choice = "2"
    if choice == "1":
        getListings()
    elif choice == "2":
        if not os.path.isdir('json'):
            os.mkdir('json')
        scraped = []
        notavl = []
        if not os.path.isfile('EasyBroker.csv'):
            with open('EasyBroker.csv', 'w', encoding=encoding, newline='') as f:
                csv.DictWriter(f, fieldnames=fieldnames).writeheader()
        else:
            with open('EasyBroker.csv', encoding=encoding, newline='') as f:
                for line in csv.DictReader(f):
                    scraped.append(line['publicacion_url'])
            print(f"Already scraped URLs {scraped}")
        if os.path.isfile('NotAvailable.txt'):
            with open('NotAvailable.txt', encoding=encoding) as f:
                for line in f:
                    notavl.append(line.strip())
            print(f"Not available URLs {notavl}")
        threads = []
        with open('urls.txt', 'r') as f:
            urls = f.read().splitlines()
        for url in set(urls):
            if url in notavl:
                print(f"Not available {url}")
            elif url in scraped:
                print(f"Already scraped {url}")
            else:
                t = threading.Thread(target=getDetails, args=(url,))
                t.start()
                if thread_count == 1:
                    t.join()
                threads.append(t)
                time.sleep(0.1)
                if test:
                    break
        for thread in threads:
            thread.join()
        convert('EasyBroker.csv')
    elif choice == "3":
        convert("EasyBroker.csv")
    elif choice == "4":
        exit()


def pprint(msg):
    try:
        print(f"{datetime.now()}".split(".")[0], msg)
    except:
        traceback.print_exc()


def getSoup(url, driver=None):
    if driver is not None:
        driver.get(url)
        time.sleep(1)
        content = driver.page_source
    else:
        # if test:
        #     print(requests.get('http://lumtest.com/myip.json', proxies=proxies).text)
        content = requests.get(url, headers={'user-agent': 'Mozilla/5.0'}, proxies=proxies).content
    return BeautifulSoup(content, 'lxml')


def click(driver, xpath, js=False):
    if js:
        driver.execute_script("arguments[0].click();", getElement(driver, xpath))
    else:
        WebDriverWait(driver, timeout).until(EC.element_to_be_clickable((By.XPATH, xpath))).click()


def getElement(driver, xpath):
    return WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.XPATH, xpath)))


def getElements(driver, xpath):
    return WebDriverWait(driver, timeout).until(EC.presence_of_all_elements_located((By.XPATH, xpath)))


def sendkeys(driver, xpath, keys, js=False):
    if js:
        driver.execute_script(f"arguments[0].value='{keys}';", getElement(driver, xpath))
    else:
        getElement(driver, xpath).send_keys(keys)


def getChromeDriver(proxy=None):
    options = webdriver.ChromeOptions()
    if debug:
        # print("Connecting existing Chrome for debugging...")
        options.debugger_address = "127.0.0.1:9222"
    else:
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("excludeSwitches", ["enable-logging"])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument("--disable-blink-features")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument('--user-data-dir=C:/easybroker/ChromeProfile')
    if not images:
        # print("Turning off images to save bandwidth")
        options.add_argument("--blink-settings=imagesEnabled=false")
    if headless:
        # print("Going headless")
        options.add_argument("--headless")
        options.add_argument("--window-size=1920x1080")
    if maximize:
        # print("Maximizing Chrome ")
        options.add_argument("--start-maximized")
    if proxy:
        # print(f"Adding proxy: {proxy}")
        options.add_argument(f"--proxy-server={proxy}")
    if incognito:
        # print("Going incognito")
        options.add_argument("--incognito")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)


def logo():
    print(r"""
    ___________                     __________                __                 
    \_   _____/____    _________.__.\______   \_______  ____ |  | __ ___________ 
     |    __)_\__  \  /  ___<   |  | |    |  _/\_  __ \/  _ \|  |/ // __ \_  __ \
     |        \/ __ \_\___ \ \___  | |    |   \ |  | \(  <_> )    <\  ___/|  | \/
    /_______  (____  /____  >/ ____| |______  / |__|   \____/|__|_ \\___  >__|   
            \/     \/     \/ \/             \/                    \/    \/       
=======================================================================================
                 easybroker.com scraper by github.com/evilgenius786
=======================================================================================
[+] CSV/JSON Output
[+] Resumable
[+] Duplicate checking
[+] Multi-threaded
_______________________________________________________________________________________
""")


if __name__ == "__main__":
    main()
