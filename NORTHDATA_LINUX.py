import time
from selenium import webdriver
from selenium.webdriver.common.by import By
import json
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
import pandas as pd
import os
import requests
import psycopg2
import math
from webdriver_manager.chrome import ChromeDriverManager
from psycopg2 import sql as sqlpsycop
from datetime import datetime,timedelta
source_data = "google_search"

data_table_updated = 'north_data_updated'
data_table_history = 'north_data_history'

path= os.getcwd()
directory_path = "data"

def download_pdf(url, save_path):
    response = requests.get(url)
    with open(save_path, 'wb') as file:
        file.write(response.content)
def connection_db():
    # Database connection parameters
    dbname = "postgres"
    user = "dbuser"
    password = "black44#!55"
    host = "10.10.1.55"  # or your database server address
    port = "5432"  # or your database server port

    # Establish a connection to the database
    connection = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )
    cursor = connection.cursor()

    return [connection, cursor]
def create_directory(directory_path):
    try:
        os.makedirs(directory_path)
        print(f"Directory '{directory_path}' created successfully.")
    except FileExistsError:
        print(f"Directory '{directory_path}' already exists.")

create_directory(directory_path)

returntype = connection_db()
db = returntype[0]
cursor = returntype[1]

def create_table_andinsert_data():
    # Check if the table exists
    check_table_query = sqlpsycop.SQL("""
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_name = {}
                )
            """).format(sqlpsycop.Literal(data_table_updated))
    cursor.execute(check_table_query)
    table_exists = cursor.fetchone()[0]
    if not table_exists:
        # Define the SQL command to create the table for updated data
        create_table_query = sqlpsycop.SQL("""
                          CREATE TABLE IF NOT EXISTS {} (
                            id SERIAL PRIMARY KEY,
                            start_time TIMESTAMP DEFAULT NULL,
                            end_time TIMESTAMP DEFAULT NULL,
                            company_id TEXT,
                            company_name TEXT,
                            url TEXT,	
                            name TEXT,
                            register TEXT,
                            address TEXT,
                            gegenstand TEXT,
                            telephone TEXT,
                            fax TEXT,
                            email TEXT,
                            website TEXT,
                            ust_id TEXT,
                            graph TEXT,
                            konzernjahresabschluss TEXT,
                            publications TEXT,
                            finanzen TEXT,
                            mktg_tech TEXT,
                            balance TEXT,
                            income TEXT,
                            wz_branchencode TEXT,
                            wz_name TEXT,
                            liste_der_geselleschafter BYTEA,
                            markenbekanntmachungen TEXT,
                            jahressabschluss TEXT,
                            HTML TEXT
                           );
                    """).format(sqlpsycop.Identifier(data_table_updated))

        # Execute the SQL command
        cursor.execute(create_table_query)

        # Commit the changes to the database
        db.commit()

        # Define the SQL command to insert data from the source table into the destination table
        insert_data_query = sqlpsycop.SQL("""
                        INSERT INTO {} (company_id, name,url)
                        SELECT id, name ,northdata
                        FROM {};
                    """).format(sqlpsycop.Identifier(data_table_updated), sqlpsycop.Identifier(source_data))

        # Execute the SQL command to insert data
        cursor.execute(insert_data_query)

        # Commit the changes to the database
        db.commit()

        # Define the SQL command to create the table history
        create_table_query = sqlpsycop.SQL("""
                                        CREATE TABLE IF NOT EXISTS {} (
                                        id SERIAL PRIMARY KEY,
                                        start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                        end_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                        company_id TEXT,
                                        company_name TEXT,
                                        url TEXT,	
                                        name TEXT,
                                        register TEXT,
                                        address TEXT,
                                        gegenstand TEXT,
                                        telephone TEXT,
                                        fax TEXT,
                                        email TEXT,
                                        website TEXT,
                                        ust_id TEXT,
                                        graph TEXT,
                                        konzernjahresabschluss TEXT,
                                        publications TEXT,
                                        finanzen TEXT,
                                        mktg_tech TEXT,
                                        balance TEXT,
                                        income TEXT,
                                        wz_branchencode TEXT,
                                        wz_name TEXT,
                                        liste_der_geselleschafter BYTEA,
                                        markenbekanntmachungen TEXT,
                                        jahressabschluss TEXT,
                                        HTML TEXT
                                            );
                                    """).format(sqlpsycop.Identifier(data_table_history))

        # Execute the SQL command
        cursor.execute(create_table_query)

        # Commit the changes to the database
        db.commit()
    else:
        insert_data_query = sqlpsycop.SQL("""
            INSERT INTO {} (company_id, name, url)
            SELECT id, name, northdata
            FROM {}
            WHERE NOT EXISTS (
                SELECT 1
                FROM {} dest
                WHERE dest.company_id = {}.id::TEXT
            );
        """).format(
            sqlpsycop.Identifier(data_table_updated),
            sqlpsycop.Identifier(source_data),
            sqlpsycop.Identifier(data_table_updated),
            sqlpsycop.Identifier(source_data)
        )
        # Execute the SQL command to insert data
        cursor.execute(insert_data_query)

        # Commit the changes to the database
        db.commit()


create_table_andinsert_data()

try:
    # Define the SQL command to select the next company to scrape
    select_next_company_query = sqlpsycop.SQL("""
                SELECT id
                FROM {}
                WHERE end_time is NULL           
            """).format(sqlpsycop.Identifier(data_table_updated))

    # Execute the SQL command to select the next company
    cursor.execute(select_next_company_query)

    # Fetch the result
    result_endtime = cursor.fetchall()

    if len(result_endtime) == 0:
        print('-----------------------------------------------------')
        print('!!!!!!!   All companies are scraped you want to scrape the data again!!! --------')
        print('-----------------------------------------------------')

        choice = input('Do You want to Run the script again pres y(YES) or n(NO) to continue !!!!')

        if choice == 'y':
            update_start_time_query = sqlpsycop.SQL("""
                        UPDATE {}
                        SET start_time = NULL,
                        end_time = NULL                ;
                    """).format(sqlpsycop.Identifier(data_table_updated))

            # Execute the SQL command to update start_time
            cursor.execute(update_start_time_query)

            # Commit the changes to the database
            db.commit()
        else:
            sys.exit()
except Exception as E:
    returntype = connection_db()
    db = returntype[0]
    cursor = returntype[1]


def get_next_company_to_scrape(table_name):
    try:
        # Define the SQL command to select the next company to scrape
        select_next_company_query = sqlpsycop.SQL("""
                    SELECT id,company_id, name,url
                    FROM {}
                    WHERE start_time IS NULL
                    ORDER BY id
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED;
                """).format(sqlpsycop.Identifier(table_name))

        # Execute the SQL command to select the next company
        cursor.execute(select_next_company_query)

        # Fetch the result
        result = cursor.fetchone()

        if result:
            # Get column names
            column_names = [desc[0] for desc in cursor.description]

            # Create a dictionary with column names as keys and result values as values
            result_dict = {column_names[i]: result[i] for i in range(len(column_names))}

            # Update the start_time to mark the company as in progress
            update_start_time_query = sqlpsycop.SQL("""
                        UPDATE {}
                        SET start_time = CURRENT_TIMESTAMP
                        WHERE id = {};
                    """).format(sqlpsycop.Identifier(table_name), sqlpsycop.Literal(result_dict['id']))

            # Execute the SQL command to update start_time
            cursor.execute(update_start_time_query)

            # Commit the changes to the database
            db.commit()

            # Return the company name
            return result_dict
    except:
        pass


def scraping_source(companyinfo):

    element = WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'input[placeholder="Firma oder Person"]')))
    if companyinfo['url'] == "" or companyinfo['url'] == None:
        driver.find_element(By.CSS_SELECTOR, 'input[placeholder="Firma oder Person"]').clear()
        time.sleep(0.5)
        driver.find_element(By.CSS_SELECTOR, 'input[placeholder="Firma oder Person"]').send_keys(companyinfo['name'])
        # driver.find_element(By.CSS_SELECTOR,'input[placeholder="Firma oder Person"]' ).send_keys('Robert Bosch GmbH, Gerlingen')
        # driver.find_element(By.CSS_SELECTOR,'input[placeholder="Firma oder Person"]' ).send_keys('incari GmbH, Berlin')
        time.sleep(0.5)
        try:
            driver.find_element(By.CSS_SELECTOR, "div.results.transition.visible a").click()
            time.sleep(0.5)
        except:
            pass
        try:
            driver.find_element(By.CSS_SELECTOR, 'input[placeholder="Firma oder Person"]').send_keys(Keys.ENTER)
        except:
            pass
    else:
        driver.get(companyinfo['url'])



    time.sleep(2)

    try:
        driver.find_element(By.CSS_SELECTOR, "span#cmpwelcomebtnyes a").click()
        time.sleep(0.5)
    except:
        a = 1
        pass
    try:
        driver.find_elements(By.CSS_SELECTOR, "div.content div.summary a")[0].click()
    except:
        pass

    try:
        for check_pont_loop in driver.find_elements(By.CSS_SELECTOR, "div.content div.summary a"):
            if urls_inputs['name'].lower().replace(".", "") in check_pont_loop.text.replace(".", "").lower():
                check_pont_loop.click()
                break
    except:
        pass

    item = dict()
    item['company_name'] = companyinfo['name']
    item['company_id'] = companyinfo['id']
    item['url'] = driver.current_url
    try:
        for index, loop in enumerate(
                driver.find_elements(By.CSS_SELECTOR, "div.ui.stackable.two.column.grid div.column")[
                    0].find_elements(By.XPATH, "./*")):
            # for inner_index , inner_loop in enumerate(loop.find_elements(By.CSS_SELECTOR,"h3")):
            if loop.text.strip() == 'NAME':
                try:
                    item['NAME'] = \
                        driver.find_elements(By.CSS_SELECTOR, "div.ui.stackable.two.column.grid div.column")[
                            0].find_elements(By.XPATH, "./*")[index + 1].find_element(By.CSS_SELECTOR,
                                                                                      "div.item").text
                except:
                    item['NAME'] = ""
            if loop.text.strip() == 'REGISTER':
                try:
                    # item['REGISTER'] =  driver.find_elements(By.CSS_SELECTOR,"div.ui.stackable.two.column.grid div.column")[0].find_elements(By.XPATH,"./*")[index+1].find_element(By.CSS_SELECTOR,"div.item").text.replace('Ut',"").strip()
                    item['REGISTER'] = \
                        driver.find_elements(By.CSS_SELECTOR, "div.ui.stackable.two.column.grid div.column")[
                            0].find_elements(By.XPATH, "./*")[index + 1].text
                except:
                    item['REGISTER'] = ""
            if loop.text.strip() == 'ADRESSE':
                try:
                    item['ADRESSE'] = \
                        driver.find_elements(By.CSS_SELECTOR, "div.ui.stackable.two.column.grid div.column")[
                            0].find_elements(By.XPATH, "./*")[index + 1].find_element(By.CSS_SELECTOR,
                                                                                      "div.item").text
                except:
                    item['ADRESSE'] = ""
            if loop.text.strip() == 'GEGENSTAND':
                try:
                    item['GEGENSTAND'] = \
                        driver.find_elements(By.CSS_SELECTOR, "div.ui.stackable.two.column.grid div.column")[
                            0].find_elements(By.XPATH, "./*")[index + 1].text
                except:
                    item['GEGENSTAND'] = ""
            if loop.text.strip() == 'WEITERE INFORMATIONEN':
                all_info_social = \
                    driver.find_elements(By.CSS_SELECTOR, "div.ui.stackable.two.column.grid div.column")[
                        0].find_elements(By.XPATH, "./*")[index + 1].text
                for table_loop in all_info_social.split('\n'):
                    if 'Tel.' in table_loop:
                        item['Telephone'] = table_loop.split('Tel.')[-1].strip()
                    if 'Fax' in table_loop:
                        item['Fax'] = table_loop.split('Fax')[-1].strip()
                    if 'E-Mail' in table_loop:
                        item['E-Mail'] = table_loop.split('E-Mail')[-1].strip()
                    if 'Website' in table_loop:
                        item['Website'] = table_loop.split('Website')[-1].strip()
                    if 'USt.-Id.' in table_loop:
                        item['UST-ID'] = table_loop.split('USt.-Id.')[-1].strip()
    except:
        try:

            for index, loop in enumerate(
                    driver.find_elements(By.CSS_SELECTOR, "div.ui.stackable")[0].find_element(By.CSS_SELECTOR,
                                                                                              "div.column").find_elements(
                        By.XPATH, './*')):
                # for inner_index , inner_loop in enumerate(loop.find_elements(By.CSS_SELECTOR,"h3")):
                if loop.text.strip() == 'NAME':
                    try:
                        item['NAME'] = \
                            driver.find_elements(By.CSS_SELECTOR, "div.ui.stackable")[0].find_element(By.CSS_SELECTOR,
                                                                                                      "div.column").find_elements(
                                By.XPATH, './*')[index + 1].find_element(By.CSS_SELECTOR, "div.item").text
                    except:
                        item['NAME'] = ""
                if loop.text.strip() == 'REGISTER':
                    try:
                        # item['REGISTER'] =  driver.find_elements(By.CSS_SELECTOR,"div.ui.stackable.two.column.grid div.column")[0].find_elements(By.XPATH,"./*")[index+1].find_element(By.CSS_SELECTOR,"div.item").text.replace('Ut',"").strip()
                        item['REGISTER'] = \
                            driver.find_elements(By.CSS_SELECTOR, "div.ui.stackable")[0].find_element(By.CSS_SELECTOR,
                                                                                                      "div.column").find_elements(
                                By.XPATH, './*')[index + 1].text
                    except:
                        item['REGISTER'] = ""
                if loop.text.strip() == 'ADRESSE':
                    try:
                        item['ADRESSE'] = \
                            driver.find_elements(By.CSS_SELECTOR, "div.ui.stackable")[0].find_element(By.CSS_SELECTOR,
                                                                                                      "div.column").find_elements(
                                By.XPATH, './*')[index + 1].find_element(By.CSS_SELECTOR, "div.item").text
                    except:
                        item['ADRESSE'] = ""
                if loop.text.strip() == 'GEGENSTAND':
                    try:
                        item['GEGENSTAND'] = \
                            driver.find_elements(By.CSS_SELECTOR, "div.ui.stackable")[0].find_element(By.CSS_SELECTOR,
                                                                                                      "div.column").find_elements(
                                By.XPATH, './*')[index + 1].text
                    except:
                        item['GEGENSTAND'] = ""
                if loop.text.strip() == 'WEITERE INFORMATIONEN':
                    all_info_social = \
                        driver.find_elements(By.CSS_SELECTOR, "div.ui.stackable")[0].find_element(By.CSS_SELECTOR,
                                                                                                  "div.column").find_elements(
                            By.XPATH, './*')[index + 1].text
                    for table_loop in all_info_social.split('\n'):
                        if 'Tel.' in table_loop:
                            item['Telephone'] = table_loop.split('Tel.')[-1].strip()
                        if 'Fax' in table_loop:
                            item['Fax'] = table_loop.split('Fax')[-1].strip()
                        if 'E-Mail' in table_loop:
                            item['E-Mail'] = table_loop.split('E-Mail')[-1].strip()
                        if 'Website' in table_loop:
                            item['Website'] = table_loop.split('Website')[-1].strip()
                        if 'USt.-Id.' in table_loop:
                            item['UST-ID'] = table_loop.split('USt.-Id.')[-1].strip()
        except:
            pass

    item['graph'] = []

    try:

        for charts_data in json.loads(
                driver.find_element(By.CSS_SELECTOR, "div.tab-content.has-bar-charts").get_attribute('data-data'))[
                               'item'][:3]:
            item2 = dict()
            try:
                item2['title'] = charts_data['title']
            except:
                item2['title'] = ""
            item2['data'] = []
            for years_chart in charts_data['data']['data']:
                item3 = dict()
                try:
                    item3['year'] = years_chart['year']
                except:
                    item3['year'] = ""
                try:
                    item3['value'] = years_chart['formattedValue']
                except:
                    item3['value'] = ""
                try:
                    item3['publicationTitle'] = years_chart['source']['publicationTitle']
                except:
                    item3['publicationTitle'] = ""
                try:
                    item3['note'] = years_chart['note']
                except:
                    item3['note'] = ""
                item2['data'].append(item3)
            item['graph'].append(item2)

        item['graph'] = str(item['graph'])
    except:
        item['graph'] = str([])

    # click three tabs one by one
    try:
        item['KONZERNJAHRESABSCHLUSS'] = driver.find_element(By.CSS_SELECTOR,
                                                             'div.drill-downs.charts div.tab-content').get_attribute(
            'data-data')
    except:
        item['KONZERNJAHRESABSCHLUSS'] = ""

    try:
        element_to_move_to = driver.find_element(By.CSS_SELECTOR, 'a[data-tab="tab-dd-1"]')
        actions = ActionChains(driver)
        actions.move_to_element(element_to_move_to)
        actions.perform()
        time.sleep(2)
    except:
        time.sleep(0.5)
        a = 1
        pass

    item['publications'] = []

    liste_der_geselleschafter = False
    markenbekanntm = False
    jahressabschluss = False

    for publica in driver.find_elements(By.CSS_SELECTOR, "div.ui.feed div.event"):
        item_pub = dict()
        try:
            item_pub['icon_id'] = \
                publica.find_element(By.CSS_SELECTOR, "div.label a").get_attribute('href').split('id=')[-1]
        except:
            item_pub['icon_id'] = ''
        try:
            item_pub['date'] = publica.find_element(By.CSS_SELECTOR, "div.content div.date").text
        except:
            item_pub['date'] = ''
        try:
            item_pub['summary'] = publica.find_element(By.CSS_SELECTOR, "div.content div.summary").text
        except:
            item_pub['summary'] = ''
        item['publications'].append(item_pub)
    item['publications'] = str(item['publications'])
    # download csvs and store as json in database

    for tables_csv in driver.find_elements(By.CSS_SELECTOR, "table.ui.bizq.very.compact.celled.small"):

        if tables_csv.find_element(By.CSS_SELECTOR, "tr th.first").text.strip() == 'Finanzen':
            count_while = 0
            while count_while < 60:
                try:
                    count_while = count_while + 1
                    tables_csv.find_element(By.CSS_SELECTOR, 'a[title="CSV/Excel Download"]').click()
                    time.sleep(2)
                    break
                except Exception as E:
                    pass
            time.sleep(3)

            count_while1 = 0
            while count_while1 < 60:
                try:
                    count_while1 = count_while1 + 1
                    finanzen = dict()
                    finanzen['Finanzen'] = []
                    file_names = os.listdir(path + '/data')
                    Finanzen_file = pd.read_csv("data/" + file_names[0], sep=';', encoding='iso-8859-1', skiprows=2)
                    rows = len(Finanzen_file)
                    columns = list(Finanzen_file.columns)
                    columns.remove('Unnamed: 0')

                    for cols_loop in columns:
                        item_csv = dict()
                        item_csv['dataum'] = cols_loop
                        for record_loops in range(0, rows, 2):
                            try:
                                check_nan = math.isnan(Finanzen_file["Unnamed: 0"][record_loops + 1])
                            except:
                                check_nan = False

                            if check_nan == True:
                                item_csv[f'{Finanzen_file["Unnamed: 0"][record_loops]}'] = list(
                                    Finanzen_file[cols_loop][record_loops:record_loops + 2].fillna(""))
                            else:
                                try:
                                    item_csv[f'{Finanzen_file["Unnamed: 0"][record_loops]}'] = \
                                        Finanzen_file[cols_loop][record_loops]
                                except:
                                    pass
                                try:
                                    item_csv[f'{Finanzen_file["Unnamed: 0"][record_loops + 1]}'] = \
                                        Finanzen_file[cols_loop][record_loops + 1]
                                except:
                                    pass

                        finanzen['Finanzen'].append(item_csv)

                    try:
                        os.remove('data/' + file_names[0])
                        time.sleep(1)
                    except:
                        a = 1
                        pass
                    item['Finanzen'] = str(finanzen)
                    break
                except Exception as E:
                    pass



        elif tables_csv.find_element(By.CSS_SELECTOR, "tr th.first").text.strip() == 'Mktg & Tech':
            count_while2 = 0
            while count_while2 < 60:
                try:
                    count_while2 = count_while2 + 1
                    tables_csv.find_element(By.CSS_SELECTOR, 'a[title="CSV/Excel Download"]').click()
                    time.sleep(1)
                    break
                except Exception as E:
                    pass
            time.sleep(5)

            count_while3 = 0
            while count_while3 < 60:
                try:
                    count_while3 = count_while3 + 1
                    Mktg_Tech = dict()
                    Mktg_Tech['Mktg & Tech'] = []
                    file_names = os.listdir(path + '/data')
                    Mktg_Tech_file = pd.read_csv("data/" + file_names[0], sep=';', encoding='iso-8859-1',
                                                 skiprows=2)
                    # Mktg_Tech_file = Mktg_Tech_file.to_dict('records')

                    rows = len(Mktg_Tech_file)
                    columns = list(Mktg_Tech_file.columns)
                    columns.remove('Unnamed: 0')

                    for cols_loop in columns:
                        item_csv = dict()
                        item_csv['dataum'] = cols_loop
                        for record_loops in range(0, rows, 2):
                            try:
                                check_nan = math.isnan(Mktg_Tech_file["Unnamed: 0"][record_loops + 1])
                            except:
                                check_nan = False

                            if check_nan == True:
                                item_csv[f'{Mktg_Tech_file["Unnamed: 0"][record_loops]}'] = list(
                                    Mktg_Tech_file[cols_loop][record_loops:record_loops + 2].fillna(""))
                            else:
                                try:
                                    item_csv[f'{Mktg_Tech_file["Unnamed: 0"][record_loops]}'] = \
                                        Mktg_Tech_file[cols_loop][record_loops]
                                except:
                                    pass
                                try:
                                    item_csv[f'{Mktg_Tech_file["Unnamed: 0"][record_loops + 1]}'] = \
                                        Mktg_Tech_file[cols_loop][record_loops + 1]
                                except:
                                    pass

                        Mktg_Tech['Mktg & Tech'].append(item_csv)

                    try:
                        os.remove('data/' + file_names[0])
                        time.sleep(1)
                    except:
                        a = 1
                    item['Mktg & Tech'] = str(Mktg_Tech)
                    break
                except Exception as E:
                    pass


        elif (tables_csv.find_element(By.CSS_SELECTOR, "tr th.first").text.strip()) == 'EUR' or (
                tables_csv.find_element(By.CSS_SELECTOR, "tr th.first").text.strip() == 'Tsd. €'):
            count_while4 = 0
            while count_while4 < 60:
                try:
                    count_while4 = count_while4 + 1
                    tables_csv.find_element(By.CSS_SELECTOR, 'a[title="CSV/Excel Download"]').click()
                    time.sleep(1)
                    break
                except Exception as E:
                    pass
            time.sleep(5)
            count_while5 = 0
            while count_while5 < 60:
                try:
                    count_while5 = count_while5 + 1
                    file_names = os.listdir(path + '/data')
                    set_csv_name = file_names[0].split('_')[0]

                    euro_tsd = dict()
                    euro_tsd[f'{set_csv_name}'] = []

                    balance_file = pd.read_csv("data/" + file_names[0], sep=';', encoding='iso-8859-1', skiprows=2)
                    # balance_file = balance_file.to_dict('records')

                    rows = len(balance_file)
                    columns = list(balance_file.columns)
                    columns.remove('Unnamed: 0')

                    for cols_loop in columns:
                        item_csv = dict()
                        item_csv['dataum'] = cols_loop
                        for record_loops in range(0, rows, 2):
                            try:
                                check_nan = math.isnan(balance_file["Unnamed: 0"][record_loops + 1])
                            except:
                                check_nan = False

                            if check_nan == True:
                                item_csv[f'{balance_file["Unnamed: 0"][record_loops]}'] = list(
                                    balance_file[cols_loop][record_loops:record_loops + 2].fillna(""))
                            else:
                                try:
                                    item_csv[f'{balance_file["Unnamed: 0"][record_loops].strip()}'] = \
                                        balance_file[cols_loop][record_loops]
                                except:
                                    a = 1
                                try:
                                    item_csv[f'{balance_file["Unnamed: 0"][record_loops + 1].strip()}'] = \
                                        balance_file[cols_loop][record_loops + 1]
                                except:
                                    a = 1

                        euro_tsd[f'{set_csv_name}'].append(item_csv)

                    try:
                        os.remove('data/' + file_names[0])
                        time.sleep(1)
                    except:
                        a = 1
                    item[f'{set_csv_name}'] = str(euro_tsd)
                    break
                except Exception as E:
                    pass
    try:
        item['wz-branchencode'] = driver.find_element(By.CSS_SELECTOR, "div[title='WZ-Branchencode']").text
    except:
        item['wz-branchencode'] = ''
    try:
        item['wz-name'] = \
            [v.text.split('\n')[-1] for v in driver.find_elements(By.CSS_SELECTOR, "div.general-information") if
             v.find_element(By.CSS_SELECTOR, 'div.item div').get_attribute('title') == 'WZ-Branchencode'][0]
    except:
        item['wz-name'] = ""

    item['HTML'] = driver.find_element(By.CSS_SELECTOR, "section.ui.segments").get_attribute('outerHTML').replace(
        ' ', "").replace('\n\n', "")

    # three seperate links

    liste_der_geselleschafter = False
    markenbekanntm = False
    jahressabschluss = False

    for publica_three in driver.find_elements(By.CSS_SELECTOR, "div.ui.feed div.event"):
        if ('Liste der Gesellschafter' in publica_three.find_element(By.CSS_SELECTOR,
                                                                     "div.content div.summary").text) and (
                liste_der_geselleschafter == False):
            new_url = publica_three.find_element(By.CSS_SELECTOR, "div.content div.summary a").get_attribute('href')
            driver.execute_script("window.open('about:blank', '_blank');")
            # Get the handles of all open tabs/windows
            window_handles = driver.window_handles

            # Switch to the new tab (the last handle in the list)
            new_tab_handle = window_handles[-1]
            driver.switch_to.window(new_tab_handle)

            driver.get(new_url)
            time.sleep(1)

            pdf_url = driver.page_source.split('PDFObject.embed("')[-1].split('",')[0]

            name_pdf = driver.current_url.split('=')[-1]
            download_pdf(pdf_url, 'data/' + name_pdf + '.pdf')
            liste_der_geselleschafter = True

            with open('data/' + name_pdf + '.pdf', 'rb') as Liste_pdf:
                Liste_pdf_file = Liste_pdf.read()

            try:
                os.remove('data/' + name_pdf + '.pdf')
                time.sleep(1)
            except:
                a = 1
                pass
            item['liste_der_geselleschafter'] = Liste_pdf_file

            # Close the new tab
            driver.close()

            # Switch back to the default tab (the first handle in the list)
            default_tab_handle = window_handles[0]
            driver.switch_to.window(default_tab_handle)
        if ('Jah­res­ab­schluss' in publica_three.find_element(By.CSS_SELECTOR,
                                                               "div.content div.summary").text) and (
                jahressabschluss == False):
            new_url = publica_three.find_element(By.CSS_SELECTOR, "div.content div.summary a").get_attribute('href')
            driver.execute_script("window.open('about:blank', '_blank');")
            # Get the handles of all open tabs/windows
            window_handles = driver.window_handles

            # Switch to the new tab (the last handle in the list)
            new_tab_handle = window_handles[-1]
            driver.switch_to.window(new_tab_handle)

            driver.get(new_url)
            time.sleep(2)

            item['jahressabschluss'] = driver.find_element(By.CSS_SELECTOR, "div.publication-text").text

            # Close the new tab
            driver.close()

            # Switch back to the default tab (the first handle in the list)
            default_tab_handle = window_handles[0]
            driver.switch_to.window(default_tab_handle)
            jahressabschluss = True

        if ('Markenbekanntmachungen' in publica_three.find_element(By.CSS_SELECTOR,
                                                                   "div.content div.summary").text):
            new_url = publica_three.find_element(By.CSS_SELECTOR, "div.content div.summary a").get_attribute('href')
            driver.execute_script("window.open('about:blank', '_blank');")
            # Get the handles of all open tabs/windows
            window_handles = driver.window_handles

            # Switch to the new tab (the last handle in the list)
            new_tab_handle = window_handles[-1]
            driver.switch_to.window(new_tab_handle)

            driver.get(new_url)

            time.sleep(1)
            try:
                if len(item['Markenbekanntmachungen']) > 0:
                    pass
            except:
                item['Markenbekanntmachungen'] = []

            for loop_table_rpws in driver.find_elements(By.CSS_SELECTOR, "div.publication-text table tr"):
                new_dict = dict()
                new_dict['date'] = loop_table_rpws.find_elements(By.CSS_SELECTOR, "td")[0].text
                new_dict['name'] = loop_table_rpws.find_elements(By.CSS_SELECTOR, "td")[1].text
                try:
                    new_dict['url'] = loop_table_rpws.find_elements(By.CSS_SELECTOR, "td a")[0].get_attribute(
                        'href')
                except:
                    new_dict['url'] = ""
                try:
                    item['Markenbekanntmachungen'].append(new_dict)
                except:
                    a = 1
                    pass

            # Close the new tab
            driver.close()
            # Switch back to the default tab (the first handle in the list)
            default_tab_handle = window_handles[0]
            driver.switch_to.window(default_tab_handle)

    try:
        item['Markenbekanntmachungen'] = str(item['Markenbekanntmachungen'])
    except:
        item['Markenbekanntmachungen'] = str([])

    check_exit_items = ['url', 'NAME', 'REGISTER', 'ADRESSE', 'GEGENSTAND', 'Telephone', 'Fax', 'E-Mail', 'Website',
                        'UST-ID', 'graph', 'KONZERNJAHRESABSCHLUSS', 'publications', 'Finanzen', 'Mktg & Tech',
                        'balance', 'income', 'wz-branchencode', 'wz-name', 'Markenbekanntmachungen',
                        'jahressabschluss', 'HTML', 'liste_der_geselleschafter']
    for check_item in check_exit_items:
        if check_item == 'liste_der_geselleschafter':
            item[f'{check_item}'] = b''
        elif check_item not in list(item.keys()):
            item[f'{check_item}'] = ""

    if item['NAME'] != "":

        while True:
            try:
                update_query = (
                    f"UPDATE {data_table_updated} "
                    "SET end_time = CURRENT_TIMESTAMP ,company_name = %(company_name)s,register = %(REGISTER)s,address=%(ADRESSE)s,gegenstand = %(GEGENSTAND)s ,telephone = %(Telephone)s, fax = %(Fax)s, email = %(E-Mail)s, website = %(Website)s, ust_id = %(UST-ID)s, graph = %(graph)s, konzernjahresabschluss = %(KONZERNJAHRESABSCHLUSS)s, publications = %(publications)s, finanzen = %(Finanzen)s, mktg_tech = %(Mktg & Tech)s,balance = %(balance)s, income = %(income)s, wz_branchencode = %(wz-branchencode)s, wz_name = %(wz-name)s, liste_der_geselleschafter = %(liste_der_geselleschafter)s,markenbekanntmachungen = %(Markenbekanntmachungen)s,jahressabschluss = %(jahressabschluss)s ,HTML=%(HTML)s "
                    "WHERE name = %(NAME)s"
                )
                cursor.execute(update_query, item)
                db.commit()

                query = (
                    f"INSERT INTO {data_table_history} "
                    "(company_id,company_name,url,name,register,address,gegenstand,telephone,fax,email,website,ust_id,graph,konzernjahresabschluss,publications,finanzen,mktg_tech,balance,income,wz_branchencode,wz_name,liste_der_geselleschafter,markenbekanntmachungen,jahressabschluss,HTML)"
                    "VALUES (%(company_id)s,%(company_name)s,%(url)s, %(NAME)s, %(REGISTER)s, %(ADRESSE)s,%(GEGENSTAND)s, %(Telephone)s, %(Fax)s, %(E-Mail)s, %(Website)s, %(UST-ID)s, %(graph)s, %(KONZERNJAHRESABSCHLUSS)s, %(publications)s, %(Finanzen)s, %(Mktg & Tech)s, %(balance)s, %(income)s, %(wz-branchencode)s, %(wz-name)s,%(liste_der_geselleschafter)s, %(Markenbekanntmachungen)s, %(jahressabschluss)s, %(HTML)s)"
                )
                # Execute the query with the data
                cursor.execute(query, item)
                db.commit()
                break
            except Exception as E:
                returntype = connection_db()
                db = returntype[0]
                cursor = returntype[1]


def check_blocked_rows(data_table_name):
    three_days_ago = datetime.now() - timedelta(minutes=1)
    # Construct the SQL query to select the next company
    select_next_company_query = sqlpsycop.SQL("""
                SELECT id,company_id,name,url
                FROM {}
                WHERE end_time IS NULL 
                AND start_time <= {}
                ORDER BY id
                LIMIT 1
            """).format(sqlpsycop.Identifier(data_table_name), sqlpsycop.Literal(three_days_ago))
    # Execute the query to select the next company
    cursor.execute(select_next_company_query)

    # Fetch the selected row
    selected_row = cursor.fetchone()
    if selected_row:
        # Get column names
        column_names_blocked_rows = [desc[0] for desc in cursor.description]

        # Create a dictionary with column names as keys and result values as values
        result_dict_blocked_rows = {column_names_blocked_rows[i]: selected_row[i] for i in range(len(column_names_blocked_rows))}

        update_end_time_query = sqlpsycop.SQL("""
                                      UPDATE {}
                                      SET 
                                      start_time = CURRENT_TIMESTAMP
                                      WHERE name=%s and id=%s;
                                  """).format(sqlpsycop.Identifier(data_table_name))

        # Execute the SQL command to update start_time
        cursor.execute(update_end_time_query, (result_dict_blocked_rows['name'], result_dict_blocked_rows['id']))

        # Commit the changes to the database
        db.commit()
        return result_dict_blocked_rows
    else:
        return []


chrome_options = webdriver.ChromeOptions()
chrome_options.add_experimental_option("prefs", {
    "download.default_directory": path + f'/{directory_path}'
})

driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)
# driver = webdriver.Chrome(options=chrome_options)
# driver = webdriver.Chrome()
driver.maximize_window()

driver.get('https://www.northdata.de/_login')

driver.find_element(By.CSS_SELECTOR, 'input[name="email"]').send_keys('pjanski@gmx.de')
time.sleep(0.5)
driver.find_element(By.CSS_SELECTOR, 'input[name="password"]').send_keys('Ku1N5ROpoMjqCWIppO8a')
time.sleep(0.5)
driver.find_element(By.CSS_SELECTOR, 'button[type="submit"]').click()

while True:
    next_company = get_next_company_to_scrape(data_table_updated)
    if next_company != None:
        scraping_source(next_company)
    else:
        get_company_name_id = check_blocked_rows(data_table_updated)
        if len(get_company_name_id) > 0:
            scraping_source(get_company_name_id)
        else:
            print('-----------------------------------------------------')
            print('!!!!!!!  Great All companies are scraped !!! --------')
            print('-----------------------------------------------------')
            break


driver.quit()