from requests_html import HTMLSession
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
import mysql.connector
import os

token = os.environ['TOKEN_STB007_WEBMASTER']
header={"Authorization": f'OAuth {token}'}

user = '317060157'

from_email = os.environ['EMAIL_FOR_NOTIFICATION']
password = os.environ['EMAIL_FOR_NOTIFICATION_PASSWORD']
to_email = os.environ['EMAIL_FOR_NOTIFICATION_TO_STB']

def connect_db ():

    config = {
    'user': os.environ['DB_TRUSTHOST_STB_USER'],
    'password': os.environ['DB_TRUSTHOST_STB_PASS'],
    'host': os.environ['DB_TRUSTHOST_STB_HOST'],
    'database': os.environ['DB_TRUSTHOST_STB_DB_NAME'],
    'raise_on_warnings': True,
    'use_pure': True
    }
    cnx = mysql.connector.connect(**config)

    return cnx

def read_last_date (cnx):

    cursor = cnx.cursor()
    query = ("SELECT date, quantity_pages FROM seo_yandex_searchable_pages ORDER BY id DESC LIMIT 0, 1;")
    cursor.execute(query)
    row = cursor.fetchall()
    cursor.close()
    cnx.close()
    initial_date = row[0][0].strftime('%Y-%m-%d')
    initial_pages = row[0][1]
    initial_data = [initial_date, initial_pages]

    return initial_data

def send_data_mysql (total_result):
    cnx = connect_db()
    cursor = cnx.cursor()
    
    query = ("INSERT INTO seo_yandex_searchable_pages (date, quantity_pages) VALUES (%(date)s, %(quantity_pages)s);")
    
    insert_data = {
    'date':total_result[0],
    'quantity_pages':total_result[1]
    }

    cursor.execute(query, insert_data)
    cnx.commit()
    cursor.close()
    cnx.close()

def send_mail(from_email, password, to_email, subject, message):
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = from_email
    msg['To'] = to_email
    msg.attach(MIMEText(f'<h1 style="color: red">{subject}</h1><div>{message}</div>', 'html'))  
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(from_email, password)
    server.sendmail(from_email, to_email, msg.as_string())
    server.quit()

def query_from_ya_webmaster ():
    
    session = HTMLSession()

    sites_url = f'https://api.webmaster.yandex.net/v4/user/{user}/hosts/?oauth_token={token}'
    resp = session.get(sites_url,  headers=header)
    resp = resp.json()

    try:
        resp['error_code'] == 'INVALID_OAUTH_TOKEN'
        send_mail(from_email, password, to_email, 'Error - INVALID_OAUTH_TOKEN','<p>INVALID_OAUTH_TOKEN for Yandex Webmaster for sterbrust007@yandex.ru</p> <p><a href="https://docs.google.com/document/d/1xcNmMM4zLGzcIInnxSAE8vrelM9UCRaBTVjn1jcVcDE/">Intruction on how to get token</a></p>')
    except:
        stb_dict = []
        for host_id in resp['hosts']:
            moi_sity = host_id['host_id']
            if 'sterbrust.com' in moi_sity:
                stb_dict.append(moi_sity)

    searchable_pages_count = []
    for host in stb_dict:
        in_search = f'https://api.webmaster.yandex.net/v4/user/{user}/hosts/{host}/summary/?oauth_token={token}'
        res = session.get(in_search,  headers=header)
        res = res.json()
        count_in_host = res['searchable_pages_count']
        searchable_pages_count.append(count_in_host)    
    itogo_pages = sum(searchable_pages_count)

    return itogo_pages

def main():
    cnx = connect_db()
    initial_data = read_last_date(cnx)

    todayDate = datetime.today().strftime('%Y-%m-%d')
    if todayDate > initial_data[0]:
        itogo_pages = query_from_ya_webmaster()
        if initial_data[1] != itogo_pages:
            total_result = [todayDate, itogo_pages]
            send_data_mysql(total_result)
main()   



