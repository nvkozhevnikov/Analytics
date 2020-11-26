from requests_html import HTMLSession
from datetime import datetime
import re
import mysql.connector
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib


'''
Load all external links from Yandex Webmaster for all subdomains and main domain.
'''

token = os.environ['TOKEN_STB007_WEBMASTER']
header={"Authorization": f'OAuth {token}'}

user = '317060157'
host_for_check_updates ='https:sterbrust.com:443'

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
    query = ("SELECT date FROM seo_external_links_stb ORDER BY id DESC LIMIT 0, 1;")
    cursor.execute(query)
    row = cursor.fetchall()
    cursor.close()
    cnx.close()
    initial_date = row[0][0].strftime('%Y-%m-%d')

    return initial_date

def query_host_data (hosts):      

    session = HTMLSession()
    
    hosts_data = []
    for host in hosts:        
        sites_url = f'https://api.webmaster.yandex.net/v4/user/{user}/hosts/{host}/links/external/history?oauth_token={token}&indicator=LINKS_TOTAL_COUNT'
        link_count = session.get(sites_url, headers=header)
        link_count = link_count.json()

        data_dict = {}
        output_all = []
        pattern_host = r"https\:([\w-]{,20})\.sterbrust\.com\:443"
        pattern_host_root = r"https\:(sterbrust)\.com\:443"
        pattern_replace = r"-"

        for links in link_count['indicators']['LINKS_TOTAL_COUNT']:
            date = links['date'][0:10]
            value = links['value']
            output = [date, value]
            output_all.append(output)

        host_shortened = re.findall(pattern_host, host)

        if len(host_shortened) > 0:
            #If the name contains "-" then replace it to "_"
            if re.findall(pattern_replace, host_shortened[0]):
                hs = host_shortened[0].replace('-','_')
                data_dict[hs] = output_all
                hosts_data.append(data_dict)
            else:
                data_dict[host_shortened[0]] = output_all
                hosts_data.append(data_dict)
        if len(host_shortened) == 0:
            host_shortened = re.findall(pattern_host_root, host)
            if len(host_shortened) > 0:
                data_dict[host_shortened[0]] = output_all
                hosts_data.append(data_dict)

    return hosts_data

def query_hosts():
    session = HTMLSession()

    sites_url = f'https://api.webmaster.yandex.net/v4/user/{user}/hosts/?oauth_token={token}'
    resp = session.get(sites_url,  headers=header)
    resp = resp.json()
    print(resp)

    try:
        resp['error_code'] == 'INVALID_OAUTH_TOKEN'
        send_mail(from_email, password, to_email, 'Error - INVALID_OAUTH_TOKEN','<p>INVALID_OAUTH_TOKEN for Yandex Webmaster for sterbrust007@yandex.ru</p> <p><a href="https://docs.google.com/document/d/1xcNmMM4zLGzcIInnxSAE8vrelM9UCRaBTVjn1jcVcDE/">Intruction on how to get token</a></p>')
    except:
        hosts = []
        pattern = r"(https\:[\w\.-]{,20}sterbrust\.com\:443)"


        for host in resp['hosts']:
            include = re.findall(pattern, host['host_id'])
            if len(include) > 0:
                hosts.append(host['host_id'])
        return hosts

def get_list_dates(last_date, hosts_data):

#This algorithm needs to be improved
    dates_list = []
    for x in hosts_data[0]['abakan']:
        if last_date < x[0]:
            dates_list.append(x[0])

    return dates_list

def data_preparation(hosts_data, next_date):

    j = len(hosts_data) - 1
    i = 0
    insert = {}

    while i <= j:
        for k,v in hosts_data[i].items():
            for dates in v:
                if next_date == dates[0]:
                    insert[k] = dates[1]
        i += 1
    insert['date'] = next_date

    return insert

def send_data_mysql (insert):
    cnx = connect_db()
    cursor = cnx.cursor()
    
    query = ("INSERT INTO seo_external_links_stb (date, abakan, achinsk, angarsk, arkhangelsk, astrakhan, barnaul, belgorod, biysk, blagoveshchensk, bratsk, bryansk, cheboksary, chelyabinsk, chita, ekb, "
        "irkutsk, ivanovo, izhevsk, kaliningrad, kaluga, kazan, kemerovo, khabarovsk, khanty_mansiysk, kirov, kostroma, kotlas, krasnodar, krasnoyarsk, kurgan, kursk, lipetsk, magnitogorsk, miass, "
        "msk, murmansk, naberezhnye_chelny, nizhniy_tagil, novokuznetsk, novosibirsk, omsk, orel, orenburg, penza, perm, petrozavodsk, pskov, pyatigorsk, rostov, ryazan, salekhard, samara, saransk, "
        "saratov, simferopol, smolensk, solikamsk, spb, staryy_oskol, stavropol, sterbrust, surgut, syktyvkar, syzran, taganrog, tambov, tolyatti, tomsk, tula, tver, tyumen, ufa, ukhta, ulan_ude,"
        " ulyanovsk, velikiy_novgorod, vladikavkaz, vladimir, vladivostok, volgograd, vologda, voronezh, yaroslavl, yoshkar_ola) VALUES (%(date)s, %(abakan)s, %(achinsk)s, %(angarsk)s, %(arkhangelsk)s, "
        "%(astrakhan)s, %(barnaul)s, %(belgorod)s, %(biysk)s, %(blagoveshchensk)s, %(bratsk)s, %(bryansk)s, %(cheboksary)s, %(chelyabinsk)s, %(chita)s, %(ekb)s, %(irkutsk)s, %(ivanovo)s, %(izhevsk)s, "
        "%(kaliningrad)s, %(kaluga)s, %(kazan)s, %(kemerovo)s, %(khabarovsk)s, %(khanty_mansiysk)s, %(kirov)s, %(kostroma)s, %(kotlas)s, %(krasnodar)s, %(krasnoyarsk)s, %(kurgan)s, %(kursk)s, "
        "%(lipetsk)s, %(magnitogorsk)s, %(miass)s, %(msk)s, %(murmansk)s, %(naberezhnye_chelny)s, %(nizhniy_tagil)s, %(novokuznetsk)s, %(novosibirsk)s, %(omsk)s, %(orel)s, %(orenburg)s, %(penza)s, "
        "%(perm)s, %(petrozavodsk)s, %(pskov)s, %(pyatigorsk)s, %(rostov)s, %(ryazan)s, %(salekhard)s, %(samara)s, %(saransk)s, %(saratov)s, %(simferopol)s, %(smolensk)s, %(solikamsk)s, %(spb)s, "
        "%(staryy_oskol)s, %(stavropol)s, %(sterbrust)s, %(surgut)s, %(syktyvkar)s, %(syzran)s, %(taganrog)s, %(tambov)s, %(tolyatti)s, %(tomsk)s, %(tula)s, %(tver)s, %(tyumen)s, %(ufa)s, %(ukhta)s, "
        "%(ulan_ude)s, %(ulyanovsk)s, %(velikiy_novgorod)s, %(vladikavkaz)s, %(vladimir)s, %(vladivostok)s, %(volgograd)s, %(vologda)s, %(voronezh)s, %(yaroslavl)s, %(yoshkar_ola)s);")
    
    cursor.execute(query, insert)
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

def check_for_updates(host_for_check_updates):
    session = HTMLSession()
      
    sites_url = f'https://api.webmaster.yandex.net/v4/user/{user}/hosts/{host_for_check_updates}/links/external/history?oauth_token={token}&indicator=LINKS_TOTAL_COUNT'
    result_get = session.get(sites_url, headers=header)
    result_json = result_get.json()

    elements = len(result_json['indicators']['LINKS_TOTAL_COUNT']) - 1
    last_element =result_json['indicators']['LINKS_TOTAL_COUNT'][elements]
    date = last_element['date'][0:10]

    return date

def main():

    cnx = connect_db()
    last_date = read_last_date(cnx)
    check_date = check_for_updates(host_for_check_updates)

#Check if new data has appeared, if 'yes', then load all data
    if last_date != check_date:
        hosts = query_hosts()
        hosts_data = query_host_data(hosts)
        dates_list = get_list_dates(last_date, hosts_data)

#Iterate over all date that we are not in the database
        for next_date in dates_list:
            insert = data_preparation(hosts_data, next_date)
            send_data_mysql(insert)

main()