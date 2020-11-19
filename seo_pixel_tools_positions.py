import mysql.connector
from requests_html import HTMLSession
from datetime import datetime
import os

'''
Unloads statistics of the occupied site positions in Yandex search results for two regions (Moscow, Nizhny Novgorod) and Google.
'''


token = os.environ['TOKEN_PIXEL_TOOLS']
projects = ['3903','4549']

log = logging.getLogger(__name__)

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
	query = ("SELECT date FROM seo_pixel_tools_positions_stb ORDER BY id DESC LIMIT 0, 1;")
	cursor.execute(query)
	row = cursor.fetchall()
	cursor.close()
	cnx.close()
	initial_date = row[0][0].strftime('%Y-%m-%d')

	return initial_date

def send_data_mysql (total_result):
	cnx = connect_db()
	cursor = cnx.cursor()
	
	query = ("INSERT INTO seo_pixel_tools_positions_stb "
	    "(date, ya_msk_3, ya_msk_10, ya_msk_30, ya_msk_100_plus, ya_nn_3, ya_nn_10, ya_nn_30, ya_nn_100_plus, g_3, g_10, g_30, g_100_plus) "
	    "VALUES (%(date)s, %(ya_msk_3)s, %(ya_msk_10)s, %(ya_msk_30)s, %(ya_msk_100_plus)s, %(ya_nn_3)s, %(ya_nn_10)s, %(ya_nn_30)s, %(ya_nn_100_plus)s, %(g_3)s, %(g_10)s, %(g_30)s, %(g_100_plus)s);")
	
	insert_data = {
	'date':total_result[0],
	'ya_msk_3':total_result[1],
	'ya_msk_10':total_result[2],
	'ya_msk_30':total_result[3],
	'ya_msk_100_plus':total_result[4],
	'ya_nn_3':total_result[5],
	'ya_nn_10':total_result[6],
	'ya_nn_30':total_result[7],
	'ya_nn_100_plus':total_result[8],
	'g_3':total_result[9],
	'g_10':total_result[10],
	'g_30':total_result[11],
	'g_100_plus':total_result[12]
	}

	cursor.execute(query, insert_data)
	cnx.commit()
	cursor.close()
	cnx.close()

def query_pixel_tools_last_id (projets, initial_date):

	#Query all updates in the pixel tools
	projects_result = {}
	for number_of_project in projets:
		url = f'https://tools.pixelplus.ru/projects/api/v1/updates/get?token={token}&project_id={number_of_project}'
		session = HTMLSession()
		updates = session.get(url).json()

		list_id_for_update  = []
		for i in updates:
			update_date = datetime.strftime(datetime.strptime (i['date'],'%Y-%m-%d %H:%M:%S'), '%Y-%m-%d')
			if update_date > initial_date:				
				insert = {update_date:i['id']}
				list_id_for_update.append(insert)
				projects_result[number_of_project] = list_id_for_update

	return projects_result
	

def query_pixel_tools_data(last_update_id, number_of_project):

	session = HTMLSession()

	def offset_query (offset, limits):

		positions_url = f'https://tools.pixelplus.ru/projects/api/v1/positions/get?token={token}&project_id={number_of_project}&update_id={last_update_id}&limit={limits}&offset={offset}'
		r = session.get(positions_url)
		return r.json()

	limits = 50000
	offset = 5

	query_positions = offset_query (offset, limits)

	quantity_elements = len(query_positions) - 1

	ya_msk_3 = 0
	ya_msk_10 = 0
	ya_msk_30 = 0
	ya_msk_100_plus = 0
	ya_nn_3 = 0
	ya_nn_10 = 0
	ya_nn_30 = 0
	ya_nn_100_plus = 0
	g_3 = 0
	g_10 = 0
	g_30 = 0
	g_100_plus = 0

	res_mass = []
	
	i = 0
	while i <= quantity_elements:

		pos = query_positions[i]['position']
		ss_id = query_positions[i]['ss_id']

		ss_id = int(ss_id)
		pos = int(pos)

		i += 1

	# Report Yandex MSK
		if ss_id == 1:
			if pos <= 3:
				ya_msk_3 += 1
			if pos <= 10:
				ya_msk_10 += 1
			if pos <= 30:
				ya_msk_30 += 1
			if pos >= 100:
				ya_msk_100_plus += 1

	# Report Yandex NN
		if ss_id == 24:
			if pos <= 3:
				ya_nn_3 += 1
			if pos <= 10:
				ya_nn_10 += 1
			if pos <= 30:
				ya_nn_30 += 1
			if pos >= 100:
				ya_nn_100_plus += 1
		
	# Report Google	

		if ss_id == 23:
			if pos <= 3:
				g_3 += 1
			if pos <= 10:
				g_10 += 1
			if pos <= 30:
				g_30 += 1
			if pos >= 100:
				g_100_plus += 1

	res_mass.append(ya_msk_3)
	res_mass.append(ya_msk_10)
	res_mass.append(ya_msk_30)
	res_mass.append(ya_msk_100_plus)

	res_mass.append(ya_nn_3)
	res_mass.append(ya_nn_10)
	res_mass.append(ya_nn_30)
	res_mass.append(ya_nn_100_plus)

	res_mass.append(g_3)
	res_mass.append(g_10)
	res_mass.append(g_30)
	res_mass.append(g_100_plus)

	return res_mass

def cacl_and_send_mysql (first_project,second_project, date_update):
	total_result = [x+y for x,y in zip(first_project, second_project)]
	total_result.insert(0,date_update)
	send_data_mysql(total_result)

def main():	

	cnx = connect_db()
	initial_date = read_last_date(cnx)
	list_ids_and_dates = query_pixel_tools_last_id (projects, initial_date)
	if len(list_ids_and_dates) > 0:		
		p3903 = list_ids_and_dates['3903']
		p4549 = list_ids_and_dates['4549']
		p3903.reverse()
		p4549.reverse()

		j = 0
		while j < len(p3903):
			date_3903 = [t for t in p3903[j].keys()][0]
			date_4549 = [t for t in p4549[j].keys()][0]

			if initial_date != date_3903 and date_3903 == date_4549:
				v_p3903 = query_pixel_tools_data([t for t in p3903[j].values()][0], '3903')
				v_p4549 = query_pixel_tools_data([t for t in p4549[j].values()][0], '4549')
				cacl_and_send_mysql (v_p3903, v_p4549, date_3903)
			j += 1

main()