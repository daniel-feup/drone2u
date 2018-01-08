import heapq
import sys
import psycopg2
import csv
	
latitude = []
longitude = []
i = int()

def main():
	#Connection string
	conn_string = "host='db.fe.up.pt' dbname='ee12299' user='ee12299' password='drone'"
	# print the connection string we will use to connect
	print ("Connecting to database\n	->%s" % (conn_string))

	# get a connection, if a connect cannot be made an exception will be raised here
	conn = psycopg2.connect(conn_string)

	# conn.cursor will return a cursor object, you can use this cursor to perform queries
	cursor = conn.cursor()
	print ("Connected!\n")


	cursor.execute ("SELECT id_w FROM ola.waypoint ORDER BY id_w ASC")
	#ver = cursor.fetchall()
	#x = ver.index(max(ver)) #Verifica qual é o maior id que existe na BD para inserir no próximo 
	x =0
	i = 0
	altura = 100
	

	for y in latitude:
		
		cursor.execute("INSERT INTO ola.waypoint VALUES (%s, %s, %s, %s,%s)", (x, x+200 , y, longitude[i],altura,)) #insere dados na próxima posição
		conn.commit()
		x = x+1
		i = i+1

	conn.close()




with open ('waypoints.csv', 'r') as csvfile:
	reader = csv.reader(csvfile, delimiter = ',')
	next (reader)
	#for row in reader:
	#	print(row)
	for row in reader:
		print(row)
		w,z = row[0:2]

		latitude.append(z)  #Grava coordenadas no fim da lista
		longitude.append(w)

	
def dms_to_dd(d, m, s):
	dd = d + float(m)/60 + float(s)/3600
	return dd


if __name__ == '__main__':

    main()


