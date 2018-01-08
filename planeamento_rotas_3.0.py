import heapq
import sys
import psycopg2
import csv
import geopy.distance
import numpy as np
import itertools
from math import sin, cos, sqrt, atan2, radians
from collections import defaultdict
import queue  
from collections import namedtuple
import time

armazens = []
armazens_latitude = []
latitude = []  #lista para os pontos da latitude (funciona tipo array)
longitude = [] 
id_wayp = []
enc_atendidas = []
n = int()

def db_atualizar_faz(id_e): #atuliza o estado da encomenda para processado

	conn_string = "host='db.fe.up.pt' dbname='ee12299' user='ee12299' password='drone'"
	conn = psycopg2.connect(conn_string)
	cursor = conn.cursor()
	ok = 'Processado'
	cursor.execute("UPDATE ola.faz SET estado = (%s)  WHERE id_e = (%s)",(ok, id_e,))
	conn.commit()	
	conn.close()

def db_ocupar_drone(disp):

	conn_string = "host='db.fe.up.pt' dbname='ee12299' user='ee12299' password='drone'"
	conn = psycopg2.connect(conn_string)
	cursor = conn.cursor()
	cursor.execute("UPDATE ola.drone SET reservado=TRUE WHERE id_d = %s",(str(disp)))  #coloca o drone escolhido para encomenda como reservado
	conn.commit()	
	conn.close()


def db_deleterows():##############################Apagar registos da tabela wprota##############################

	conn_string = "host='db.fe.up.pt' dbname='ee12299' user='ee12299' password='drone'"
	conn = psycopg2.connect(conn_string)
	cursor = conn.cursor()
	cursor.execute("Truncate ola.wprota")
	conn.commit()	
	conn.close()

def db_drone():##############################Verificar drones disponiveis##############################

	conn_string = "host='db.fe.up.pt' dbname='ee12299' user='ee12299' password='drone'"
	conn = psycopg2.connect(conn_string)
	cursor = conn.cursor()

	cursor.execute("SELECT id_d, disponibilidade,latitude, longitude, reservado FROM ola.drone ORDER BY id_d ASC") #vai buscar os drones todos à BD
	res = cursor.fetchall()

	lista_drones = list(res) 
	#print(lista_drones[0][2]) #[0][x] - x0 = id / x1 = disponibilidade / x2 = latitude /x3 = longitude

	conn.close()
	return lista_drones


def db_rota(id_ee,lista,lista2,lista3, drone):##############################ENVIO DA ROTA PARA BASE DE DADOS##############################

	conn_string = "host='db.fe.up.pt' dbname='ee12299' user='ee12299' password='drone'"
	conn = psycopg2.connect(conn_string)
	cursor = conn.cursor()

	##############################VARIAVEIS##############################
	i=0
	l=len(lista)
	l2=len(lista2)
	lat=''
	lon=''
	lat2=''
	lon2=''
	id_waypp=''
	id_waypp2=''

	if (lista3 != -1):
		l3=len(lista3)

		for i in range (1,l3-1):
			lat = (lat + repr(latitude[lista3[i]]))
			lon = (lon + repr(longitude[lista3[i]]))

			if(i!=l3-1):
				lat = (lat + ';')
				lon = (lon + ';')
			
	##############################OBTER LATITUDE, LONGITUDE E ID_WAYPOINT##############################
	for i in range(0,l):
		lat = (lat + repr(latitude[lista[i]]))
		lon = (lon + repr(longitude[lista[i]]))
		id_waypp = (id_waypp + repr(id_wayp[lista[i]])) 

	##############################ADICIONAR PONTO E VIRGULA ATÉ AO ULTIMO VALOR (EXCLUSIVE)##############################
		if(i!=l-1):
			lat = (lat + ';')
			lon = (lon + ';')
			id_waypp = (id_waypp + ';')

	##############################OBTER LATITUDE, LONGITUDE E ID_WAYPOINT##############################
	
	for j in range(1,l2):
		lat2 = (lat2 + repr(latitude[lista2[j]]))
		lon2 = (lon2 + repr(longitude[lista2[j]]))
		id_waypp2 = (id_waypp2 + repr(id_wayp[lista2[j]])) 

		##############################ADICIONAR PONTO E VIRGULA ATÉ AO ULTIMO VALOR (EXCLUSIVE)##############################
		if(j!=l2-1):
			lat2 = (lat2 + ';')
			lon2 = (lon2 + ';')
			id_waypp2 = (id_waypp2 + ';')

	if (l2 == 1):
		lat2 = str('-')
		lon2 = str('-')

	##############################ENVIO DA ROTA PARA BASE DE DADOS##############################
	cursor.execute ("INSERT INTO ola.wprota (id_wr,latitude,longitude,latitude2,longitude2,id_d) VALUES ('%d','%s', '%s','%s','%s','%s')"%(id_ee,lat, lon,lat2,lon2,drone)) # pedido para guardar na tabela das rotas
	
	conn.commit()	
	conn.close()

def db_waypoints():

	conn_string = "host='db.fe.up.pt' dbname='ee12299' user='ee12299' password='drone'"
	conn = psycopg2.connect(conn_string)
	cursor = conn.cursor()
	
	cursor.execute ("SELECT id_w, latitude, longitude FROM ola.waypoint")
	ver = cursor.fetchall()
	for row in ver:
		id_way, la, lo = row[0:3]  #guarda a primeira e segunda coluna das varivaeis la e lo, respetivamente

		id_wayp.append(int(id_way))
		latitude.append(float(la))  #guarda o que esta na variavel "la" no fundo a lista "latitude". 
		longitude.append(float(lo))
		
	cursor.execute ("SELECT latitude, longitude FROM ola.armazem ORDER by id_a ASC")
	ok = cursor.fetchall();

	for row in ok:
		la,lo = row[0:2]  #guarda a primeira e segunda coluna das varivaeis la e lo, respetivamente

		latitude.append(float(la))  #guarda o que esta na variavel "la" no fundo a lista "latitude". 
		longitude.append(float(lo))
		armazens_latitude.append(float(la))
	
	armazens = list(ok) #cria uma lista com os ids dos waypoints e armazéns
	len_id = len(id_wayp)+1
	len_arm = len(armazens)
	for i in range(0,(len_arm)):
		id_wayp.append(len_id+i)


	conn.close()
	return armazens

def db_encomenda():

	print('Encomendas atendidas', enc_atendidas)
	##############################VARIAVEIS##############################
	id_enc = []
	id_enc2 = []
	pe = []
	pr = []
	allprstr = ''

	conn_string = "host='db.fe.up.pt' dbname='ee12299' user='ee12299' password='drone'"
	#print ("Connecting to database\n	->%s" % (conn_string))
	conn = psycopg2.connect(conn_string)
	# conn.cursor will return a cursor object, you can use this cursor to perform queries
	cursor = conn.cursor()

	##############################SE NÃO HÁ ENCOMENDAS##############################
	cursor.execute ("SELECT id_e FROM ola.encomenda") 
	ver =  cursor.fetchone()
	if(ver == None):
		return -1, -1, -1
	
	##############################ENCOMENDAS DO ARMAZEM##############################
	cursor.execute ("SELECT id_e, latitude, longitude FROM ola.armazem JOIN ola.encomenda ON (ola.armazem.id_a = ola.encomenda.armazem_recolha)")
	allpr = cursor.fetchall();

	##############################CRIAR STRING COM OS VALORES##############################
	i = 0
	for i in range(0,len(allpr)):
		allprstr = str(allpr[i])
		allprstr = allprstr.replace('(', '')
		allprstr = allprstr.replace(')', '')
		allprstr = allprstr.split(',')
		
		id_enc.append(int(allprstr[0]))
		pr.append(allprstr[1]+','+allprstr[2])

	##############################ENTREGAS DO PONTO DE ENTREGA##############################
	cursor.execute ("SELECT id_e, latitude, longitude FROM ola.ponto_entrega_recolha JOIN ola.encomenda ON (ola.ponto_entrega_recolha.id_er = ola.encomenda.ponto_recolha)")
	allpr = cursor.fetchall();
	
	##############################CRIAR STRING COM OS VALORES##############################
	i = 0
	for i in range(0,len(allpr)):
		allprstr = str(allpr[i])
		allprstr = allprstr.replace('(', '')
		allprstr = allprstr.replace(')', '')
		allprstr = allprstr.split(',')
		
		id_enc.append(int(allprstr[0]))
		pr.append(allprstr[1]+','+allprstr[2])

	i=0
	j=0

	##############################COLOCAR IDs POR ORDEM##############################
	while(i<len(id_enc)):
		for j in range(0,len(id_enc)):
			if(id_enc[i]<id_enc[j]):
				temp = id_enc[i]
				temp2 = pr[i]
				id_enc[i] = id_enc[j]
				pr[i] = pr[j]
				id_enc[j] = temp
				pr[j] = temp2
		i=i+1

	##############################PONTO DE RECOLHA##############################
	cursor.execute ("SELECT id_e, latitude, longitude FROM ola.ponto_entrega_recolha JOIN ola.encomenda ON (ola.ponto_entrega_recolha.id_er = ola.encomenda.ponto_entrega)")
	allpr2 = cursor.fetchall();

	##############################CRIAR STRING COM OS VALORES##############################
	for i in range(0,len(allpr2)):
		allprstr2 = str(allpr2[i])
		allprstr2 = allprstr2.replace('(', '')
		allprstr2 = allprstr2.replace(')', '')
		allprstr2 = allprstr2.split(',')
		
		id_enc2.append(int(allprstr2[0]))
		pe.append(allprstr2[1]+','+allprstr2[2])
	
	i=0
	j=0
	temp = 0
	temp2 = 0

	##############################COLOCAR IDs POR ORDEM##############################
	while(i<len(id_enc2)):
		for j in range(0,len(id_enc2)):
			if(id_enc2[i]<id_enc2[j]):
				temp = id_enc2[i]
				temp2 = pe[i]
				id_enc2[i] = id_enc2[j]
				pe[i] = pe[j]
				id_enc2[j] = temp
				pe[j] = temp2
		i=i+1


	##############################VALIDAR IDs DE ENCOMENDAS IGUAIS##############################
	for i in range(0,len(id_enc)):
		if(id_enc[i]==id_enc2[i]):
			mrd=0
		else:
			mrd=1
	
	conn.close()

	if(mrd==0):
		return (id_enc, pr, pe)
	else:
		print('ERRO NAS ENCOMENDAS')

Edge = namedtuple('Edge', ['vertex', 'weight'])

#modulo para criar o grafo. O que precisas de ver aqui é o add.edge, é o que nos interessa. Que tem um ponto inicial, final e um peso
class GraphUndirectedWeighted(object):  
    def __init__(self, vertex_count):
        self.vertex_count = vertex_count
        self.adjacency_list = [[] for _ in range(vertex_count)]

    def add_edge(self, source, dest, weight):  #adiciona arestas
        assert source < self.vertex_count
        assert dest < self.vertex_count
        self.adjacency_list[source].append(Edge(dest, weight))
        self.adjacency_list[dest].append(Edge(source, weight))

    def get_edge(self, vertex):
        for e in self.adjacency_list[vertex]:
            yield e

    def get_vertex(self):
        for v in range(self.vertex_count):
            yield v

# Algoritmo, também não há muito a dizer
def dijkstra(graph, source, dest):  
    q = queue.PriorityQueue()
    parents = []
    distances = []
    start_weight = float("inf")

    for i in graph.get_vertex():
        weight = start_weight
        if source == i:
            weight = 0
        distances.append(weight)
        parents.append(None)

    q.put(([0, source]))

    while not q.empty():
        v_tuple = q.get()
        v = v_tuple[1]

        for e in graph.get_edge(v):
            candidate_distance = distances[v] + e.weight
            if distances[e.vertex] > candidate_distance:
                distances[e.vertex] = candidate_distance
                parents[e.vertex] = v
                # primitive but effective negative cycle detection
                if candidate_distance < -1000:
                    raise Exception("Negative cycle detected")
                q.put(([distances[e.vertex], e.vertex]))

    shortest_path = []
    end = dest
    while end is not None:
        shortest_path.append(end)
        end = parents[end]

    shortest_path.reverse()

    return shortest_path, distances[dest]



def distancia(lat1, lon1):  #Calcula distância entre duas coordenadas

	# approximate radius of earth in km
	R = 6373.0
	x = len(latitude)
	y = len(longitude)
	for i in range(x):
		for j in range(y):

			lat1 = radians(latitude[i])  #latitude de todos os waypoints
			lon1 = radians(longitude[i]) #longitude de todos os waypoints
			lat2 = radians(latitude[j])
			lon2 = radians(longitude[j])

			dlon = lon2 - lon1
			dlat = lat2 - lat1

			a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2 #fórmula de Haversine
			c = 2 * atan2(sqrt(a), sqrt(1 - a))

			distance = R * c

			np.distancematrix[i,j] = distance # matriz n por n que marca a distancia entre todos os pontos. Comeca na posição 0 e calculada até ao fim da lista de waypoints


def distancia_armazem(pe, arm):  #Calcula distância entre ponto entrega e armazem

	comp = 0
	R = 6373.0
	x = 3
	for i in range(x):

		lat1 = radians(pe[0]) #Latitude do ponto de entrega
		lon1 = radians(pe[1]) #Longitde do ponto de entrega
		lat2 = radians(float(arm[i][0])) #Latitude de todos os armazéns
		lon2 = radians(float(arm[i][1])) #longitude de todos os armazéns

		dlon = lon2 - lon1
		dlat = lat2 - lat1

		a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2 #fórmula de Haversine
		c = 2 * atan2(sqrt(a), sqrt(1 - a))

		distance = R * c

		if(distance == 0): # Se distancia for igual a 0, significa que é armazem e fica lá o drone
			id_arm = i
			break

		if(comp == 0):  # guarda o primeiro valor de distancia no comparador
			comp = distance
			id_arm = i

		if (comp > distance): # Escolhe a menor distancia
			comp = distance
			id_arm = i

		np.armazem_distance[0][i] = distance


	return id_arm

def distancia_drone(pr, drone):  #Calcula distância entre ponto recolha e posição drone
	comp = 0
	R = 6373.0
	x = len(drone)
	y = len(latitude)
	for i in range(x):

		lat1 = radians(pr[0]) #Latitude do ponto de recolha
		lon1 = radians(pr[1]) #longitude do ponto de recolha
		lat2 = radians(float(drone[i][2])) #latitude dos dornes disponíveis
		lon2 = radians(float(drone[i][3])) #longitude dos drones disponíveis

		dlon = lon2 - lon1
		dlat = lat2 - lat1

		a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2 #fórmula de Haversine
		c = 2 * atan2(sqrt(a), sqrt(1 - a))

		distance = R * c

		if(comp == 0):  # guarda o primeiro valor de distancia no comparador
			comp = distance
			id_drone = i

		if(distance == 0): #caso haja uma distancia igual a 0, pode ir logo esse drone
			id_drone = i
			break

		if (comp > distance): # Escolhe a menor distancia
			comp = distance
			id_drone = i


	return id_drone

def distancia_drone_waypoints(gps_drone):  #Calcula distância entre duas coordenadas, neste caso entre a posição do drones e wp

	R = 6373.0
	x = len(latitude)
	z = int(0)
	for j in range(x):
	
		lat1 = radians(float(gps_drone[0])) #latitude do drone
		lon1 = radians(float(gps_drone[1])) #longitude do drone
		lat2 = radians(latitude[j]) #latitude de todos os waypoints
		lon2 = radians(longitude[j]) #longitude de todos os waypoints

		dlon = lon2 - lon1
		dlat = lat2 - lat1

		a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2  #fórmula de Haversine
		c = 2 * atan2(sqrt(a), sqrt(1 - a))

		distance = R * c

		np.distancematrix[x,j] = distance #adiciona a distancia entre a posição do drone e todos os waypoints


def grafo (pr,pe, flag):

	g = GraphUndirectedWeighted(len(latitude))  #cria uma grafo vazio com o número de vértices igual ao numero de waypoints

	x = len(latitude)
	y = len(longitude)
	
	if(flag == 2):  #caso especifico para determinar o caminho do drone. Adiciona mais uma linha ao grafo
		g = GraphUndirectedWeighted(len(latitude)+1)
		final = latitude.index(pe[0])
		for i in range(x+1):
			for j in range(y+1):

				if  0 < np.distancematrix[i,j] < 1:  #Apenas considera pontos a menos 1km
				
					g.add_edge(i,j,np.distancematrix[i,j]) #adiciona esse pontos à matriz


	for i in range(x):
		for j in range(y):

			if 0 < np.distancematrix[i,j] < 1:
				
				g.add_edge(i,j,np.distancematrix[i,j])  #adiciona uma aresta ao grafo se for menor que o tamanho indicado no if anterior


	if(flag == 1):  #flag para calculo da rota normal
		inicial = latitude.index(pr[0])
		final = latitude.index(pe[0])

	if(flag == 0): #flag para calculo do armazem mais proximo
		inicial = latitude.index(pr[0])
		final = pe 

	if(flag == 2): #flag para calculo do drone mais perto
		inicial = i +1


	pontos = dijkstra(g,inicial,final) #copias esses pontos para uma lista
	distancia = pontos[1] #distancia do ponto inicial ao final (para efeitos de teste depois)
	pontos = pontos[0] #como os pontos ficam todos na mesma posicão (particularidade do modulo usado), é necessário esta linha
	str1 = str(pontos)  # necessário porque a lista vem com apenas uma posicção com os vários pontos, passar pra string e depois fazer divisão dos vários pontos
	str1 = str1.replace("[","")
	str1 = str1.replace("]","")
	lista = str1.split(",")
	lista = list(map(int, lista)) #passa a lista dos pontos de string para int
	return lista, distancia

def mudar_pe_pr(pe,pr): #mudei isto de sitio so porque pronto

	##############################ELIMINAR CHARs DESNECESSÁRIOS##############################
	pe = pe.replace("'", '')
	pr = pr.replace("'", '')

	##############################DIVIDIR POR VIRGULAS##############################
	pe = list(map(float, pe.split(',')))
	pr = list(map(float, pr.split(',')))

	return pe,pr

def search_list(id_arm):  #procurar a posição do armazem na lista

	x = armazens[id_arm][0]
	id_arm = latitude.index(float(x))
	return id_arm

def search_drone():

	id_drone = []
	lista_drones = db_drone() #lista dos drones
	x = len(lista_drones)
	#print(lista_drones[0][0]) #[0][x] - x0 = id / x1 = disponibilidade / x2 = latitude /x3 = longitude /x4 = reservado

	for i in range(0,x):
		if (lista_drones[i][1] == True and lista_drones[i][4] == False):
			id_drone.append(lista_drones[i])  #guardar uma lista com todos os drones disponiveis

	return id_drone


if __name__ == '__main__':
	
	print('Programa a correr')
	starttime=time.time()

	##############################VARIAVEIS##############################
	enc=0
	cnt=0
	last_enc = 0
	id_enc = []
	enough = 0
	enc_wait = 0

	armazens =db_waypoints()  # buscar os waypoints à BD

	np.distancematrix = np.empty([len(latitude)+1, len(longitude)+1]) # cria a matriz para as distâncias entre waypoints
	np.armazem_distance = np.empty([1 , len(armazens)]) #cria a matriz para as distâncias entre armazem e ponte entrega

	distancia(latitude, longitude)  # cálculo das distância entre todos entre os waypoints

	#db_deleterows() #apaga a tabela wprota (porque senao cada vez que se iniciava o programa ele tinha entrys iguais. Num programa a correr
				#desde o inicio nao daria esse problema)
	
	while(1):
		

		if(enc_wait == 0):
			(id_enc_lista, pr_lista, pe_lista) = db_encomenda() #vai buscar os pontos incial e final à BD

		
		##############################VERIFICAR SE JÁ HÁ ENCOMENDAS#######################################

		if(id_enc_lista == -1):  #caso não haja encomendas, começa o ciclo de novo. Fica aqui parado basicamente até haver encomenda
			continue
		

		##############################VERIFICAR SE A ENCOMENDA ESTÁ EM ESPERA##############################
		if(len(enc_atendidas) != 0 and enc_wait == 0):
			
			for i in range(0,len(id_enc_lista)):   # lê a lista toda de encomendas e compara com as que já foram atendidas
				if((id_enc_lista[i]) in enc_atendidas):
					repeat = 1 #caso já tenha sido atendida
					continue
				else:
					repeat = 0 #caso não tenha sida atendida

					id_drone = search_drone()

					if(len(id_drone) != 0): #Vê se tem algum drone disponivel. Se nao tiver, nao ira ter nada neste vetor
						drone = id_drone
					else:
						drone = -1
						enc_wait = 1
						id_enc = id_enc_lista[i]
						break

					enc_atendidas.append(id_enc_lista[i])  #adiciona a nova encomenda à lista de atendidas e vai buscar os pontos inicial e final
					id_enc = id_enc_lista[i]
					pe = pe_lista[i]
					pr = pr_lista[i]
					break

					
		elif(len(enc_atendidas) == 0 and enc_wait == 0):  # caso seja a primeira a encomenda
			
			id_drone = search_drone()
			if(len(id_drone) != 0):  #Vê se tem algum drone disponivel. Se nao tiver nao ira ter nada neste vetor
				drone = id_drone
			else:
				drone = -1
				enc_wait = 1
				pe = 1
				id_enc = id_enc_lista[0]
				continue

			enc_atendidas.append(id_enc_lista[0]) #adiciona a primeira encomenda à lista de atendidas e vai buscar os pontos inicial e final
			id_enc = id_enc_lista[0]
			pe = pe_lista[0]
			pr = pr_lista[0]
			repeat = 0

		

		if(pe!=1 and repeat==0 and drone != -1):##############################ATUAR EM CASO DE NOVA ENCOMENDA##############################
			
			pr_novo, pe_novo = mudar_pe_pr(pr,pe) 
			lista = grafo(pr_novo,pe_novo, 1) #serve para imprimir o grafo
			distancia = lista[1]

			lista = lista[0]
			lenn = len(lista)

			id_arm = distancia_armazem(pe_novo,armazens)  #vai buscar o id do armazem que está mais proximo do local de entrega
			id_arm = search_list(id_arm) #descobre qual é esse id na lista de waypoints para calcular o Dijkstra

			lista2 = grafo(pe_novo, id_arm, 0)  #grafo para a distância entre o ponto de entrega e armazens mais proximos
			lista2 = lista2[0]

			id_drone2 = distancia_drone(pr_novo, drone)  #qual o drone mais perto do ponto de recolha
			gps_drone = drone[id_drone2][2:4]  #latitude e longitude desse drone
			id_drone2 = drone[id_drone2][0]  #id_d na bd
			db_ocupar_drone(id_drone2)

			if (latitude.index(float(gps_drone[0])) != latitude.index(pr_novo[0])):  #ver o caminho do drone até ponto entrega
				distancia_drone_waypoints(gps_drone)
				lista3 = grafo(1, pr_novo, 2) #vai calcular o caminho do drone ao ponto de recolha
				print(lista3) # caminho + distância. Ainda não está a mandar para a BD. ver se é necessário ou não
				lista3 = lista3[0]
			else:
				lista3 = -1

			print('\n###################################### \nNOVA ENCOMENDA\n ID =', id_enc,'  DISTANCIA = ', distancia,'Km')
			print('ponto entrega:', pe, '\nponto recolha:', pr, '\ndrone:', id_drone2, '\n#######################################\n')
	
			db_atualizar_faz(id_enc)
			db_rota(id_enc,lista, lista2, lista3, id_drone2) #fica com o mesmo id da encomenda
			
		
		elif(drone==-1 and enc_wait == 1):
			print('\n######################\n Drones ocupados, encomenda em standby:', id_enc,'\n######################\n')
			enc_wait=0

		time.sleep(10.0 - ((time.time() - starttime) % 10.0)) #tempo de esperar entre pesquisas