'''
	# Asignatura: Sistemas de Gestion de Datos y de la Informacion
	# Practica 3 (MongoDB)
	# Grupo 10 (Adrian Garcia y Frank Julca)
	# Declaracion de integridad: Adrian Garcia y Frank Julca
	declaramos que esta solución es fruto exclusivamente de nuestro trabajo personal.
	No hemos sido ayudados por ninguna otra persona ni hemos obtenido la solucion de fuentes externas,
	y tampoco hemos compartido nuestra solucion con nadie. Declaramos ademas que no hemos realizado de manera
	deshonesta ninguna otra actividad que pueda mejorar nuestros resultados ni perjudicar los resultados de los demas.
	
'''
from pymongo import MongoClient
from bson.objectid import ObjectId

client = MongoClient('localhost',27017)

db = client.sgdi_pr3

# 1. Fecha y título de las primeras 'n' peliculas vistas por el usuario 'user_id'.
# >>> usuario_peliculas( 'fernandonoguera', 3 )
def usuario_peliculas(user_id, n):
	return db['usuarios'].find(
		{"_id":user_id},
		{"_id":0,"visualizaciones":{"$slice" : n},"visualizaciones.titulo":1,"visualizaciones.fecha":1})
	

# 2. _id, nombre y apellidos de los primeros 'n' usuarios a los que les gusten 
# varios tipos de película ('gustos') a la vez.
# >>> usuarios_gustos(  ['terror', 'comedia'], 5  )
def usuarios_gustos(gustos, n):
	return db.usuarios.find(
		{"gustos":{"$all": gustos}},
		{"_id":1,"nombre":1,"apellido1":1, "apellido2":1}).limit(n)
	

# 3. Numero de películas producidas (aunque sea parcialmente) en un pais 
# >>> num_peliculas( 'España' )
def num_peliculas(pais):
	return db.peliculas.find({"pais": pais}).count()
	
	
# 4. _id de los usuarios que viven en tipo de via y en un piso concreto.
# >>> usuarios_via_num('Plaza', 1)
def usuarios_via_num(tipo_via, numero):
	return db.usuarios.find(
		{"direccion.tipo_via": tipo_via, "direccion.piso": numero}, 
		{"_id":1})

	
	
# 5. _id de usuario de un determinado sexo y edad en un rango
# >>> usuario_sexo_edad('M', 50, 80)
def usuario_sexo_edad( sexo, edad_min, edad_max ):
	return db.usuarios.find(
		{"sexo": sexo, "edad": {"$gte":edad_min ,"$lte":edad_max}}, 
		{"_id":1})


	
# 6. Nombre, apellido1 y apellido2 de los usuarios cuyos apellidos coinciden,
# ordenado por edad ascendente
# >>> usuarios_apellidos()
def usuarios_apellidos():
	return db['usuarios'].find(
		{ '$where' : 'this.apellido1 == this.apellido2 '},
		{'_id':0,'nombre':1,'apellido1':1,'apellido2':1}).sort('edad',1)

	
# 7.- Titulo de las peliculas cuyo director tienen un nombre que empieza por
# un prefijo
# >>> pelicula_prefijo( 'Yol' )
def pelicula_prefijo( prefijo ):
	regexp = "^"+prefijo
	return db['peliculas'].find(
		{ 'director' :{'$regex':regexp} },
		{'_id':0,'titulo':1})
	
	

# 8.- _id de usuarios con exactamente 'n' gustos cinematográficos, ordenados por
# edad descendente
# >>> usuarios_gustos_numero( 6 )
def usuarios_gustos_numero(n):
	return db['usuarios'].find(
		{"gustos":{"$size":n}},{}).sort('edad',-1)
	
	
	
# 9.- usuarios que vieron una determinada pelicula en un periodo concreto
# >>> usuarios_vieron_pelicula( '583ef650323e9572e2813189', '1994-03-14', '2016-12-31' )
def usuarios_vieron_pelicula(id_pelicula, inicio, fin):
	return db['usuarios'].find(
		{'visualizaciones':{'$elemMatch':{'_id':ObjectId(id_pelicula), 'fecha':{'$gte':inicio,'$lte':fin}}}},{})

