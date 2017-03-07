'''
    # Asignatura: Sistemas de Gestion de Datos y de la Informacion
    # Practica 2 (Mineria de datos y recuperacion de la informacion)
    # Grupo 10 (Adrian Garcia y Frank Julca)
    # Declaracion de integridad: Adrian Garcia y Frank Julca
    declaramos que esta solucion es fruto exclusivamente de nuestro trabajo personal.
    No hemos sido ayudados por ninguna otra persona ni hemos obtenido la solucion de fuentes externas,
    y tampoco hemos compartido nuestra solucion con nadie. Declaramos ademas que no hemos realizado de manera
    deshonesta ninguna otra actividad que pueda mejorar nuestros resultados ni perjudicar los resultados de los demas.
    
 '''

import csv
from math import log

class ID3(object):
	
	# Recibe el conjunto de instancias y el atributo seleccionado. Devuelve un diccionario con las instancias
	# en las que aparecen cada valor del atributo .
	def particiones_por_atributo(self, datos, atributo):
		attr_particiones = {}

		for row in datos:
			attr_val = row[atributo] 
			if attr_val not in attr_particiones:
				attr_particiones[attr_val] = [row]
			else:
				attr_particiones[attr_val].append(row)
		return attr_particiones
	
	# Recibe el conjunto de instancias, cuenta las apariciones de las clases y devuelve la que mas veces aparece (moda)
	def moda(self, datos):	
		clase_repeticiones = {}
		for row in datos:
			clase = row['class']
			if clase not in clase_repeticiones:
				clase_repeticiones[clase] = 1
			else:
				clase_repeticiones[clase] += 1

		# Obtenemos la clase que mas veces aparece
		reps_max = max(clase_repeticiones.items(), key=lambda x: x[1])[1]

		# Obtenemos aquellas clases que aparezcan el mismo numero de veces que la clase maxima
		maximos = [(clase,reps) for clase,reps in clase_repeticiones.items() if reps == reps_max]

		# Desempatamos entre ellas por el mayor nombre de la clase
		ordenada_por_clase = sorted(maximos, key=lambda x:x[0],reverse=True)

		return ordenada_por_clase[0]

	# Recibe un conjunto de instancias a clasificar y una lista de atributos sobre los que ramificar.
	# Ejecuta el metodo TDIDT para construir y devolver el arbol de clasificacion ID3.
	def tdidt(self, datos, n_datos, atributos):

		# Hacemos la moda para ver la clase que mas veces aparece en el conjunto de datos y obtenemos sus repeticiones
		clase_moda, repeticiones = self.moda(datos)
		
		# Si no hay mas atributos candidatos o todas las instancias del conjunto son de una clase, devolvemos 
		# una hoja con la clase
		if not atributos or repeticiones == n_datos:
			return {'type':'leaf','class':clase_moda}

		# Seleccionamos un atributo de la lista de atributos
		atributo = self.seleccionaAtributo(datos,atributos)

		# Eliminamos el atributo seleccionado de la lista de candidatos
		nuevos_candidatos = [attr for attr in atributos if attr != atributo]
		
		# Creamos un nodo interno con el atributo seleccionado
		node = {'type':'node','attr':atributo,'children':{}}
		
		# Obtenemos las instancias en las que aparecen cada valor del atributo seleccionado
		particiones = self.particiones_por_atributo(datos,atributo)

		# Generamos recursivamente el arbol de clasificacion
		for valor in self.attr_values[atributo]:	
			hijo = {}
			if valor not in particiones:
				hijo = {'type':'leaf','class':clase_moda}
			else:
				hijo = self.tdidt(particiones[valor],len(particiones[valor]),nuevos_candidatos)
			node['children'][valor] = hijo
		return node

	# Devuelve un diccionario que cuenta cuantas veces aparece cada clase para cada valor de cada atributo
	# {AtributoN:{ValorN:{ClaseN:repeticiones}}...}
	def contarClasesPorValorDeAtributo(self,datos):
		attr_val_classes = {}

		for row in datos:
			# Creamos un diccionario por cada atributo menos class (cabecera CSV)
			if not attr_val_classes:
				for attr in row:
					if attr != 'class':
						attr_val_classes[attr] = {}

			# Calculamos el numero de veces que se repite una clase para cada valor de un atributo
			for attr in attr_val_classes:
				if row[attr] not in attr_val_classes[attr]:
					attr_val_classes[attr][row[attr]] = {row['class']: 1}
				else:
					if row['class'] in attr_val_classes[attr][row[attr]]:
						attr_val_classes[attr][row[attr]][row['class']] += 1
					else:
						attr_val_classes[attr][row[attr]][row['class']] = 1

		return attr_val_classes

	# Recibimos el conjunto de instancias y una lista de atributos. Contamos sus 
	# Devolvemos el atributo seleccionado.
	def seleccionaAtributo(self,datos,atributos):

		# Contamos las apariciones de cada clase para cada valor de cada atributo
		attr_val_classes = self.contarClasesPorValorDeAtributo(datos)
		n_instancias = len(datos)

		# Calculamos la entropia para cada atributo
		attr_entropia = []
		for attr in atributos:
			entropia_atributo = 0

			for attr_val in attr_val_classes[attr]:
				entropia_valor = 0
				partition_size = 0
				# Calculamos N (tam. de particion)
				for clase,frec in attr_val_classes[attr][attr_val].items():
					partition_size += frec

				# Calculamos entropia para cada particion
				for clase,frec in attr_val_classes[attr][attr_val].items():
					frac = frec/partition_size
					entropia_valor -= frac * log(frac,2)

				# Multiplicamos cada entropia por la proporcion de instancias en cada subconjunto de datos
				entropia_atributo += (partition_size/n_instancias)*(entropia_valor) 

			# Obtenemos una lista de tuplas (atributo, entropia)
			attr_entropia.append((attr,entropia_atributo))

		# Obtenemos el valor de la entropia minima
		val_min = min(attr_entropia, key=lambda x: x[1])[1]

		# Obtenemos aquellos atributos que tengan la entropia minima
		lista_menores = [attr for attr,entropia in attr_entropia if entropia==val_min]

		# Desempatamos entre ellas por el que tenga mayor nombre
		ordenada_por_nombre = sorted(lista_menores)

		return ordenada_por_nombre[0]

	# Recibe el arbol de clasificacion y la instancia a clasificar. Recorremos el arbol recursivamente
	# hasta encontrar una hoja y devolver la clase encontrada.
	def clasifica_arbol(self,arbol,instancia):

		if arbol['type'] == 'leaf':
			return arbol['class']
		else:
			for attr,val in instancia.items():
				if attr == arbol['attr']:
					n_instancia = {a:v for a,v in instancia.items() if a != attr}
					return self.clasifica_arbol(arbol['children'][val],n_instancia)

	# Recibe un arbol de clasificacion ID3, un nodo padre y el valor del atributo del padre seleccionado
	# Recorremos el arbol recursivamente para crear el grafo con la notacion necesaria y guardarla en 'nodos'
	# y en 'aristas'.
	def recorre_arbol(self, arbol, nodo_padre, valor_attr_padre):
		if arbol['type'] == 'leaf':
			clase = arbol['class']
			id_clase = (clase+str(self.llamada)).replace(' ','_')
			self.nodos.append(id_clase+' [label="'+clase+'"];')
			self.aristas.append(nodo_padre+' -> '+id_clase+'[label="'+valor_attr_padre.title()+'"];')
			return
		else:
			# Es un nodo (representa un atributo y tiene children)
			attr = arbol['attr']
			id_nodo = (attr+str(self.llamada)).replace(' ','_')
			self.nodos.append(id_nodo+' [label="'+attr.title()+'",shape="box"];')
			if self.llamada > 0:
				self.aristas.append(nodo_padre+' -> '+id_nodo+'[label="'+valor_attr_padre.title()+'"];')

			# Recorremos cada valor del hijo
			for valor_attr in arbol['children']:
				self.llamada += 1
				self.recorre_arbol(arbol['children'][valor_attr],id_nodo, valor_attr)
	'''
	Vuelca el arbol de clasificacion almacenado en la clase en un fichero de texto en formato DOT. 
	El metodo no devuelve nada.
	'''
	def save_tree(self, fichero):
		self.nodos = []
		self.aristas = []
		self.llamada = 0

		# Recorremos el arbol para crear los nodos y aristas del grafo
		self.recorre_arbol(self.tree,None,None)

		# Guardamos los datos obtenidos en el fichero
		output_file = open(fichero,'w+')
		output_file.write('digraph tree {\n//nodos \n') 
		for nodo in self.nodos:
			output_file.write(nodo+'\n')

		output_file.write('//aristas \n')
		for arista in self.aristas:
			output_file.write(arista+'\n')

		output_file.write('}')
		output_file.close()

	'''
	El constructor lee el fichero de texto fichero formado por instancias en formato CSV con cabecera. 
	El constructor no devuelve nada pero crea y almacena el arbol de clasificacion como un atributo del 
	objeto para poder usarlo para clasificar mas adelante.
	'''
	def __init__(self, fichero):
		input_file = csv.DictReader(open(fichero))

		self.tree = {}
		datos = []
		n_instancias = 0

		# Guardamos en datos todas las instancias (array de arrays)
		for row in input_file:
			n_instancias += 1
			datos.append(row)

		# Contamos las apariciones de cada clase para cada valor de cada atributo
		attr_val_classes = self.contarClasesPorValorDeAtributo(datos)

		# Guardamos los posibles valores de cada atributo
		self.attr_values = { attr: list(attr_val_classes[attr].keys()) for attr in attr_val_classes}

		# Obtenemos todos los atributos
		candidatos = [attr for attr in attr_val_classes]

		# Ejecutamos el algoritmo TDIDT
		self.tree = self.tdidt(datos, n_instancias ,candidatos)

	'''
	A partir de una instancia (diccionario que contiene los valores para cada atributo sin incluir la clase) 
	devuelve una cadena de texto con la clase que predice el arbol de clasificacion.
	'''
	def clasifica(self, instancia):
		return self.clasifica_arbol(self.tree,instancia)
	
	'''
	Este metodo sirve para medir la tasa de aciertos del arbol ID3 previamente entrenado. Para ello recibe un conjunto
	de test almacenado en fichero , que tiene el mismo formato CSV con cabecera que el fichero usado en el constructor, 
	y clasifica cada instancia de test leida. Devuelve una tupla (aciertos , total , tasa) conteniendo el numero total 
	de instancias de test clasificadas correctamente, el numero total de instancias de test y la tasa de aciertos 
	(aciertos/total).
	'''
	def test(self, fichero):
		input_file = csv.DictReader(open(fichero))

		predictedClasses = []
		aciertos = 0
		total = 0

		print ('Test:')
		for row in input_file:
			# Preparamos las instancias sin la clase
			instance = {}
			for attr in row:
				if attr != 'class':
					instance[attr] = row[attr]
			# Mostramos los valores de los atributos de la instancia completa, incluida la clase.
			print (str(instance) + '-' + row['class'])

			# Obtenemos la clase predicha a partir de la instancia
			predictedClass = self.clasifica(instance)

			# Mostramos la clase que predice el arbol de clasificacion ID3.
			print ('Clase predicha: ' + predictedClass)

			# Indicamos si la clasificacion acierta o falla y actualizamos los contadores.
			if predictedClass == row['class']:
				aciertos += 1
				print ('\t--> Acierto')
			else:
				print ('\t--> Fallo')

			total += 1

			print ('\n')

		# Devolvemos una tupla con los resultados del test
		tasa = aciertos / total
		resultTest = (aciertos, total, tasa)

		return resultTest

id3 = ID3("car.train")
#clase = id3.clasifica ({'day': 'holiday', 'wind': 'high', 'rain': 'heavy', 'season': 'winter'})
id3.save_tree("tree.dot")
print(id3.test("car.test"))
