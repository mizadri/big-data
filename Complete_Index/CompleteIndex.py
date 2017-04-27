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
import string
from math import log
from os import walk
import sys
import codecs
import bitarray as b

'''Recibe un numero y devuelve la representacion en unario del mismo'''
def unary(num):
	return b.bitarray((num-1)*'1'+'0')

'''Recibe un numero y devuelve el offset(representacion en binario sin el primer 1)'''
def offset(num):
	bit_arr = b.bitarray(bin(num)[2:])
	return bit_arr[1:]

'''Recibe un numero y devuelve su representacione en elias gamma'''
def elias_gamma(num):
	longitud = int(log(num,2)) + 1
	return unary(longitud) + offset(num)

'''Recibe un numero y devuelve su representacione en elias gamma'''
def elias_delta(num):
	longitud = int(log(num,2)) + 1
	return elias_gamma(longitud) + offset(num)

'''Recibe un numero y devuelve su representacione en variable bytes'''
def variable_bytes(num):

	blocks_array = []
	bitarray_num = b.bitarray(bin(num)[2:])
	bitarray_vb = b.bitarray()
	nbits = bitarray_num.length()

	block = []
	bit_count = 0
	# Recorre el numero en binario a la inversa para almacenarlo en bloques de 7 bits
	for bit in bitarray_num[::-1]:

		block.append(bit)
		bit_count += 1
		if bit_count % 7 == 0 or bit_count == nbits: # Cada 7 bits guarda un bloque, o si se ha terminado el numero
			blocks_array.append(block)
			block = []

	total_bytes = 0
	n_byte = 0
	if nbits % 7 == 0:
		total_bytes = int(nbits/7)
	else:
		total_bytes = int(nbits/7)+1

	# Recorre los bloques de 7 bits y les va añadiendo el bit marcador en funcion de su posicion 
	for block in blocks_array[::-1]:
		n_byte += 1
		if n_byte == total_bytes: # Comprueba si es el ultimo byte -> marcador = 1
			sep = []
			tam_block = len(block)
			if tam_block < 7:
				sep = [False] * (7-tam_block)
			bitarray_vb += b.bitarray([True]+sep+block[::-1])
		else:
			sep = []
			tam_block = len(block)
			if tam_block < 7:
				sep = [False] * (7-tam_block)
			bitarray_vb += b.bitarray([False]+sep+block[::-1])

	return bitarray_vb

'''Recibe un bitarray codificado en unario y devuelve un array con los numeros que codifica'''
def decode_unary_stream(bitarr):
	num_array = []
	bit_count = 0
	for bit in bitarr:
		bit_count += 1
		if bit == 0:
			num_array.append(bit_count)
			bit_count = 0
	return num_array

'''Recibe un bitarray codificado en elias-gamma y devuelve un array con los numeros que codifica'''
def decode_gamma_stream(bitarr):
	count_len = 0
	decoded_array = []

	len_finished = False

	binary_num = 0

	# Se recorre el bitarray cambiando entre una fase de leer la longitud del numero en unario y luego
	# reconstruir el numero con el offset
	
	for bit in bitarr:

		if not len_finished:

			count_len += 1
			if bit == 0:
				len_finished = True
				count_len -= 1
				binary_num = 2 ** (count_len)
				if count_len == 0: 
				# Si se lee un 0 como bit y no se han leido mas bits sera un 1, no hay que leer el offset
					decoded_array.append(1)
					len_finished = False
		else:
			count_len -= 1

			if bit == 1:
				binary_num += 2 ** count_len

			if count_len == 0:
				decoded_array.append(binary_num)
				len_finished = False

	return decoded_array

'''Recibe un bitarray codificado en elias-delta y devuelve un array con los numeros que codifica'''
def decode_delta_stream(bitarr):
	count_len = 0
	decoded_array = []

	len_finished = False
	calcular_delta = False
	binary_num = 0

	# Para leer una cadena de numeros en elias_delta primero se usa la tecnica de elias_gamma para
	# reconstruir la longitud del numero binario y una vez se ha obtenido se pasa a una fase de leer
	# el offset y recalcular el numero original
	for bit in bitarr:

		if calcular_delta:
			delta_len -= 1
			if bit == 1:
				delta_num += 2 ** delta_len

			if delta_len == 0:
				calcular_delta = False
				decoded_array.append(delta_num)

		else:

			if not len_finished:

				count_len += 1
				if bit == 0:
					len_finished = True
					count_len -= 1
					binary_num = 2 ** (count_len)
					if count_len == 0:
						decoded_array.append(1)
						len_finished = False
			else:
				count_len -= 1

				if bit == 1:
					binary_num += 2 ** count_len

				if count_len == 0:
					calcular_delta = True
					delta_len = binary_num - 1
					delta_num = 2 ** delta_len
					len_finished = False

	return decoded_array

'''Recibe un bitarray codificado en variable-bytes y devuelve un array con los numeros que codifica'''
def decode_variable_bytes_stream(bitarr):
	num_array = []
	num_bytes = []
	result = []

	bit_count = 7
	byte = [0]*7
	read_bits = False
	lastByte = False
	# Recorre todo el bitarray y va almacenando los bytes(sin el separador)de cada numero 
	# en un arrray(num_bytes). Cuando encuentra el bit de final de numero almacena num_bytes
	# en otro array. Que al final contendra un array de bytes por cada numero
	for bit in bitarr:
		if read_bits:
			bit_count -= 1
			byte[bit_count] = bit
			if bit_count == 0:
				num_bytes.append(byte)
				bit_count = 7
				byte = [0]*7
				read_bits = False
				if lastByte:
					num_array.append(num_bytes)
					num_bytes = []
		else:
			if bit == 1:
				lastByte = True
			else:
				lastByte = False

			read_bits = True

	# Traduce todos los numeros almacenados como arrays de sus bytes a numeros enteros
	# Primero los agrupa en un array unico de booleanos para poder calcular su longitud
	# facilmente y poder ir calculando la potencia de la posicion(si es True) y recontruir el numero original
	for bytes_numero in num_array:
		numero = 0
		firstByte = True
		num = []
		startCounting = False
		for byte in bytes_numero:
			reversed_byte = []

			for bit in byte[::-1]:
				if firstByte:
					if bit == 1:
						startCounting = True
					if startCounting:
						reversed_byte.append(bit)
				else:
					reversed_byte.append(bit)

			num += reversed_byte

		bits_num = len(num)
		int_num = 0
		for bit in num:
			bits_num -= 1
			if bit == 1:
				int_num += 2**bits_num
		result.append(int_num)

	return result
	
# Dada una linea de texto, devuelve una lista de palabras no vacias 
# convirtiendo a minusculas y eliminando signos de puntuacion por los extremos
# Ejemplo:
#   > extrae_palabras("Hi! What is your name? John.")
#   ['hi', 'what', 'is', 'your', 'name', 'john']
def extrae_palabras(linea):
	return filter(lambda x: len(x) > 0, map(lambda x: x.lower().strip(string.punctuation), linea.split()))


class CompleteIndex(object):

	def __init__(self, directorio, compresion=None):
		self.indice_completo = {}
		self.ndocs = 0
		self.doc_ruta = {}
		self.encode = None
		self.decode = None

		# Para aplicar la compresion almacenamos en las variables encode y decode las funciones 
		#	necesarias para comprimir un numero y descomprimir un bitarray
		if compresion == 'unary':
			self.encode = unary
			self.decode = decode_unary_stream
		elif compresion == 'variable-bytes':
			self.encode = variable_bytes
			self.decode = decode_variable_bytes_stream
		elif compresion == 'elias-gamma':
			self.encode = elias_gamma
			self.decode = decode_gamma_stream
		elif compresion == 'elias-delta':
			self.encode = elias_delta
			self.decode = decode_delta_stream

		# Recorremos el directorio indicado recursivamente visitando cada carpeta y archivo
		for (path, carpetas, archivos) in walk(directorio):
			if len(archivos) > 0:
				for archivo in archivos:
					self.ndocs += 1
					ruta = path+'/'+archivo

					fd = codecs.open(ruta,'r','iso-8859-1')
					nword = 0
					indice_anterior = 0
					# Almacenar los desplazamientos de las palabras de un documento
					palabras_doc = {}
					palabra_ultima_aparicion = {}
					for line in fd:
						for word in extrae_palabras(line):
							nword += 1
							if word in palabras_doc:
								indice_anterior = palabra_ultima_aparicion[word]
								palabras_doc[word].append(nword-indice_anterior)
							else:
								palabras_doc[word] = [nword]

							palabra_ultima_aparicion[word]=nword

					# Insertar los desplazamientos de todas las palabras del documento en el indice completo
					for word,apariciones in palabras_doc.items():
						pal_apariciones = apariciones
						encoded_offsets = b.bitarray()

						if self.encode != None:
							for offset in apariciones:
								encoded_offsets += self.encode(offset)
							pal_apariciones = encoded_offsets

						if word in self.indice_completo:
							self.indice_completo[word].append((self.ndocs,pal_apariciones))
						else:
							self.indice_completo[word] = [(self.ndocs,pal_apariciones)]

					fd.close()

					# Usamos el numero de documento como indice y lo guardamos en un diccionario para recuperar la ruta mas tarde
					if self.ndocs not in self.doc_ruta:
						self.doc_ruta[self.ndocs] = ruta
					
	# Avanza todos los indices de heads_ps
	def advanceAll(self, heads_ps):
		for i in range(len(heads_ps)):
			heads_ps[i] += 1

	# Avanza el indice de heads_ps que apunta al menor doc_id
	def advanceMin(self, lista_ps, heads_ps):
		lista_minimos = []
		heads = []  
		indice = 0
		minimo = sys.maxsize
		i_min = 0

		for head in heads_ps:
			doc,apariciones = lista_ps[indice][head]
			if doc < minimo:
				minimo = doc
				i_min = indice
			indice += 1

		heads_ps[i_min] += 1

	# Devuelve True si los indices de heads_ps apuntan al mismo documento
	def sameDocID(self, lista_ps, heads_ps):

		i = 0
		cond = True
		doc_anterior = None
		for lista_palabra in lista_ps:
			doc,apariciones = lista_palabra[heads_ps[i]]
			
			if doc_anterior != None:
				if doc_anterior != doc:
						cond = False
						break
			doc_anterior = doc
			i += 1

		return cond

	# Devuelve True si todas las palabras de la consulta son consecutivas
	def consecutive(self, lista_ps,heads_ps):
		offsets = []
		i = 0
		# Se obtienen todas las apariciones de las palabras y se cargan en una lista
		for lista in lista_ps:
			palabra_offsets = list(lista[heads_ps[i]][1])
			if self.decode != None:
				palabra_offsets = self.decode(palabra_offsets)
			offsets.append(palabra_offsets)
			i += 1

		# Traducir offsets a posiciones
		for lista_doc in offsets:
			sum_acumulada = 0
			for i in range(len(lista_doc)):
				sum_acumulada += lista_doc[i]
				if i > 0:
					lista_doc[i] = sum_acumulada

		result = False
		# Comprueba que todas las palabras de la consulta sean consecutivas
		# Para ello por todas las posiciones en las que aparece la primera palabra de la consulta,
		# se comprueba si todas las siguientes palabras contienen todos los numeros de posicion consecutivos
		for starting_index in offsets[0]:
			cond = True
			for i in range(1,len(offsets)):
				if (starting_index + i) not in offsets[i]:
					cond = False
			result = result or cond

		return result

	# Devuelve True si todos los indices de heads_ps no han llegado a su fin
	# Esto pasa si alguno de ellos ha recorrido la lista completa 
	def allNoNIL(self, lista_ps, heads_ps):
		i = 0
		for head in heads_ps:
			if head == len(lista_ps[i]):
				return False
			i += 1

		return True

	# Recibe una lista con una posicion por cada palabra de la consulta, en cada posicion 
	# se almacena una lista de tuplas (doc,[apariciones])
	# Devuelve una lista de documentos que contengan todas las palabras de la consulta consecutivas
	def intersect_phrase(self, lista_ps):
		cond = True
		# Para representar los heads, usamos una lista de indices(heads_ps) que guarda el
		# documento que se esta comprobando en una iteracion dada. Inicialmente todos apuntan al
		# documento de la posicion 0 de la lista de tuplas
		heads_ps = [0]*len(lista_ps)
		respuesta = []
		while cond:
			if(self.sameDocID(lista_ps,heads_ps)):
				if(self.consecutive(lista_ps,heads_ps)):
					respuesta.append(lista_ps[0][heads_ps[0]][0])
				self.advanceAll(heads_ps)
			else:
				self.advanceMin(lista_ps,heads_ps)
			cond = self.allNoNIL(lista_ps,heads_ps)
		return respuesta

	# Dada una consulta recupera del indice las listas de apariciones de cada palabra y 
	# llama a la funcion intersect_phrase para obtener los documentos en las que aparezcan
	# todas las palabras de la consulta de forma consecutiva
	def consulta_frase(self, frase):
		palabras_consulta = extrae_palabras(frase)
		lista_ps = []
		result = []


		for palabra in palabras_consulta:
			if palabra not in self.indice_completo:
				return []
			else:
				lista_ps.append(self.indice_completo[palabra])

		ids_docs = self.intersect_phrase(lista_ps)

		for ndoc in ids_docs:
			result.append(self.doc_ruta[ndoc])

		return result
	
	# Devuelve el numero de bits que ocupan todas las listas de desplazamientos almacenados
	# en el indice completo
	def num_bits(self):
		nbits = 0
		for palabra,lista_docs in self.indice_completo.items():
			for doc,apariciones in lista_docs:
				if self.encode == None:
					nbits += len(apariciones)*32
				else:
					nbits += len(apariciones)
		return nbits

cNone = CompleteIndex('docs')
print('cNone: '+str(cNone.num_bits())+' bits')
print(cNone.consulta_frase("Duo Dock"))
print(cNone.consulta_frase("Health Service"))


#cUnary = C.CompleteIndex('20news-18828',compresion='unary')
#print('cUnary: '+str(cUnary.num_bits())+' bits')
#print(cUnary.consulta_frase("from the Freedom From Religion"))

#cVBytes = C.CompleteIndex('20news-18828',compresion='variable-bytes')
#print('cVBytes: '+str(cVBytes.num_bits())+' bits')
#print(cVBytes.consulta_frase("from the Freedom From Religion"))

#cGamma = C.CompleteIndex('20news-18828',compresion='elias-gamma')
#print('cGamma: '+str(cGamma.num_bits())+' bits')
#print(cGamma.consulta_frase("from the Freedom From Religion"))

#cDelta = C.CompleteIndex('20news-18828',compresion='elias-delta')
#print('cDelta: '+str(cDelta.num_bits())+' bits')
#print(cDelta.consulta_frase("from the Freedom From Religion"))