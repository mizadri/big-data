'''
    # Asignatura: Sistemas de Gestion de Datos y de la Informacion
    # Practica 1 (MapReduce y ApacheSpark)
    # Grupo 10 (Adrian Garcia y Frank Julca)
    # Declaracion de integridad: Adrian Garcia y Frank Julca
    declaramos que esta solucion es fruto exclusivamente de nuestro trabajo personal.
    No hemos sido ayudados por ninguna otra persona ni hemos obtenido la solucion de fuentes externas,
    y tampoco hemos compartido nuestra solucion con nadie. Declaramos ademas que no hemos realizado de manera
    deshonesta ninguna otra actividad que pueda mejorar nuestros resultados ni perjudicar los resultados de los demas.
'''

from mrjob.job import MRJob

class MRWeather(MRJob):

    # Fase MAP (line es una cadena de texto) emite la fecha como clave y la bateria como valor.
    def mapper(self, key, line):
        words = line.split(',')
        
        # Evitamos la cabecera
        if words[0] != 'date-time':
            date, hour = words[0].split()
            
            # Obtenemos la fecha con el formato MM/AAAA
            formatDate = date.split("/")[1] + "/" + date.split("/")[0]
            yield formatDate, float(words[8])

    # Fase REDUCE (key es una cadena texto con la fecha, values un generador de parejas (fecha, bateria))
    # emite como clave fecha MM/AAAA en forma de cadena y como valor un diccionario con la maxima,
    # minima y media de valores de las baterias de esa fecha
    def reducer(self, key, values):
        
        # Transformamos el generador en una lista y devolvemos un diccionario con los resultados
        valuesList = list(values)
        battery = {}
        battery['max'] = max(valuesList)
        avg = sum(valuesList) / len(valuesList)
        battery['avg'] = round(avg,2)
        battery['min'] = min(valuesList)
        
        yield key, battery

if __name__ == '__main__':
    MRWeather.run()
