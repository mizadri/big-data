/*
	# Asignatura: Sistemas de Gestion de Datos y de la Informacion
	# Practica 3 (MongoDB)
	# Grupo 10 (Adrian Garcia y Frank Julca)
	# Declaracion de integridad: Adrian Garcia y Frank Julca
	declaramos que esta solución es fruto exclusivamente de nuestro trabajo personal.
	No hemos sido ayudados por ninguna otra persona ni hemos obtenido la solucion de fuentes externas,
	y tampoco hemos compartido nuestra solucion con nadie. Declaramos ademas que no hemos realizado de manera
	deshonesta ninguna otra actividad que pueda mejorar nuestros resultados ni perjudicar los resultados de los demas.
*/

// Desde termianl de mongo: 
// load("/home/mizadri/Documents/sgdi/pr3/consultas1.js")

/* AGGREGATION PIPELINE */
// 1.- Paises y numero de peliculas, ordenado por numero de peliculas descendente (en empate por nombre pais ascendente)
function agg1(){
	return db.peliculas.aggregate(
		[
			{ $unwind : "$pais" }, 
			{ $group: { _id : "$pais", peliculas : { $sum : 1 } } }, 
			{ $sort: { peliculas: -1, _id : 1 } }
		]
	)
}

// 2.- Listado de los 3 tipos de película más populares en 'Emiratos Árabes Unidos' (en caso de
// empate, romper por nombre de película ascendente)
function agg2(){
	return db.usuarios.aggregate(
		[
			{ $match : { "direccion.pais" : "Emiratos Árabes Unidos" } },
			{ $unwind : "$gustos" },
			{ $group : { _id: "$gustos", usuarios : { $sum : 1 } } },
			{ $sort: { usuarios : -1, _id : 1 } },
			{ $limit: 3 }
		]
	)
}

// 3.- Listado (Pais, edad minima-maxima-media) de mayores de 17 años, contando
// únicamente paises con mas de un usuario mayor de 17 años.
function agg3(){
	return db.usuarios.aggregate(
		{"$match":{"edad":{"$gt":17}}},
		{"$group":{"_id":"$direccion.pais","count":{"$sum":1},"edad-min":{"$min":"$edad"},"edad-max":{"$max":"$edad"},"edad-media":{"$avg":"$edad"}}},
		{"$match":{"count":{"$gt":1}}})
}
// Antes teniamos una fase mas que elimina count ya que no es solicitado:
//{"$project":{"count":0}
// En la version de uno de nosotros (3.4) funciona perfectamente.
// Sin embargo, en la version 3.2 de mongo esta fase de proyeccion causa una excepcion. 

// 4.- Listado (Titulo pelicula, numero de visualizaciones) de las 10 peliculas 
// más vistas (en caso de empate, romperlo por titulo ascendente)
function agg4(){
	return db.usuarios.aggregate(
		{"$unwind":"$visualizaciones"},
		{"$group":{"_id":"$visualizaciones.titulo","vista":{"$sum":1}}},
		{"$sort":{"vista":-1,"_id":1}},{"$limit":10});
}




/* MAPREDUCE */
// 1.- Paises y numero de peliculas
function mr1(){

	var map1=function(){
		for (var i = 0; i < this.pais.length; i++){
			emit(this.pais[i], 1); 
		}
	}

	var reduce1=function(key,values){
		return Array.sum(values);
	}

	return db.peliculas.mapReduce(map1, reduce1, {out: {inline : 1}})

}

// 2.- Tipo de pelicula y usuarios de 'Emiratos Árabes Unidos' a los que les gusta
function mr2(){

	var map2=function(){
		if(this.direccion.pais == "Emiratos Árabes Unidos"){
			for (var i = 0; i < this.gustos.length; i++){
				emit(this.gustos[i], 1);
			}
		}
	}

	var reduce2=function(key,values){
		return Array.sum(values);
	}
	return db.usuarios.mapReduce(map2, reduce2, {out: {inline : 1}})
	//db.mr2.find().pretty();
}

// 3.- Pais - edad minima-maxima-media de mayores de 17 años
function mr3(){
	var mapF=function(){
		emit(this.direccion.pais, {"count":1,"sum":this.edad,"min":this.edad,"max":this.edad}); 
	};
	var reduceF=function(key,values){
		var reducedVal = {"count":0,"sum":0,"min":10000,"max":0};
		for (var i = 0; i < values.length; i++) {
			if(values[i].min<reducedVal.min){
				reducedVal.min = values[i].min;
			}
			if(values[i].max > reducedVal.max){
				reducedVal.max = values[i].max;
			}
			reducedVal.sum += values[i].sum;
			reducedVal.count += values[i].count;
		}
		return reducedVal;
	};
	var finalizeF=function(key, reducedVal){
		reducedVal.avg = reducedVal.sum/reducedVal.count;
		delete reducedVal.count;
		delete reducedVal.sum;
		return reducedVal;
	};
	return db.usuarios.mapReduce(mapF,reduceF,{query:{edad:{"$gt":17}},out: {inline : 1}, finalize:finalizeF});
	//db.mr3.find().pretty();
}

// 4.- Titulo de pelicula y numero de visualizaciones
function mr4(){
	var mapF=function(){
		for (var i = 0; i < this.visualizaciones.length; i++) {
			emit(this.visualizaciones[i].titulo,1);
		}
	};
	var reduceF=function(key,values){
		return Array.sum(values);
	};

	return db.usuarios.mapReduce(mapF,reduceF,{out: {inline : 1}});
	//db.mr4.find().pretty();
}