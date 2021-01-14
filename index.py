# Using flask to make an api 
# import necessary libraries and functions 
from flask import Flask, jsonify, request 

import os

from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *


# creating a Flask app 
app = Flask(__name__) 

# on the terminal type: curl http://127.0.0.1:5000/ 
# returns hello world when we use GET. 
# returns the data that we send when we use POST. 



class SparkData:

	def __init__(self):
		self._spark = SparkSession.builder\
    .master("local")\
    .appName("Data")\
    .getOrCreate()

		self._df1 = self._spark.read.csv("files/data1.csv", header=True, inferSchema=True)
		self._df2 = self._spark.read.csv("files/data2.csv", header=True, inferSchema=True)
		self._df3 = self._spark.read.csv("files/data3.csv", header=True, inferSchema=True)
		self._df4 = self._spark.read.csv("files/data4.csv", header=True, inferSchema=True)
		self._df5 = self._spark.read.csv("files/data5.csv", header=True, inferSchema=True)
		self._df6 = self._spark.read.csv("files/data5.csv", header=True, inferSchema=True)
		self._df = self._df1.union(self._df2).union(self._df3).union(self._df4).union(self._df5).union(self._df6)

	@property
	def getDataFrame(self):
		return self._df

	@property
	def getGoodStudentRegion(self):
		return self._df.filter(self._df.ects>=60).groupBy("ville").agg(count("ects").alias("count_ects")).sort(col("count_ects").desc()).toJSON().take(100)

	@property
	def getGoodStudentEtablissement(self):
		return self._df.filter(self._df.ects>=60).groupBy("universite").agg(count("ects").alias("count_ects")).sort(col("count_ects").desc()).toJSON().take(100)

	@property
	def getStopStudy(self):
		return self._df.select("nom","prenom","etude").filter(self._df.etude=="stop").show()

	@property
	def getMoreStudentRegion(self):
		return self._df.groupBy("ville").agg(\
				count("nom").alias("nombre_etudiant"),\
				count(when(self._df.entreprise!="",0)).alias("nombre_etudiant_travaillant"),\
				avg("duree_contrat").alias("nombre_jour_travail")\
			).sort(col("nombre_etudiant").desc()).show()

	@property
	def getMeanEmbauche(self):
		return self._df.filter("entreprise!=''").groupBy("universite").agg(\
    	avg("duree_contrat").alias("nombre_jour_travail"),\
    	count("nom").alias("nombre etudiant embauché")\
		).sort(col("nombre_jour_travail").desc()).show()

	@property
	def getMoreAlternance(self):
		return self._df.filter("type_contrat=='Alternance'").groupBy("ville").agg(\
	    count("nom").alias("nombre etudiant embauché")\
		).sort(col("nombre etudiant embauché").desc()).show()


def init():
	os.system('cmd /c "curl -H "X-API-Key: 03207a10" https://my.api.mockaroo.com/data.csv > "files/data1.csv"')
	os.system('cmd /c "curl -H "X-API-Key: 03207a10" https://my.api.mockaroo.com/data.csv > "files/data2.csv"')
	os.system('cmd /c "curl -H "X-API-Key: 03207a10" https://my.api.mockaroo.com/data.csv > "files/data3.csv"')
	os.system('cmd /c "curl -H "X-API-Key: 03207a10" https://my.api.mockaroo.com/data.csv > "files/data4.csv"')
	os.system('cmd /c "curl -H "X-API-Key: 03207a10" https://my.api.mockaroo.com/data.csv > "files/data5.csv"')
	os.system('cmd /c "curl -H "X-API-Key: 03207a10" https://my.api.mockaroo.com/data.csv > "files/data6.csv"')
	print("ok")


def home():
	return jsonify( "Bonjour a tous" ) 


def get_good_student():
	return jsonify( {"region":sparkproj.getGoodStudentRegion,"etablissement": sparkproj.getGoodStudentEtablissement} ) 

def get_stop_student():
	return jsonify( sparkproj.getStopStudy ) 

def get_more_student():
	return jsonify( sparkproj.getMoreStudentRegion ) 

def get_mean_embauche():
	return jsonify( sparkproj.getMeanEmbauche ) 

def get_more_alternance():
	return jsonify( sparkproj.getMoreAlternance ) 


# driver function 
if __name__ == '__main__':
	init()

	sparkproj = SparkData()

	app = Flask(__name__)
	
	app.add_url_rule('/', 'home', home)
	app.add_url_rule('/good_student', 'get_good_student', get_good_student)
	app.add_url_rule('/stop_student', 'get_stop_student', get_stop_student)
	app.add_url_rule('/more_student', 'get_more_student', get_more_student)
	app.add_url_rule('/mean_embauche', 'get_mean_embauche', get_mean_embauche)
	app.add_url_rule('/more_alternance', 'get_more_alternance', get_more_alternance)

	
	app.run(debug = True) 
