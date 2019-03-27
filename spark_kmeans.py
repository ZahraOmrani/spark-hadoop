import findspark
findspark.init('/home/csgrpup/spark-2.1.0-bin-hadoop2.7')
import pyspark
from pyspark.sql import SparkSession 
from pyspark.ml.feature import VectorAssembler
import numpy as np
from numpy import linalg as LA
import csv
from pyspark.ml.clustering import KMeans

spark= SparkSession.builder.appName('cluster').getOrCreate()

error=[]
list_num_of_clusters=[]
i=-1
while i <len(len_points)-1:
    for num_of_clusters in range(10, 200, 50):
        list_num_of_clusters.append(num_of_clusters)
        k=0
        label_hadoop=[]
        i=i+1
        A=np.zeros((len_points[i],len_points[i]), dtype=int)
        b = np.loadtxt("/home/csgrpup/Desktop/my_random_points/matrix"+str(len_points[i])+'cluster'+str(num_of_clusters)+".txt", dtype=int)

        dataset=spark.read.csv("/home/csgrpup/Desktop/my_random_points/randompoints"+str(len_points[i])+'cluster'+str(num_of_clusters)+".txt", sep=';',inferSchema=True)
        final_data=dataset.select('_c0','_c1')
        assembler=VectorAssembler(inputCols=final_data.columns, outputCol='features')
        data=assembler.transform(final_data)
        kmeans=KMeans(k=len_points[i])
        model=kmeans.fit(data)
        results=model.transform(data)
        label_list = [int(i.prediction) for i in results.select('prediction').collect()]

        for e in range(0,len_points[i]):
            for f in range(0,len_points[i]):
                if(label_list[e]==label_list[f]):
                    A[e][f]=1 
        error.append(LA.norm(abs(A-b)))
        print(LA.norm(abs(A-b)))
		
#in the next cell
with open('/home/csgrpup/Desktop/results_spark.csv', mode='w') as results_file:
    results_writer = csv.writer(results_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    results_writer.writerow(list_num_of_clusters)
    results_writer.writerow(len_points)
    results_writer.writerow(error)