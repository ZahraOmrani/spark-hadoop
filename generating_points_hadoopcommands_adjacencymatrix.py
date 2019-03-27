import numpy as np
import matplotlib.pyplot as plt
import numpy as np
import random
from numpy import genfromtxt
from numpy import linalg as LA
import csv
import pandas as pd
len_points=[]
list_num_of_clusters=[]
for i in range(2000, 3900, 100): #i is the number of points in each cluster 
    data = []
    centroid=[]
    list_X_data=[]
    list_Y_data=[]
    
    for c in range(10, 200, 50): #c is the number of clusters
        list_X_data=[]
        list_Y_data=[]
        
        temp = i
        s = 0
        k_list_complete = []
        b=0
        label=[]
        list_num_of_clusters.append(c)
        cluster=c
       
        for k in range(0, c):
           
            num_of_data_in_cluster=abs(int(np.random.randn()*2+i/c))
            while num_of_data_in_cluster>s:
                num_of_data_in_cluster=abs(int(np.random.randn()*2+i/c))
                break
            s = s + num_of_data_in_cluster
            if k==c-1 and i<=s:
                num_of_data_in_cluster =0
                break
            elif k==c-1 and i>=s:
                num_of_data_in_cluster = i - s
                print(num_of_data_in_cluster)
                break
            else:
            print(num_of_data_in_cluster)
                
            
            
              
            for j in range(0, num_of_data_in_cluster):
                label.append(b)    
            centroid_x = int(np.random.rand()*1000)
            centroid.append(centroid_x)
            centroid_y = int(np.random.rand()*1000)
            centroid.append(centroid_y)
            #variance is always a number between 1 and 3 so clusters will be denser
            variance = np.random.rand() * 3 



            temp = i - s
            
            b=b+1

            X_data = np.around(np.random.randn(num_of_data_in_cluster),decimals=5)* variance + centroid_x
            Y_data = np.around(np.random.randn(num_of_data_in_cluster),decimals=5)* variance + centroid_y
			
            list_X_data.extend(X_data)
            list_Y_data.extend(Y_data)
            if temp<=0:
                break
        #A is the Adjacency matrix of each group of points 
        A=np.zeros((len(list_X_data),len(list_X_data)), dtype=int)

        for o in range(0,len(list_X_data)):
            for c in range(0,len(list_X_data)):
                if(label[o]==label[c]):
                    A[o][c]=1
        #generating all the commands that are needed to run in hadoop and writing them in commands2.sh
        file = open('/home/csgrpup/Desktop/commands2.sh','+a')
        file.write('hadoop fs -copyFromLocal /home/csgrpup/Desktop/my_random_points/randompoints'+str(len(list_X_data))+'cluster'+str(cluster)+'.txt /test/\nhadoop jar /home/csgrpup/Desktop/kmeans_hadoop.jar io.github.mameli.KMeans /test/randompoints'+str(len(list_X_data))+'cluster'+str(cluster)+'.txt'+ ' /test/out_kmeans_randompoints'+str(len(list_X_data))+'cluster'+str(cluster)+'/ ' +str(cluster)+' 2 0.00005\nhadoop fs -cat /test/out_kmeans_randompoints'+str(len(list_X_data))+'cluster'+str(cluster)+'/*\nhadoop fs -copyToLocal /test/out_kmeans_randompoints'+str(len(list_X_data))+'cluster'+str(cluster)+ ' /home/hduser/out_hadoop1/\nhadoop fs -rm /test/randompoints'+str(len(list_X_data))+'cluster'+str(c)+'.txt/\nhadoop fs -rm /test/out_kmeans_randompoints'+str(len(list_X_data))+'cluster'+str(cluster)+'/*\n')
        file.close()
        
        np.savetxt('/home/csgrpup/Desktop/my_random_points/matrix'+str(len(list_X_data))+'cluster'+str(cluster)+'.txt', A, fmt='%d')

        f=open('/home/csgrpup/Desktop/my_random_points/randompoints'+str(len(list_X_data))+'cluster'+str(cluster)+'.txt',"w")
        for e in range(len(list_X_data)):
            f.write("%f;%f;\r\n"%(list_X_data[e],list_Y_data[e]))
        f.close()
    
        len_points.append(len(list_X_data))
        print("len_X"+str(len(list_X_data)))
        print("len_Y"+str(len(list_Y_data)))
        print("len_label"+str(len(label)))
  
       
        plt.scatter(list_X_data,list_Y_data)
        plt.show() 
    
print(len_points)
print(list_num_of_clusters)    
        