#!/usr/bin/env python
# coding: utf-8

# In[1]:


import math
import json


# In[2]:


import findspark
findspark.init()
import pyspark


# In[3]:


conf = pyspark.SparkConf().setAppName("App")
conf = (conf.setMaster('local[*]'))
sc = pyspark.SparkContext(conf=conf)


# ### read the file
# 

# In[5]:


file = sc.textFile('/home/mr_malviya/Desktop/Secret/Study/Spring_2019/DSP/P0/handout/data')


# ### finding the word counts and writing the output as json file

# In[9]:


rawcounts = file.flatMap(lambda x : x.split(' ')).map(lambda word : (word,1)).reduceByKey(lambda a,b : a+b)


# In[10]:


rawcounts = rawcounts.collectAsMap()


# In[11]:


rawcounts = json.dumps(rawcounts)


# In[12]:


f = open('/home/mr_malviya/Desktop/rawcounts.json','w')


# In[13]:


f.write(str(rawcounts))


# In[14]:


f.close()

