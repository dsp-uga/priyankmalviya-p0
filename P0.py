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


# ## Reading the file

# In[4]:


file = sc.textFile('/home/mr_malviya/Desktop/Secret/Study/Spring_2019/DSP/P0/handout/data')


# In[5]:


file.count()


# ## Finding the word count and writing the output as json

# In[49]:


rawcounts = file.flatMap(lambda x : x.split(' ')).map(lambda x : x.lower()).map(lambda word : (word,1)).reduceByKey(lambda a,b : a+b)


# In[50]:


rawcounts = rawcounts.collectAsMap()


# In[51]:


rawcounts = json.dumps(rawcounts)


# In[52]:


f = open('/home/mr_malviya/Desktop/rawcounts.json','w')


# In[53]:


f.write(str(rawcounts))


# In[54]:


f.close()


# ## Find the top 40 words across all documents

# In[60]:


spA = file.flatMap(lambda x : x.split(' ')).map(lambda x : x.lower()).map(lambda x : (x,1)).reduceByKey(lambda a,b : a + b)


# In[61]:


spA = spA.filter(lambda x : x[1] > 1)
spA.take(5)


# In[62]:


spA = spA.sortBy(lambda a : a[1], False)


# In[63]:


spA = spA.top(40, key = lambda x : x[1])


# In[64]:


spA = json.dumps(spA)


# In[65]:


f = open('/home/mr_malviya/Desktop/spA.json','w')


# In[66]:


f.write(str(spA))


# In[67]:


f.close()


# ### stopwords

# In[32]:


stopwords = sc.textFile('/home/mr_malviya/Desktop/Secret/Study/Spring_2019/DSP/P0/handout/stopwords.txt')
stopwords = stopwords.collect()


# In[21]:


file = sc.textFile('/home/mr_malviya/Desktop/Secret/Study/Spring_2019/DSP/P0/handout/data')


# In[45]:


spB = file.flatMap(lambda x : x.split(' ')).filter(lambda x : x.lower() not in stopwords).map(lambda x : (x,1)).reduceByKey(lambda a,b : a+b)


# In[46]:


spB = spB.sortBy(lambda a : a[1], False)


# In[47]:


spB = spB.top(40, key = lambda x : x[1])


# In[55]:


spB = json.dumps(spB)


# In[57]:


f = open('/home/mr_malviya/Desktop/sp2.json','w')


# In[58]:


f.write(str(spB))


# In[59]:


f.close()

