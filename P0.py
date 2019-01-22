#!/usr/bin/env python
# coding: utf-8

# In[34]:


import math
import json
import string
import operator


# In[35]:


import findspark
findspark.init()
import pyspark


# In[36]:


conf = pyspark.SparkConf().setAppName("App")
conf = (conf.setMaster('local[*]'))
sc = pyspark.SparkContext(conf=conf)


# ## Reading the file

# In[37]:


file = sc.textFile('/home/mr_malviya/Desktop/Secret/Study/Spring_2019/DSP/P0/handout/data')


# In[38]:


file.count()


# In[39]:


stopwords = sc.textFile('/home/mr_malviya/Desktop/Secret/Study/Spring_2019/DSP/P0/handout/stopwords.txt')
stopwords = stopwords.collect()


# ## Finding the word count and writing the output as json

# In[40]:


rawcounts = file.flatMap(lambda x : x.split(' ')).map(lambda x : x.lower()).map(lambda word : (word,1)).reduceByKey(lambda a,b : a+b)


# In[41]:


rawcounts = rawcounts.collectAsMap()


# In[42]:


rawcounts = json.dumps(rawcounts)


# In[43]:


f = open('/home/mr_malviya/Desktop/rawcounts.json','w')


# In[44]:


f.write(str(rawcounts))


# In[45]:


f.close()


# ## Find the top 40 words across all documents

# In[46]:


spA = file.flatMap(lambda x : x.split(' ')).map(lambda x : x.lower()).map(lambda x : (x,1)).reduceByKey(lambda a,b : a + b)


# In[47]:


spA = spA.filter(lambda x : x[1] > 1)
spA.take(5)
type(spA)


# In[48]:


spA = spA.sortBy(lambda a : a[1], False)
spA = spA.filter(lambda x : x[0] != '')


# In[49]:


spA = spA.top(40, key = lambda x : x[1])
spA = dict(spA)


# In[55]:


spA = json.dumps(spA, indent = 4)


# In[56]:


f= open('/home/mr_malviya/Desktop/sp1.json','w+')


# In[58]:


f.write(spA)


# In[59]:


f.close()


# ### implementing stopwords

# In[60]:


file = sc.textFile('/home/mr_malviya/Desktop/Secret/Study/Spring_2019/DSP/P0/handout/data')


# In[61]:


spB = file.flatMap(lambda x : x.split(' ')).map(lambda x : x.lower()).filter(lambda x : x not in stopwords).map(lambda x : (x,1)).reduceByKey(lambda a,b : a+b)


# In[62]:


spB = spB.sortBy(lambda a : a[1], False)
spB = spB.filter(lambda x : x[0] != '')


spB.take(5)


# In[63]:


spB = spB.top(40, key = lambda x : x[1])
spB = dict(spB)


# In[64]:


spB = json.dumps(spB, indent = 4)


# In[65]:


f = open('/home/mr_malviya/Desktop/sp2.json','w')


# In[66]:


f.write(str(spB))


# In[67]:


f.close()


# ### Removing punctuations from start or end of the words

# In[375]:


spC =sc.textFile('/home/mr_malviya/Desktop/Secret/Study/Spring_2019/DSP/P0/handout/data')


# In[376]:


spC = spC.flatMap(lambda x : x.split(' '))
spC = spC.filter(lambda x : len(x) != 1) 
spC.take(5)


# In[377]:


def removepunct(x):
        x = x.strip(".,:;'!?")
        return x


# In[378]:


spC = spC.map(lambda x : removepunct(x))
spC.take(5)


# In[379]:


spC = spC.map(lambda x : x.lower()).filter(lambda x : x not in stopwords).map(lambda x : (x,1)).reduceByKey(lambda a,b : a + b)


# In[380]:


spC = spC.filter(lambda x : x[0] != '')


# In[381]:


spC = spC.sortBy(lambda a : a[1], False)


# In[382]:


spC.take(5)


# In[383]:


spC = spC.top(40, key = lambda x : x[1])
spC = dict(spC)


# In[384]:


spC = json.dumps(spC, indent = 4)


# In[385]:


f = open('/home/mr_malviya/Desktop/sp3.json','w')


# In[386]:


f.write(str(spC))


# In[387]:


f.close()


# ## TF IDF 

# In[388]:


spD =sc.wholeTextFiles('/home/mr_malviya/Desktop/Secret/Study/Spring_2019/DSP/P0/handout/data')


# In[389]:


spD = spD.map(lambda x : (x[0].split('/')[-1],x[1])).map(lambda x : (x[0], x[1].split(' ')))


# In[392]:


def tolower(x):
    ls = []
    for word in x:
        ls.append(word.lower())
    return ls


# In[393]:


spD = spD.map(lambda x : (x[0], tolower(x[1])))


# In[394]:


def removepunctuation(x):
    ls = []
    for word in x:
        ls.append(word.strip(".,:;'!?"))
    return ls


# In[395]:


spD = spD.map(lambda x : (x[0],removepunctuation(x[1])))


# In[396]:


def rmsingleword(x):
    ls = []
    for word in x:
        if(len(word) != 1):
            ls.append(word)
    return ls
    


# In[397]:


spD = spD.map(lambda x : (x[0], rmsingleword(x[1])))


# In[398]:


def removestopwords(x):
    ls = []
    for word in x:
        if word not in stopwords:
            ls.append(word)
    return ls


# In[399]:


spD = spD.map(lambda x : (x[0],removestopwords(x[1])))


# In[400]:


def initializewords(x):
    ls = []
    for word in x:
        if(word != ''):
            ls.append((word,1))
    return ls


# In[401]:


spD = spD.map(lambda x : (x[0], initializewords(x[1])))


# In[402]:


def wordcount(x):
    d = {}
    for tuples in x:
        if tuples[0] not in d:
            d[tuples[0]] = 1
        else:
            d[tuples[0]]+= 1
    return d


# In[403]:


spD = spD.map(lambda x : (x[0],wordcount(x[1])))


# In[404]:


spD.take(1)


# In[405]:


def termfrequency(x):
    totalcount = 0
    for count in x.values():
        totalcount+= count
    d= {}
    for k,v in x.items():
        d[k] = v/totalcount
    return d


# In[406]:


tf = spD.map(lambda x : (x[0],termfrequency(x[1])))


# In[407]:


tf.take(1)


# In[408]:


numdocuments = tf.count()


# In[409]:


tf.map(lambda x : list (x[1]))


# ## Calculating IDF

# In[410]:


idf = tf.map(lambda x : list(x[1])).flatMap(lambda x : x).map(lambda x : (x,1)).reduceByKey(lambda a,b : a+b).map(lambda x : (x[0], math.log(numdocuments/x[1])))


# In[411]:


idf.take(5)


# In[412]:


broadtfidf = sc.broadcast(idf.collectAsMap())


# In[413]:


broadtfidf.value


# In[414]:


def calctfidf(x):
    d = {}
    for k,v in x.items():
        d[k] = v*broadtfidf.value[k]
        
    return d


# In[415]:


tfidf = tf.map(lambda x : (x[0], calctfidf(x[1])))


# In[416]:


tfidf.take(1)


# In[417]:


sortedrdd = tfidf.map(lambda x : (x[0], sorted(x[1].items(), key = operator.itemgetter(1), reverse = True)))


# In[418]:


sortedrdd.count()
type(sortedrdd)


# In[419]:


finaloutput = sortedrdd.values()


# In[420]:



finaloutput = finaloutput.map(lambda x : x[0:5])
finaloutput.count()
finaloutput = finaloutput.collect()
finalresult = dict()
print(finaloutput[0][4])
for i in range(0,8):
    for j in range(0,5):
       finalresult[finaloutput[i][j][0]] = finaloutput[i][j][1]


# In[422]:


finalresult = json.dumps(finalresult, indent = 4)


# In[423]:


f = open('/home/mr_malviya/Desktop/sp4.json','w')


# In[424]:


f.write(str(finalresult))


# In[425]:


f.close()

