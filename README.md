# priyankmalviya-p0

## Word count and tf-idf
Given a set of documents i.e 8 different books by different authors avaiable at https://www.gutenberg.org/

The repository contains the code for the project 0 tested on the set of documents mentioned above. 

Given a set of documents i.e 8 different books by different authors avaiable at https://www.gutenberg.org/

The repository contains the code for the project 0 tested on the set of documents mentioned above. 

First basic step of the project is to do the following :-
1.) Tokenize the input.
2.) Conduct basic text processing.
3.) Count up the remaining words.
4.) Serialize the word count as output to json file.

The project has four other subprojects as well :-

1.) Subproject A :
Generate a dictionary/hashmap of the top 40 words acroos all documents with the largest count.
Word count is case insensitive. The output json file contains list of 'key:value' pairs. Key is the word and value is the count
of the word.

2.) Subproject B :
Implement a list of stop words. Regenerate list of top 40 words dropping words in the stopword list. Count is case insensitive.
The output json file contains list of 'key:value' pairs. Key is the word and value is the count of the word.

3.) Subproject C :
The output till now might contain some special characters either at the end or the beginning of the words.
Remove the following special characters (,),(:),(;),(.),('),(!),(?) from the words. Then regenerate the list.
Catch in this is that words having special characters at the end or at the beginning should only be removed. Words like
'can't' or 'won't' should not be affected. 

4.) Subproject D :
Compute the TF-IDF score.
IDF = log N/n 
N = total number of documents
n = number of documents specific term appears in which in our case could range from 1 to 8.
Output contains top 5 words from each document based on their TF-IDF score.

## Technology Used
Python 
Apache Spark


