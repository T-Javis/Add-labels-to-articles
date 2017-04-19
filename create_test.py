# -*- coding: utf-8 -*-
#!/usr/bin/python

import os
import logging
import sys
import re
from base import BaseModel
import json
from bs4 import BeautifulSoup
from gensim import utils
from simserver import SessionServer
import nltk
import jieba
import MySQLdb
import threading
import time

def dal():
	return BaseModel()
db = dal().db

def cut(text):  #--结巴分词 去除停用词
	text2=list(jieba.cut(text, cut_all = False))
	text3=stop(text2)
	return text3

def translate_synonymes(text):  #--同义词转换
	for row in synonymes:
		text = text.replace(row['word'], row['normal'])
	return text

def stop(text):
	textstop=[]
	for x in text:
		if x not in stopwords:
			textstop.append(x)
	return textstop


prog_path = os.path.abspath(os.path.dirname(__file__))
stopwords = list(set([line.rstrip().decode('utf-8') for line in open(os.path.join(os.path.dirname(__file__), 'stopwords.txt'))])) #--停用词 对停用词处理
stopwords+=['\n',' ','\r','\t','','\xe2\x80\x8b','\r\n']
jieba.load_userdict('/home/label/label.txt');


logging.basicConfig(level=logging.INFO,   #--设置日志等级 CRITICAL > ERROR > WARNING > INFO > DEBUG > NOTSET
		format='%(asctime)s %(filename)s:%(lineno)d %(levelname)s %(message)s',
		filename=os.path.join(prog_path, 'l2.log'),
		filemode='w')
logger = logging.getLogger('mylogger')


#pgresql
synonymes = db.query('select * from synonyme')
label_meal_db=db.query('select * from label_meal')
label_wear_db=db.query('select * from label_wear')
label_dic=[]

#mysql
con = MySQLdb.connect(host="*.*.*.*", user="***", passwd="***",db="news_data",port=****,charset='utf8')
cur = con.cursor()
cur.execute('select * from grab_news')
mysql_db=cur.fetchall()
article_title_text_dic=[]

'''
for i in range(0,len(label_db)):
#for i in range(0,3):
	label_list=label_db[i]
	label_id=label_list['id']
	label=label_list['name']
	#label_translate_synonymes=translate_synonymes(label)
	label_translate_synonymes=label
	#label_dic.append({'id': 'doc_%i' % label_id, 'tokens': [label_translate_synonymes], 'payload': label_translate_synonymes})
	label_dic.append({'id': 'doc_%i' % label_id, 'tokens': cut(label_translate_synonymes), 'payload': label})
	logger.info(i)
	logger.info('label_id= %s' % label_id)


server_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'servers/create_test',)  #--model path
server = SessionServer(server_path)  
server.drop_index()  #--删除所有索引
utils.upload_chunked(server, label_dic, chunksize=1000) #--simserver分块处理
server.train(label_dic, method='lsi')  #--训练已处理后的问题
server.index(label_dic)  #--建立索引文件
#print(server.status())
print('---------finish------------')
'''


def with_synonyme_meal():
	for i in range(0,len(label_meal_db)):
		#for i in range(0,3):
		label_list=label_meal_db[i]
		label_id=label_list['id']
		label=label_list['name']
		label_translate_synonymes=translate_synonymes(label)
		#label_translate_synonymes=label
		#label_dic.append({'id': 'doc_%i' % label_id, 'tokens': [label_translate_synonymes], 'payload': label_translate_synonymes})
		label_dic.append({'id': 'doc_%i' % label_id, 'tokens': cut(label_translate_synonymes), 'payload': label})
		logger.info(i)
		logger.info('label_id= %s' % label_id)
	
	for j in range(0,len(mysql_db)):
		mysql_data_list=mysql_db[j]
		article_id=mysql_data_list[0]	#id
		article_label=mysql_data_list[1] #label
		article_title=mysql_data_list[2] #title
		article_text=mysql_data_list[4] #text
		if article_title==None:
			article_title=''
		if article_text==None:
			article_text=''
		article_title_text=article_title+article_text
		article_title_text_translate_synonymes=translate_synonymes(article_title_text)
		article_title_text_dic.append({'id': 'doc_%i' % article_id, 'tokens': cut(article_title_text_translate_synonymes), 'payload': article_title_text})

	server_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'servers/create_test_withsyn_meal',)  #--model path
	server = SessionServer(server_path)  
	server.drop_index()  #--删除所有索引
	utils.upload_chunked(server, label_dic, chunksize=1000) #--simserver分块处理
	server.train(article_title_text_dic, method='lsi')  #--训练已处理后的问题
	server.index(label_dic)  #--建立索引文件
	#print(server.status())
	return None


def without_synonyme_meal():
	for i in range(0,len(label_meal_db)):
		#for i in range(0,3):
		label_list=label_meal_db[i]
		label_id=label_list['id']
		label=label_list['name']
		#label_translate_synonymes=translate_synonymes(label)
		label_translate_synonymes=label
		#label_dic.append({'id': 'doc_%i' % label_id, 'tokens': [label_translate_synonymes], 'payload': label_translate_synonymes})
		label_dic.append({'id': 'doc_%i' % label_id, 'tokens': cut(label_translate_synonymes), 'payload': label})
		logger.info(i)
		logger.info('label_id= %s' % label_id)
	
	for j in range(0,len(mysql_db)):
			mysql_data_list=mysql_db[j]
			article_id=mysql_data_list[0]	#id
			article_label=mysql_data_list[1] #label
			article_title=mysql_data_list[2] #title
			article_text=mysql_data_list[4] #text
			if article_title==None:
				article_title=''
			if article_text==None:
				article_text=''
			article_title_text=article_title+article_text
			#article_title_text_translate_synonymes=translate_synonymes(article_title_text)
			article_title_text_translate_synonymes=article_title_text
			article_title_text_dic.append({'id': 'doc_%i' % article_id, 'tokens': cut(article_title_text_translate_synonymes), 'payload': article_title_text})

	server_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'servers/create_test_withoutsyn_meal',)  #--model path
	server = SessionServer(server_path)  
	server.drop_index()  #--删除所有索引
	utils.upload_chunked(server, label_dic, chunksize=1000) #--simserver分块处理
	server.train(article_title_text_dic, method='lsi')  #--训练已处理后的问题
	server.index(label_dic)  #--建立索引文件
	#print(server.status())
	return None


def with_synonyme_wear():
	for i in range(0,len(label_wear_db)):
		#for i in range(0,3):
		label_list=label_wear_db[i]
		label_id=label_list['id']
		label=label_list['name']
		label_translate_synonymes=translate_synonymes(label)
		#label_translate_synonymes=label
		#label_dic.append({'id': 'doc_%i' % label_id, 'tokens': [label_translate_synonymes], 'payload': label_translate_synonymes})
		label_dic.append({'id': 'doc_%i' % label_id, 'tokens': cut(label_translate_synonymes), 'payload': label})
		logger.info(i)
		logger.info('label_id= %s' % label_id)
	
	for j in range(0,len(mysql_db)):
		mysql_data_list=mysql_db[j]
		article_id=mysql_data_list[0]	#id
		article_label=mysql_data_list[1] #label
		article_title=mysql_data_list[2] #title
		article_text=mysql_data_list[4] #text
		if article_title==None:
			article_title=''
		if article_text==None:
			article_text=''
		article_title_text=article_title+article_text
		article_title_text_translate_synonymes=translate_synonymes(article_title_text)
		article_title_text_dic.append({'id': 'doc_%i' % article_id, 'tokens': cut(article_title_text_translate_synonymes), 'payload': article_title_text})

	server_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'servers/create_test_withsyn_wear',)  #--model path
	server = SessionServer(server_path)  
	server.drop_index()  #--删除所有索引
	utils.upload_chunked(server, label_dic, chunksize=1000) #--simserver分块处理
	server.train(article_title_text_dic, method='lsi')  #--训练已处理后的问题
	server.index(label_dic)  #--建立索引文件
	#print(server.status())
	return None


def without_synonyme_wear():
	for i in range(0,len(label_wear_db)):
		#for i in range(0,3):
		label_list=label_wear_db[i]
		label_id=label_list['id']
		label=label_list['name']
		#label_translate_synonymes=translate_synonymes(label)
		label_translate_synonymes=label
		#label_dic.append({'id': 'doc_%i' % label_id, 'tokens': [label_translate_synonymes], 'payload': label_translate_synonymes})
		label_dic.append({'id': 'doc_%i' % label_id, 'tokens': cut(label_translate_synonymes), 'payload': label})
		logger.info(i)
		logger.info('label_id= %s' % label_id)
	

	for j in range(0,len(mysql_db)):
		mysql_data_list=mysql_db[j]
		article_id=mysql_data_list[0]	#id
		article_label=mysql_data_list[1] #label
		article_title=mysql_data_list[2] #title
		article_text=mysql_data_list[4] #text
		if article_title==None:
			article_title=''
		if article_text==None:
			article_text=''
		article_title_text=article_title+article_text
		#article_title_text_translate_synonymes=translate_synonymes(article_title_text)
		article_title_text_translate_synonymes=article_title_text
		article_title_text_dic.append({'id': 'doc_%i' % article_id, 'tokens': cut(article_title_text_translate_synonymes), 'payload': article_title_text})

	server_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'servers/create_test_withoutsyn_wear',)  #--model path
	server = SessionServer(server_path)  
	server.drop_index()  #--删除所有索引
	utils.upload_chunked(server, label_dic, chunksize=1000) #--simserver分块处理
	server.train(article_title_text_dic, method='lsi')  #--训练已处理后的问题
	server.index(label_dic)  #--建立索引文件
	#print(server.status())
	return None


t1=threading.Thread(target=with_synonyme_meal)
t2=threading.Thread(target=without_synonyme_meal)
t3=threading.Thread(target=with_synonyme_wear)
t4=threading.Thread(target=without_synonyme_wear)
threads=[t1,t2,t3,t4]

if __name__=="__main__":
	print(time.localtime())
	'''
	with_synonyme_meal()
	without_synonyme_meal()
	with_synonyme_wear()
	without_synonyme_wear()
	'''
	for t in threads:
		t.start()
	t.join()
	
	print('---------finish------------')
	print(time.localtime())


