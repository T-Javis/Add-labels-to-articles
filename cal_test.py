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
import math
import numpy
import codecs
import pandas
import threading
import time
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



'''
article_db=db.query('select * from article_all')
label_num_total_list=[]
for i in range(0,len(article_db)):
	article_list=article_db[i]
	article_id=article_list['id']	
	label_update_json=article_list['label_update_json']
	label_update_json=json.loads(label_update_json)
	label_num_total_list.append(len(label_update_json))
	print(article_id)


label_num_total=sum(label_num_total_list)
print('article_all=%s'% label_num_total)
'''


def article_1():
	article_db=db.query('select * from article_all1')

	label_num_total_list=[]
	for i in range(0,len(article_db)):
		article_list=article_db[i]
		article_id=article_list['id']	
		label_update_json=article_list['label_update_json']
		label_update_json=json.loads(label_update_json)
		label_num_total_list.append(len(label_update_json))
		#print(article_id)


	label_num_total=sum(label_num_total_list)
	print('article_all1=%s'% label_num_total)
	return None


def article_2():
	article_db=db.query('select * from article_all')

	label_num_total_list=[]
	for i in range(0,len(article_db)):
		article_list=article_db[i]
		article_id=article_list['id']	
		label_update_json=article_list['label_update_json']
		label_update_json=json.loads(label_update_json)
		label_num_total_list.append(len(label_update_json))
		#print(article_id)


	label_num_total=sum(label_num_total_list)
	print('article_all2=%s'% label_num_total)
	return None


def find_different():
	article_db1=db.query('select * from article_all1')
		
	for i in range(0,len(article_db1)):
	#for i in range(0,1):
		label1=[]
		label2=[]
		label3=[]
		article_list1=article_db1[i]
		article_id1=article_list1['id']
		label_update_json1=article_list1['label_update_json']
		label_update_json1=json.loads(label_update_json1)
		#print(label_update_json1)
		print(article_id1)
		for x in label_update_json1:
			label1.append(x[0])
			#print()
		#print(label1)
		#print(article_id1)
		article_db=db.query('select * from article_all where id=%s',(article_id1,))
		#article_db=db.query('select * from article_all where id=%s',(1,))
		
		article_list=article_db[0]
		article_id=article_list['id']
		label_update_json=article_list['label_update_json']
		label_update_json=json.loads(label_update_json)
		title=article_list['title']
		introduce=article_list['introduce']
		content=article_list['content']
		js_content=json.loads(content)
		content_all=''
		for at in range(0,len(js_content)):
			js_content_list=js_content[at]
			js_content_content=js_content_list['content']
			js_content_title=js_content_list['title']
			soup_js_content_title=BeautifulSoup(js_content_title)
			soup_js_content_content=BeautifulSoup(js_content_content)
			soup_title=soup_js_content_title.get_text()
			soup_content=soup_js_content_content.get_text()
			content_all=content_all+soup_title+'.'+soup_content
			content_all=content_all.replace("\n", "")
		article=title+'.'+introduce+'.'+content_all

		for y in label_update_json:
			y2=y[0]
			if y2 not in label1:
				label2.append(y2)
			if y2 not in label1 and y2 in article:
				label3.append('1')
			elif y2 not in label1 and y2 not in article:
				label3.append('2')

		print(label2)
		#print(type(label2))
		for xx in label2:
			print(xx)
		db.execute('update article_all1 set different=%s where id=%s',(label2,article_id))
		db.execute('update article_all1 set different2=%s where id=%s',(label3,article_id))
		
		label2_json=json.dumps(label2)
		db.execute('update article_all1 set different_json=%s where id=%s',(label2_json,article_id))


		print('-'*20)	

	return None

def cal_different():
	article_db=db.query('select * from article_all1')
	c1=0
	c2=0
	for i in range(0,len(article_db)):
		article_list=article_db[i]
		article_id=article_list['id']	
		#label_update_json=article_list['label_update_json']
		different2=article_list['different2']
		
		for d in different2:
			if d=='1':
				c1=c1+1
			if d=='2':
				c2=c2+1
	
	print('in=%s'%c1)
	print('out=%s'%c2)
	print('in=%s%%'%((float(c1)/float(c1+c2))*100))
	print('out=%s%%'%((float(c2)/float(c1+c2))*100))
	
	return None


def combine():
	article_db=db.query('select * from article_all1')
	for i in range(0,len(article_db)):
		article_list=article_db[i]
		article_id=article_list['id']
		label_update_json=article_list['label_update_json']
		different_json=article_list['different_json']
		label_update=json.loads(label_update_json)
		different=json.loads(different_json)
		
		title=article_list['title']
		introduce=article_list['introduce']
		content=article_list['content']
		js_content=json.loads(content)
		content_all=''
		for at in range(0,len(js_content)):
			js_content_list=js_content[at]
			js_content_content=js_content_list['content']
			js_content_title=js_content_list['title']
			soup_js_content_title=BeautifulSoup(js_content_title)
			soup_js_content_content=BeautifulSoup(js_content_content)
			soup_title=soup_js_content_title.get_text()
			soup_content=soup_js_content_content.get_text()
			content_all=content_all+soup_title+'.'+soup_content
			content_all=content_all.replace("\n", "")
		article=title+'.'+introduce+'.'+content_all

		label1=[]
		for x in label_update:
			label1.append(x[0])
		for y in different:
			if y in article:
				label1.append(y)
		
		label1=list(set(label1))

		db.execute('update article_all1 set label_update2=%s where id=%s',(label1,article_id))		

		label1_json=json.dumps(label1)
		db.execute('update article_all1 set label_update2_json=%s where id=%s',(label1_json,article_id))

		print(article_id)
		#print(label_update)
		#print(label1)
		#print(article)






	return None
	








if __name__=="__main__":

	#find_different()
	#article_1()
	#article_2()
	#cal_different()
	combine()










