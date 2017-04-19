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

prog_path = os.path.abspath(os.path.dirname(__file__))
servers_path = os.path.join(prog_path, 'servers') #--注意servers路径问题
#km_server=SessionServer(os.path.join(servers_path, 'label_1')) #--索引
stopwords = list(set([line.rstrip().decode('utf-8') for line in open(os.path.join(os.path.dirname(__file__), 'stopwords.txt'))]))#--停用词 对停用词处理
stopwords+=['\n',' ','\r','\t','','\xe2\x80\x8b','\r\n']
jieba.load_userdict('/home/label/label.txt');


logging.basicConfig(level=logging.INFO,   #--设置日志等级 CRITICAL > ERROR > WARNING > INFO > DEBUG > NOTSET
		format='%(asctime)s %(filename)s:%(lineno)d %(levelname)s %(message)s',
		filename=os.path.join(prog_path, 'l1.log'),
		filemode='w')
logger = logging.getLogger('mylogger')


def dal():
	return BaseModel()
db = dal().db

def cut(text):  #--结巴分词 去除停用词
	text2=list(jieba.cut(text, cut_all = False))
	text3=stop(text2)
	return text3

def stop(text):
	textstop=[]
	for x in text:
		if x not in stopwords:
			textstop.append(x)
	return textstop

def translate_synonymes(text):  #--同义词转换
	synonymes = db.query('select * from synonyme')
	for row in synonymes:
		text = text.replace(row['word'], row['normal'])
	return text

def add_label(article_synonymes,min_similarity,max_results,km_server): #add label
	doc={'tokens':cut(article_synonymes)}
	result=km_server.find_similar(doc, min_similarity, max_results)
	return result


def with_synoymes_meal():
	km_server=SessionServer(os.path.join(servers_path, 'create_test_withsyn_meal1')) #--索引
	article_db=db.query('select * from article_all1')

	min_similarity=0.1 #0.2
	max_results=5 #2
	#db.execute('update article_all1 set meal=null') #initial

	for i in range(0,len(article_db)):
	#for i in range(0,3):
		article_list=article_db[i]
		article_id=article_list['id']
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
		#print(article)
		article_synonymes=translate_synonymes(article)   #--数据库问题同义词转换
		article_label_list=add_label(article_synonymes,min_similarity,max_results,km_server)
		#print(article_id)
		#print(article_id,article_label_list)
		#print
		label_list_sql=[]
		label_list_sql_sim=[]
		for l in article_label_list:
			label_id=l[0][4:] 
			similarity=l[1]
			label=l[2]
			label_list_sql.append(label)
			label_list_sql_sim.append((similarity,label))
			label_list_sql_sim_json=json.dumps(label_list_sql_sim)
			#print(article_id,label_id,similarity)
			#print(article_id)
			#print(label_id)
			#db.execute('update article_all1 set meal=%s where id=%s',(label_list_sql,article_id))
			db.execute('update article_all1 set meal_sim=%s where id=%s',(label_list_sql_sim,article_id))
			db.execute('update article_all1 set meal_sim_json=%s where id=%s',(label_list_sql_sim_json,article_id))

		#print(label_list_sql)
		#print('-'*20)
	return None




def without_synoymes_meal():
	km_server=SessionServer(os.path.join(servers_path, 'create_test_withoutsyn_meal1')) #--索引
	article_db=db.query('select * from article_all1')

	min_similarity=0.1 #0.3
	max_results=5 #2
	#db.execute('update article_all1 set meal_temp=null') #initial

	for i in range(0,len(article_db)):
		#for i in range(0,3):
		article_list=article_db[i]
		article_id=article_list['id']
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
		#article_synonymes=translate_synonymes(article)   #--数据库问题同义词转换
		article_synonymes=article
		article_label_list=add_label(article_synonymes,min_similarity,max_results,km_server)
		#print(article_id)
		#print(article_id,article_label_list)
		#print
		label_list_sql=[]
		label_list_sql_sim=[]
		for l in article_label_list:
			label_id=l[0][4:] 
			similarity=l[1]
			label=l[2]
			label_list_sql.append(label)
			label_list_sql_sim.append((similarity,label))
			label_list_sql_sim_json=json.dumps(label_list_sql_sim)
			#print(article_id,label_id,similarity)
			#print(article_id)
			#print(label_id)
			#db.execute('update article_all1 set meal_temp=%s where id=%s',(label_list_sql,article_id))
			db.execute('update article_all1 set meal_temp_sim=%s where id=%s',(label_list_sql_sim,article_id))
			db.execute('update article_all1 set meal_temp_sim_json=%s where id=%s',(label_list_sql_sim_json,article_id))
		#print(label_list_sql)
		#print('-'*20)
	return None



def with_synoymes_wear():
	km_server=SessionServer(os.path.join(servers_path, 'create_test_withsyn_wear1')) #--索引
	article_db=db.query('select * from article_all1')

	min_similarity=0.1 #0.2
	max_results=5 #2
	#db.execute('update article_all1 set wear=null') #initial

	for i in range(0,len(article_db)):
	#for i in range(0,3):
		article_list=article_db[i]
		article_id=article_list['id']
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
		article_synonymes=translate_synonymes(article)   #--数据库问题同义词转换
		article_label_list=add_label(article_synonymes,min_similarity,max_results,km_server)
		#print(article_id)
		#print(article_id,article_label_list)
		#print
		label_list_sql=[]
		label_list_sql_sim=[]
		for l in article_label_list:
			label_id=l[0][4:] 
			similarity=l[1]
			label=l[2]
			label_list_sql.append(label)
			label_list_sql_sim.append((similarity,label))
			label_list_sql_sim_json=json.dumps(label_list_sql_sim)
			#print(article_id,label_id,similarity)
			#print(article_id)
			#print(label_id)
			#db.execute('update article_all1 set wear=%s where id=%s',(label_list_sql,article_id))
			db.execute('update article_all1 set wear_sim=%s where id=%s',(label_list_sql_sim,article_id))
			db.execute('update article_all1 set wear_sim_json=%s where id=%s',(label_list_sql_sim_json,article_id))
		#print(label_list_sql)
		#print('-'*20)
	return None




def without_synoymes_wear():
	km_server=SessionServer(os.path.join(servers_path, 'create_test_withoutsyn_wear1')) #--索引
	article_db=db.query('select * from article_all1')

	min_similarity=0.1 #0.3
	max_results=5 #2
	#db.execute('update article_all1 set wear_temp=null') #initial

	for i in range(0,len(article_db)):
		#for i in range(0,3):
		article_list=article_db[i]
		article_id=article_list['id']
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
		#article_synonymes=translate_synonymes(article)   #--数据库问题同义词转换
		article_synonymes=article
		article_label_list=add_label(article_synonymes,min_similarity,max_results,km_server)
		#print(article_id)
		#print(article_id,article_label_list)
		#print
		label_list_sql=[]
		label_list_sql_sim=[]
		for l in article_label_list:
			label_id=l[0][4:] 
			similarity=l[1]
			label=l[2]
			label_list_sql.append(label)
			label_list_sql_sim.append((similarity,label))
			label_list_sql_sim_json=json.dumps(label_list_sql_sim)
			#print(article_id,label_id,similarity)
			#print(article_id)
			#print(label_id)
			#db.execute('update article_all1 set wear_temp=%s where id=%s',(label_list_sql,article_id))
			db.execute('update article_all1 set wear_temp_sim=%s where id=%s',(label_list_sql_sim,article_id))
			db.execute('update article_all1 set wear_temp_sim_json=%s where id=%s',(label_list_sql_sim_json,article_id))
		#print(label_list_sql)
		#print('-'*20)
	return None

def label_extract(text):
	text=text.decode('utf-8')
	text2=text[1:-1]
	text3=text2.split(',')
	return text3

def reverse_json(text_json):
	text=json.loads(text_json)
	return text

'''
def combine():	
	test_db=db.query('select * from article_all1')
	for i in range(0,len(test_db)):
		#print(i)
		test_data=test_db[i]
		article_id=test_data['id']
		meal_label=test_data['meal']
		wear_label=test_data['wear']
		meal_temp_label=test_data['meal_temp']
		wear_temp_label=test_data['wear_temp']
		if meal_label==None:
			meal_label_list=[]
		else:
			meal_label_list=label_extract(meal_label)
		if wear_label==None:
			wear_label_list=[]
		else:
			wear_label_list=label_extract(wear_label)
		if meal_temp_label==None:
			meal_temp_label_list=[]
		else:
			meal_temp_label_list=label_extract(meal_temp_label)
		if wear_temp_label==None:
			wear_temp_label_list=[]
		else:
			wear_temp_label_list=label_extract(wear_temp_label)
		if meal_label_list==None:
			meal_label_list=meal_temp_label
		if wear_label_list==None:
			wear_label_list=wear_temp_label
		#print(set(meal_label_list+wear_label_list))
		if meal_label_list==None and wear_label_list==None:
			meal_wear=[]
		else:
			if type(wear_label_list)==unicode:
				wear_label_list=list(wear_label_list)
			if type(meal_label_list)==unicode:
				meal_label_list=list(meal_label_list)
			meal_wear=list(set(meal_label_list+wear_label_list))
			db.execute('update article_all1 set meal_wear=%s where id=%s',(meal_wear,article_id))
	return None
'''


def combine_meal_wear():  #combine meal and wear label
	article_db=db.query('select * from article_all1')

	for i in range(0,len(article_db)):
		#for i in range(0,3):
		#print(i)
		article_list=article_db[i]
		article_id=article_list['id']
		meal_sim_json=article_list['meal_sim_json']
		wear_sim_json=article_list['wear_sim_json']
		meal_temp_sim_json=article_list['meal_temp_sim_json']
		wear_temp_sim_json=article_list['wear_temp_sim_json']

		if meal_sim_json!=None:
			meal_sim_list=reverse_json(meal_sim_json)			
		else:
			meal_sim_list=[]

		if wear_sim_json!=None:
			wear_sim_list=reverse_json(wear_sim_json)			
		else:
			wear_sim_list=[]
		
		if meal_temp_sim_json!=None:
			meal_temp_sim_list=reverse_json(meal_temp_sim_json)
		else:
			meal_temp_sim_list=[]

		if wear_temp_sim_json!=None:
			wear_temp_sim_list=reverse_json(wear_temp_sim_json)
		else:
			wear_temp_sim_list=[]

		meal_wear_list=meal_sim_list+wear_sim_list+meal_temp_sim_list+wear_temp_sim_list
		
		#remove repeat and sort
		meal_wear_list_label_norepeat=[]
		meal_wear_list_label_norepeat_dic={}
		for mwl in meal_wear_list:  
			meal_wear_list_sim=mwl[0]
			meal_wear_list_label=mwl[1]
			meal_wear_list_label_norepeat.append(meal_wear_list_label)
			meal_wear_list_label_norepeat=list(set(meal_wear_list_label_norepeat))
		
		for mwlln in meal_wear_list_label_norepeat:
			meal_wear_list_label_norepeat_dic[mwlln]=-1
			

		for x in meal_wear_list_label_norepeat_dic:
			meal_wear_dic_label=x
			meal_wear_dic_label_sim=meal_wear_list_label_norepeat_dic[x]
			for mwl in meal_wear_list:
				meal_wear_list_sim=mwl[0]
				meal_wear_list_label=mwl[1]
				if meal_wear_dic_label==meal_wear_list_label and meal_wear_dic_label_sim<meal_wear_list_sim:
					meal_wear_list_label_norepeat_dic[x]=meal_wear_list_sim

		meal_wear_list_label_norepeat_dic_sort=sorted(meal_wear_list_label_norepeat_dic.items(),key=lambda item:item[1],reverse=True)
		meal_wear_list_label_norepeat_dic_sort_json=json.dumps(meal_wear_list_label_norepeat_dic_sort)
		#print(meal_wear_list_label_norepeat_dic)
		#print(meal_wear_list_label_norepeat_dic_sort)
		db.execute('update article_all1 set meal_wear_sim=%s where id=%s',(meal_wear_list_label_norepeat_dic_sort,article_id))
		db.execute('update article_all1 set meal_wear_sim_json=%s where id=%s',(meal_wear_list_label_norepeat_dic_sort_json,article_id))
		#print(meal_wear_list_label_norepeat)
		#print(meal_wear_list_label_norepeat_dic)
		#print('-'*20)

	return None


#word_num=3
#total_word_num=79
#file_num=10
#total_file_num=1000

def tfidf(word_num,total_word_num,file_num,total_file_num): #tf-idt equation

	word_num=float(word_num)
	total_word_num=float(total_word_num)
	file_num=float(file_num)
	total_file_num=float(total_file_num)
	print(word_num,total_word_num,file_num,total_file_num)
	result=(word_num/total_word_num)*(math.log10(total_file_num/file_num))
	#print(result)

	return result


def wordcloud_fre():  #tf-idf result
	article_db=db.query('select * from article_all1')
	db.execute('update article_all1 set word_fre=null') #initial

	check_num=1000 #check numbers
	top_num=10  #need top n
	
	for i in range(0,len(article_db)):
	#for i in range(11,12):
	#for i in range(0,3):
		#print(i)
		article_list=article_db[i]
		article_id=article_list['id']
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
		#print(article)
		article_segments=cut(article)
		#print(article_segments)
		segmentDF = pandas.DataFrame({'segment':article_segments}) #create segment DataFrame
		#print(segmentDF)
		#for i in article_segments:
		#	print(i)
		segStat = segmentDF.groupby(by=["segment"])["segment"].agg({"计数":numpy.size}).reset_index().sort(columns=["计数"],ascending=False)
		z=segStat.head(check_num) #check number n
		#print('-'*20)
		#print(z)
		#print(type(z))
		word_index_list=segStat.index  #DataFrame index
		#print(word_index_list)
		word_index_list_top=word_index_list[0:top_num]  #need top n
		#print(word_index_list[0:top_num])
		print(word_index_list_top)
		top_word_dic={}
		print(article)

		for word_index in word_index_list_top:

			word_index=word_index   #index
			word_cal_list=segStat['计数']			#appear frency
			word_name=segStat.segment[word_index]  #word 
			word_cal=word_cal_list[word_index]

			#print(word_index)
			print(word_name)
			#print(word_cal)
			fre=0
			for j in range(0,len(article_db)):
			#for i in range(0,3):
				article_list=article_db[j]
				#article_id=article_list['id']
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
				article2=title+'.'+introduce+'.'+content_all
				#print(article2)
				if word_name in article2:
					fre=fre+1
			#print(fre)
			total_word_num=sum(segStat['计数'])
			ti=tfidf(word_cal,total_word_num,fre,len(article_db))   #tf-idf			
			print(ti)
			top_word_dic[word_name]=ti
		#print(top_word_dic)
		top_word_dic_sort=sorted(top_word_dic.items(),key=lambda item:item[1],reverse=True)		#sort tf-idf
		top_word_dic_sort_json=json.dumps(top_word_dic_sort)		# trans to json
		#print(type(top_word_dic_sort))
		#print(top_word_dic_sort)
		db.execute('update article_all1 set word_fre=%s where id=%s',(top_word_dic_sort,article_id))
		db.execute('update article_all1 set word_fre_json=%s where id=%s',(top_word_dic_sort_json,article_id))
		print('-'*30)

	
	return None




'''
def label_filter():
	article_db=db.query('select * from article_all1')
	for i in range(0,len(article_db)):
		#for i in range(0,3):
		article_list=article_db[i]
		article_id=article_list['id']
		title=article_list['title']
		introduce=article_list['introduce']
		content=article_list['content']
		label_meal_wear=article_list['meal_wear']
		js_content=json.loads(content)
		content_all=''
		label_update=[]
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
		print(i)
		#print(article)
		#print(label_meal_wear)
		if label_meal_wear==None:
			label_update=[]
		else:
			label_meal_wear_list=label_extract(label_meal_wear)
			for lu in label_meal_wear_list:
				lu_synonymes=translate_synonymes(lu)
				article_synonymes=translate_synonymes(article)
				if lu in article or lu_synonymes in article_synonymes:
					label_update.append(lu)
		#print(label_update)
		db.execute('update article_all1 set label_update=%s where id=%s',(label_update,article_id))
'''

def label_select():
	article_db=db.query('select * from article_all1')
	meal_wear_sim_num=2
	word_fre_num=3
	for i in range(0,len(article_db)):
		#for i in range(0,3):
		#print(i)
		label_update=[]
		article_list=article_db[i]
		article_id=article_list['id']
		meal_wear_sim_json=article_list['meal_wear_sim_json']
		word_fre_json=article_list['word_fre_json']
		meal_wear_sim=json.loads(meal_wear_sim_json)
		word_fre=json.loads(word_fre_json)
		label_update_list=meal_wear_sim[0:meal_wear_sim_num]+word_fre[0:word_fre_num]
		label_update_json=json.dumps(label_update_list)
		#print(label_update_json)		
		db.execute('update article_all1 set label_update_json=%s where id=%s',(label_update_json,article_id))
		for l in label_update_list:
			label_update_label=l[0]
			label_update.append(label_update_label)
			label_update=list(set(label_update))
			db.execute('update article_all1 set label_update=%s where id=%s',(label_update,article_id))
		
		print(i)

	return None


#t1=threading.Thread(target=with_synoymes_meal)
#t2=threading.Thread(target=without_synoymes_meal)
#t3=threading.Thread(target=with_synoymes_wear)
#t4=threading.Thread(target=without_synoymes_wear)
#threads=[t1,t2,t3,t4]



if __name__=="__main__":	
	
	with_synoymes_meal()
	without_synoymes_meal()
	with_synoymes_wear()
	without_synoymes_wear()
	#label_filter()
	combine_meal_wear()
	wordcloud_fre()	
	label_select()
	print('------------finish----------------')
	print(time.localtime())
