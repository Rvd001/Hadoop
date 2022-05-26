#!/usr/bin/python

from multiprocessing.connection import Client
from hdfs import InsecureClient
from datetime import datetime
import posixpath as psp


client = InsecureClient(url='http://localhost:9870', root='/')

# helper to print file permissions in readable format
# e.g., -rwxr-xr-x or drwxr-xrwx
def perms(p, prefix='-'):
	s = ''
	for idx in range(0, 3):
		if int(p[idx]) & 4 is 4: s = s + 'r'
		else: s = s + '-'
		if int(p[idx]) & 2 is 2: s = s + 'w'
		else: s = s + '-'
		if int(p[idx]) & 1 is 1: s = s + 'x'
		else: s = s + '-'

	return prefix + s

# helper to print stats about a file/directory
# the prefix is the first printed permission,
# a 'd' for directory otherwise '-'
def printfile(name, stats, prefix='-'):
	print(' '.join((\
		perms(stats['permission'], prefix),\
		'  -' if stats['replication'] is 0 else '%3d' % stats['replication'],\
		stats['owner'],\
		stats['group'],\
		'%10d' % stats['length'],\
		datetime.fromtimestamp(stats['modificationTime'] / 1000).strftime('%Y-%m-%d %H:%M'),\
		name)))

print('Begin')

#!/bin/sh

#1 Make a directory on HDFS named /part1/
client.makedirs('/part1/data')
client.makedirs('/activity/data')

#2 Put the Question.txt into HDFS as the path: /part1/data/
content = client.makedirs('/part1/data')
client.upload('/part1/data','/mySpace/Question.txt' )

#3 List the contents of the HDFS directory /part1/data/
client.list('/part1/data/')
#4 View the contents of Question.txt
with client.read('/activity1/data/pythonQuestion.txt') as reader:
	print(reader.read())
  
#5 Move the HDFS file /part1/data/Question.txt to /activity1/data/PythonQuestion.txt
client.rename('/part1/data/Question.txt', '/activity1/data/pythonQuestio.txt')

#6 Put the Answer.txt into HDFS as the path: /part1/data/
content = client.makedirs('/part1/data')
client.upload('/part1/data','/mySpace/Answer.txt' )

#7 View the contents of Answer.txt
with client.read('/part1/data/Answer.txt') as reader:
	print(reader.read())
  
#8 List the disk space used by the HDFS directory /part1/data/
client.status('/part1/data/')

#9 Recursively list the contents of the HDFS directory /part1/
client.list('/part1/')

#10 Remove the HDFS directory /part1/ and all files/directories underneath it
client.delete('/part1', recursive=True)
client.delete('/activity1', recursive=True)
