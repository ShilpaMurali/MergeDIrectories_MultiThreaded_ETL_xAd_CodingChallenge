#!/usr/bin/python

import threading
import time
import sys
try:
    import Queue as Q  # ver. < 3.0
except ImportError:
    import queue as Q

import os
import errno
import logging
import csv
import codecs
import glob
import json
from collections import OrderedDict
from datetime import datetime
from inspect import getsourcefile
from os.path import abspath

#Global dictionary containing all the files in Imps and Clicks
dictImpsClicks={} 

def loadDeviceAndConnectionTypes(jsonFile,key):
    try:
        with open(jsonFile) as json_data:
            d = json.load(json_data)
        return d[int(key)-1][1]
    except IOError:
            print("Cannot open",jsonFile)
###############################################
#Create a list with filenames
###############################################
def list_files(path):
    # returns a list of names (with extension, without full path) of all files
    # in folder path
    files = []
    for name in os.listdir(path):
        if os.path.isfile(os.path.join(path, name)):
                files.append(name)
    return files
	
#Build the output json response and store in "out" dir

def jsonResponseBuilder(dictImpsFiles,dictClicksFiles,fileName,key,imps,clicks,threadName):
	#print fileName
	try:
		with open(fileName,"a+") as f:
			#print ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
			timeConverted =  int(dictImpsFiles[key][0])
			timeConverted=datetime.fromtimestamp(timeConverted).isoformat()+"-07:00"
			jsonResponse = OrderedDict([('iso8601_timestamp', timeConverted ),("transaction_id",dictImpsFiles[key][1]),("connection_type",loadDeviceAndConnectionTypes(dir_path+"/in/dimensions/connection_type.json",dictImpsFiles[key][2])),("device_type",loadDeviceAndConnectionTypes(dir_path+"/in/dimensions/device_type.json",dictImpsFiles[key][3]))])
			if(imps):
				jsonResponse.update({"imps":dictImpsFiles[key][4]})
			if(clicks):
				jsonResponse.update({"clicks":dictClicksFiles[key][2]})
			json_data = json.dumps(jsonResponse,sort_keys=False)
			f.write(json_data)
			f.write("\n")
	except IOError as exception:
		pass
        #logging.exception("Cannot open",fileName)
        #raise

#Directory Traversal and merge
def fileTraversal(threadName, q, impsFiles, clicksFiles):
	#print("Inside file Traversal")
	while(True):   
		queueLock.acquire()
		if(not q.empty()):
			data = q.get()
			queueLock.release()
			start = time.time()
			#logging.debug(data)
			logging.debug('%s ETL Start' % data)
			#logging.debug('\n')
			#print("%s processing %s" % (threadName, data))
			fileMerge(data,threadName,impsFiles,clicksFiles)
			end = time.time()
			logging.debug('%s ETL complete, elapsed time %f' %(data, (end - start)))
			#logging.debug('\n')
			#logging.debug(end - start)
		else:
			logging.debug('%s ETL Queue empty. No more data to process')
			print("%s doesn't have any job to process" % (threadName))
			queueLock.release()
			break

        
def fileMerge(data,threadName,impsFiles,clicksFiles):
	dictImpsFiles={}
	dictClicksFiles={}
	imps=False,
	clicks=False
	if(dictImpsClicks[data][0]):
		imps=True
		#print('data is%s and thread name%s' % (data,threadName))
		#print(data)
		with open(dir_path+"/in/facts/imps/"+data) as mycsvfile:
			dataImps = csv.reader(mycsvfile)
			for row in dataImps:
				#print(row)
				if(str(row[1]) not in dictImpsFiles):
					dictImpsFiles[str(row[1])]=row
		#print(dictImpsFiles)
	if(dictImpsClicks[data][1]):
		clicks=True
		with open(dir_path+"/in/facts/clicks/"+data) as csvfileClicks:
			dataClicks = csv.reader(csvfileClicks)
			for rowClicks in dataClicks:
				if(str(rowClicks[1]) not in dictClicksFiles):
					dictClicksFiles[str(rowClicks[1])]=rowClicks
		#print(dictClicksFiles)
	for key in dictImpsFiles:
		fileName=dir_path+"/out/"+data.split(".", 1)[0]+".json"
		#print fileName
		if(key in dictClicksFiles):
			jsonResponseBuilder(dictImpsFiles,dictClicksFiles,fileName,key,True,True,threadName)
		else:
			jsonResponseBuilder(dictImpsFiles,dictClicksFiles,fileName,key,True,False,threadName)

				
###############################################
#Thread class
###############################################
class myThread(threading.Thread):
    def __init__(self, threadID, name, q,impsFiles, clicksFiles):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.impsFiles = impsFiles
        self.clicksFiles = clicksFiles
        self.q = q
		#self.q = q
    def run(self):
        print("Starting " + self.name)
        #print_file(self.name, self.counter, 5, self.q)
        fileTraversal(self.name, self.q, self.impsFiles, self.clicksFiles)
        print("Exiting " + self.name)
		


###############################################
# Check if directory is there
###############################################
def make_sure_path_exists(path):
    try:
        os.makedirs(path)
    except OSError as exception:
            if(exception.errno != errno.EEXIST):
                logging.exception('Got exception while checking for log directory')
                raise
            
def createDictionaryForFiles(listInput,imps,clicks):
    for i in range(len(listInput)):
        if(listInput[i] not in dictImpsClicks):
            data=[imps,clicks]
            dictImpsClicks[listInput[i]]=data
        else:
            if(clicks):
                dictImpsClicks[listInput[i]][1]=clicks
            elif(imps):
                dictImpsClicks[listInput[i]][0]=imps

def createQueueWithTimestamps(dictImpsClicks,q):  
    for keys in (dictImpsClicks):
        q.put(keys)

def clearOutputDirectory(directory):
    os.chdir(directory)
    files=glob.glob('*.json')
    for filename in files:
        os.unlink(filename)

if __name__=="__main__":
    
	dir_path=(os.path.dirname(os.path.realpath(__file__)))
	dir_path = dir_path.replace('\\','/')
	print(dir_path)

#############################################
#Getting input from command line.
#Having two options -h and -p
#-p mentions number of threads we need to use
###############################################
	numThread = 5
	exitFlag = 0
	numArg = len(sys.argv)
	if numArg == 1:
		pass
	elif numArg == 2:
		if str(sys.argv[1]) == '-h':
			print("Please use -p to mention number of threads. Else default is 5 threads.")
			sys.exit()
		elif str(sys.argv[1]) == '-p':
			print("Number of threads is not mentioned. Please mention number of threads followed by -p.")
			sys.exit()
		else:
			print("Unrecognized input. Please use either -h or -p.")
			sys.exit()
	elif numArg == 3:
		if str(sys.argv[1]) == '-p':
			if int(sys.argv[2]) > 0:
				numThread = int(sys.argv[2])
			else:
				print("Please enter a value greater than 0.")
				sys.exit()
		else:
			print("Unrecognized input. Please use either -h or -p.")
			sys.exit()
		


###############################################
#Find the number of files in the directory
#We need this make it has maximum size of the queue
###############################################
	impsFiles=list_files(dir_path+"/in/facts/imps")
	clicksFiles=list_files(dir_path+"/in/facts/clicks")

	createDictionaryForFiles(impsFiles,True,False)
	createDictionaryForFiles(clicksFiles,False,True)
	#print(dictImpsClicks)

###############################################
#creating the queue
#with all the file names
###############################################
	queueLock = threading.Lock()
	q = Q.Queue(len(dictImpsClicks))
	createQueueWithTimestamps(dictImpsClicks,q)

	#clear out directory during each run
	clearOutputDirectory(dir_path+'/out')



	###############################################
	#Check if log directory is already available if not create one to start logging
	###############################################
	make_sure_path_exists(dir_path+'/logs')
	LOG_FILENAME = dir_path+'/logs/etl.log'
	logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG)

	###############################################
	# Create new threads in loop.
	###############################################
	threads = []
	for i in range(numThread):
		thread = myThread(i+1, "Thread-"+str(i+1),q,impsFiles, clicksFiles)
		threads.append(thread)
		thread.start()

	###############################################
	# Wait for all threads to complete
	###############################################
	for t in threads:
		t.join()

	print("Exiting Main Thread")
