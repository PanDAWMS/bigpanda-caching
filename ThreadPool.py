#!/usr/bin/python

import os
import sys
import commands
import time
import signal
import popen2
import fcntl, select
import datetime
import threading
import Queue

from TaskRunner import TaskRunner

class WorkerThread(threading.Thread):
    def __init__(self,requestsQ,resultsQ,pollTimeout=5,**kwds):
        threading.Thread.__init__(self,**kwds)
        self.setDaemon(1)
        self.__requestsQ=requestsQ
        self.__resultsQ=resultsQ
        self.__pollTimeout=pollTimeout
        self.__dismissed=threading.Event()
        self.start()

    def run(self):
        while True:
            if self.__dismissed.isSet():
                break
            try:
                taskRunner=self.__requestsQ.get(True,self.__pollTimeout)
            except Queue.Empty:
                continue
            else:
                if self.__dismissed.isSet():
                    self.__requestsQ.put(request)
                    break
                try:
                    taskRunner.run()
                    status=taskRunner.status()
                    taskRunner.callback()
                    self.__resultsQ.put(taskRunner)
                except:
                    self.__resultsQ.put(taskRunner)

    def dismiss(self):
        self.__dismissed.set()

class NoRequests(Exception):
    """All work requests have been processed."""
    pass

class ThreadPool:
    def __init__(self,numWorkers,reqQSize=0,resQSize=0,pollTimeout=5):
        self.__requestsQ=Queue.Queue(reqQSize)
        self.__resultsQ=Queue.Queue(resQSize)
        self.__workers=[]
        self.__dismissedWorkers=[]
        self.__numRequests=0
        self.createWorkers(numWorkers,pollTimeout)


    def createWorkers(self,numWorkers,pollTimeout):
        for i in range(numWorkers):
            self.__workers.append(WorkerThread(self.__requestsQ,self.__resultsQ,pollTimeout=pollTimeout))

    def dismissWorkers(self,numWorkers,doJoin=False):
        dismissList=[]
        for i in range(min(numWorkers,len(self.__workers))):
            worker=self.workers.pop()
            worker.dismiss()
            dismissList.append(worker)

        if doJoin:
            for worker in dismissList:
                worker.join()
        else:
            self.__dismissedWorkers.extend(dismissList)

    def joinAllDismissedWorkers(self):
        for worker in self.__dismissedWorkers:
             worker.join()
        self.__dismissedWorkers=[]

    def putTask(self,taskRunner,block=True,timeout=None):
        assert isinstance(taskRunner,TaskRunner)
        self.__requestsQ.put(taskRunner,block,timeout)
        self.__numRequests+=1

    def numRequests(self):
        return self.__numRequests

    def poll(self,block=False):
        while True:
            if self.__numRequests==0:
                raise NoRequests
            try:
                taskRunner=self.__resultsQ.get(block)
                #taskRunner.callback()
                self.__numRequests-=1
            except Queue.Empty:
                break

    def wait(self):
        while 1:
            try:
                self.poll(True)
            except NoRequests:
                break
        for worker in self.__workers:
            worker.dismiss()
        for worker in self.__workers:
            worker.join()

