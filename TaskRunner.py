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


class TaskRunner:
    def __init__(self):
        self.__process=-1
        self.__childProcs=[]
        self.__command=None
        self.__timeout=10*3600
        self.__status=-1
        self.__callback=None
        self.__callbackArgs=None

    def makeNonBlocking(self,fd):
        fl = fcntl.fcntl(fd, fcntl.F_GETFL)
        try:
            fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NDELAY)
        except AttributeError:
            fcntl.fcntl(fd, fcntl.F_SETFL, fl | fcntl.FNDELAY)

    def setCommand(self,command,timeout=10*3600):
        self.__command=command
        self.__timeout=timeout

    def status(self):
        return self.__status

    def setCallback(self,callback,args=None):
        self.__callback=callback
        self.__callbackArgs=args

    def callback(self):
        if self.__callback!=None:
            call=self.__callback
            call(self.__status,self.__callbackArgs)

    def run(self):
        """
        for i in range(3):
            self.__status=self.runCommand(self.__command,self.__timeout)
            if self.__status==0:
                break
        """
        self.__status=self.runCommand(self.__command,self.__timeout)

    def runCommand(self,command,timeout=10*3600):
        try:
            start=datetime.datetime.now()
            child = popen2.Popen3(command, 1) # capture stdout and stderr from command
            child.tochild.close()             # don't need to talk to child
            self.__process = child.pid
            print "My process number is: %s" % self.__process
            outfile = child.fromchild
            outfd = outfile.fileno()
            errfile = child.childerr
            errfd = errfile.fileno()
            self.makeNonBlocking(outfd)            # don't deadlock!
            self.makeNonBlocking(errfd)
            outdata = errdata = ''
            outeof = erreof = 0
            while 1:
                ready = select.select([outfd,errfd],[],[],timeout/2) # wait for input
                if outfd in ready[0]:
                    outchunk = outfile.read()
                    if outchunk == '': outeof = 1
                    sys.stdout.write(outchunk)
                if errfd in ready[0]:
                    errchunk = errfile.read()
                    if errchunk == '': erreof = 1
                    sys.stderr.write(errchunk)
                now=datetime.datetime.now()
                if (now-start).seconds > timeout:
                    self.killTask()
                if outeof and erreof: break
                select.select([],[],[],.1) # give a little time for buffers to fill

            try:
                #err = child.poll()
                err=child.wait()
            except Exception, ex:
                sys.stderr.write("Error retrieving child exit code: %s" % err)
                sys.stderr.write(ex)
                return 1
            return err
        except Exception, ex:
            sys.stderr.write(ex)
            return 1
        return 0

    def findChildProcesses(self,pid):
        """
        _findChildProcesses_
        Given a PID, find all the direct child processes of that PID using ps
        command, returning a dictionary of names+proc numbers
        """

        command = "/bin/ps -e --no-headers -o pid -o ppid -o fname"

        status,output = commands.getstatusoutput(command)
        #print "ps output: %s" % output


        pieces = []
        result = []
        for line in output.split("\n"):
            pieces= line.split()
            try:
                value=int(pieces[1])
            except Exception,e:
                #print "trouble interpreting ps output %s: \n %s" % (e,pieces)
                continue
            if value==pid:
                try:
                    job=int(pieces[0])
                except ValueError,e:
                    #print "trouble interpreting ps output %s: \n %s" % (e,pieces[0])
                    continue
                result.append(job)
        return result

    def getChildren(self, pid):
        """
        recursive call to get children for a process
        """
        childProcs = self.findChildProcesses(pid)
        for child in childProcs:
            print "Child Process found: %s" % child
            self.__childProcs.append(child)
            self.getChildren(child)
        return

    def findProcesses(self):
        self.getChildren(self.__process)
        return self.__childProcs

    def killTask(self):
        print "TaskRunner.killTask called"
        if self.__process > -1:
            ###SIGTERM
            procList = self.findProcesses()
            for process in procList:
                print "Sending SIGTERM to process: %s " % process
                try:
                    os.kill(int(process), signal.SIGTERM)
                except OSError,e:
                    print "SIGKILL error: %s, removing process from list..." % e
                    procList.remove(process)
            try:
                os.kill(self.__process, signal.SIGTERM)
            except OSError:
                pass
            time.sleep(5)
            ###SIGKILL
            procList = self.findProcesses()
            for process in procList:
                print "Sending SIGKILL to process: %s " % process
                try:
                    os.kill(int(process), signal.SIGKILL)
                except OSError,e:
                    print "SIGKILL error: %s, removing process from list..." % e
                    procList.remove(process)
            try:
                os.kill(self.__process, signal.SIGKILL)
            except OSError:
                pass
        else:
            print "self.__process <= -1"
        return

