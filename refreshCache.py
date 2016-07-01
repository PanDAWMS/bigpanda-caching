#!/usr/bin/python

import os
import sys
import commands
import datetime
from TaskRunner import *
from ThreadPool import *

class refreshCache:
    def __init__(self):
        self.status=None

    def refresh(self):
        threadPool = ThreadPool(20)
        with open('urls.txt') as urls:
            for line in urls:
                line = line.rstrip('\r\n')
                taskRunner=TaskRunner()
                command="curl -L --capath /etc/grid-security/certificates --cacert /etc/grid-security/certificates/CERN-GridCA.pem --cert ~/.globus/usercert.pem --key ~/.globus/userkeynopw.pem '"+line+"' "+" >/dev/null 2>&1"
                print command
                taskRunner.setCommand(command,5*60)
                threadPool.putTask(taskRunner)
        threadPool.wait()

if __name__=='__main__':
    rf=refreshCache()
    rf.refresh()
