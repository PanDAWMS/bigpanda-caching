#!/bin/bash

status(){
        ps -ef|egrep "refreshCache.py"|grep -v grep
        return $?
}

status > /dev/null 2>&1

if [ $? -ne 0 ]
then
        /home/atlpan/python/refreshCache.py

fi

