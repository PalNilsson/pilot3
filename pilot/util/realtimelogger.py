#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Shuwei Ye, yesw@bnl.gov, September 2021

import __future__
import os,sys
import time
import json
from pilot.util.config import config
from logging import Logger, INFO
import logging

logger = logging.getLogger(__name__)
## logServer = "google-cloud-logging"


def get_realtimeLogger(args=None):
    if RealTimeLogger.gLogger == None:
         RealTimeLogger(args)
    return RealTimeLogger.gLogger


# RealTimeLogger is called if args.realTimeLogger is on
class RealTimeLogger(Logger):
    gLogger = None

    def __init__(self, args):
        super(RealTimeLogger, self).__init__(name="realTimeLogger", level=INFO)
        RealTimeLogger.gLogger = self

        self.jobInfo = {}
        self.logFiles = []
        self.openFiles = {}

        if args.use_realtime_logging:
            name = args.realtime_logname
            if name:
                logServer = args.realtime_logging_server
            else:
                logServer = ""
        items = logServer.split(':')
        logType = items[0].lower()
        h = None
        if logType == "google-cloud-logging":
            import google.cloud.logging
            from google.cloud.logging_v2.handlers import CloudLoggingHandler
            client = google.cloud.logging.Client()
            h = CloudLoggingHandler(client, name=name)
            self.addHandler(h)

        elif logType == "fluent":
            if len(items)<3:
                RealTimeLogger.gLogger = None
            fluentServer = items[1]
            fluentPort   = items[2]
            from fluent import handler
            h = handler.FluentHandler(name, host=fluentServer, port=fluentPort)
            
        elif logType == "logstash":
            pass

        else:
            pass

        if h is not None:
            self.addHandler(h)
        else:
            RealTimeLogger.gLogger = None
            del self

    def setJobInfo(self, job):
        self.jobInfo = {"TaskID":job.taskid, "PandaJobID":job.jobid}
        if 'HARVESTER_WORKER_ID' in os.environ:
            self.jobInfo["Harvester_WorkerID"] = os.environ.get('HARVESTER_WORKER_ID')
        logger.debug('setJobInfo with PandaJobID=%s', self.jobInfo["PandaJobID"])

    # prepend some panda job info
    # check if the msg is a dict-based object via isinstance(msg,dict),
    # then decide how to insert the PandaJobInf
    def sendWthJobInfo(self, msg):
        logObj = self.jobInfo.copy()
        try:
            msg = json.loads(msg)
            logObj.update(msg)
        except:
            logObj["message"] = msg
        self.info(logObj)

    def addLogFiles(self, jobOrFilenames, reset=True):
        self.closeFiles()
        if reset:
            self.logFiles = []
        if type(jobOrFilenames) == list:
            logFiles = jobOrFilenames
            for logFile in logFiles:
                self.logFiles += [logFile]
        else:
            job = jobOrFilenames
            stdout = os.path.join(job.workdir, config.Payload.payloadstdout)
            self.logFiles += [stdout]
            stderr = os.path.join(job.workdir, config.Payload.payloadstderr)
            self.logFiles += [stderr]
        if len(self.logFiles) > 0:
            logger.info('Added log files:%s', self.logFiles)

    def closeFiles(self):
        for openFile in self.openFiles.values():
            if openFile is not None:
                openFile.close()
        self.openFiles = {}
        self.logFiles = []

    def sendLogInFiles(self):
        for openFile in self.openFiles.values():
            if openFile is not None:
                lines = openFile.readlines()
                for line in lines:
                    self.sendWthJobInfo(line.strip())

    def sendingLogs(self, args, job):
        logger.info('Starting RealTimeLogger.sendingLogs')
        if len(self.jobInfo) == 0:
            self.setJobInfo(job)
        if len(self.logFiles) == 0:
            self.addLogFiles(job)
        while not args.graceful_stop.is_set():
            if job.state == '' or job.state == 'starting' or job.state == 'running':
                if len(self.logFiles) > len(self.openFiles):
                    for logFile in self.logFiles:
                        if logFile not in self.openFiles:
                            if os.path.exists(logFile):
                                openFile = open(logFile)
                                openFile.seek(0)
                                self.openFiles[logFile] = openFile
                                logger.debug('Opened log files:%s', logFile)
                            else:
                                self.openFiles[logFile] = None
                self.sendLogInFiles()
            else:
                self.sendLogInFiles() # send the remaining logs after the job completion
                self.closeFiles()
                break
            time.sleep(5)
        else:
            self.sendLogInFiles() # send the remaining logs after the job completion
            self.closeFiles()

# in pilot/control/payload.py
# define a new function run_realTimeLog(queues, traces, args)
# add add it into the thread list "targets"
