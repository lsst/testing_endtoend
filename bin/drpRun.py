#!/usr/bin/env python

# LSST Data Management System
# Copyright 2008, 2009, 2010, 2011 LSST Corporation.
# 
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the LSST License Statement and 
# the GNU General Public License along with this program.  If not, 
# see <http://www.lsstcorp.org/LegalNotices/>.

from __future__ import with_statement
from email.mime.text import MIMEText
import glob
from optparse import OptionParser
import os
import re
import shutil
import socket
import sqlite
import subprocess
import sys
import tempfile
import time

import eups
# import lsst.pex.policy as pexPolicy
from lsst.daf.persistence import DbAuth

def _checkReadable(path):
    if not os.access(path, os.R_OK):
        raise RuntimeError("Required path " + path + " is unreadable")

def _checkWritable(path):
    if not os.access(path, os.W_OK):
        raise RuntimeError("Required path " + path + " is unwritable")

class NoMatchError(RuntimeError):
    pass

class RunConfiguration(object):

    ###########################################################################
    # Configuration information
    ###########################################################################

    inputBase = "/lsst3/weekly/data"
    outputBase = "/lsst3/weekly/datarel-runs"
    pipelinePolicy = "W2012Pipe/main.paf"
    toAddress = "lsst-devel-runs@lsstcorp.org"
    pipeQaBase = "http://lsst1.ncsa.illinois.edu/pipeQA/dev/"
    pipeQaDir = "/lsst/public_html/pipeQA/html/dev"
    dbHost = "lsst10.ncsa.uiuc.edu"
    dbPort = 3306

    # One extra process will be used on the first node for the JobOffice
    machineSets = {
            'rh6-1': ['lsst5:4', 'lsst6:2'],
            'rh6-2': ['lsst6:2', 'lsst9:4'],
            'rh6-3': ['lsst11:2', 'lsst14:2', 'lsst15:2']
    }

    runIdPattern = "%(runType)s_%(datetime)s"
    lockBase = os.path.join(outputBase, "locks")
    collection = "PT1.2"
    spacePerCcd = int(160e6) # calexp only
    version = 2
    sendmail = None
    for sm in ["/usr/sbin/sendmail", "/usr/bin/sendmail", "/sbin/sendmail"]:
        if os.access(sm, os.X_OK):
            sendmail = sm
            break
    if sendmail is None:
        raise RuntimeError("Unable to find sendmail executable")

    ###########################################################################

    def __init__(self, args):
        self.datetime = time.strftime("%Y_%m%d_%H%M%S")
        self.user = os.getlogin()
        self.dbUser = DbAuth.username(RunConfiguration.dbHost,
                str(RunConfiguration.dbPort))
        self.hostname = socket.getfqdn()
        self.fromAddress = "%s@%s" % (self.user, self.hostname)

        self.options, self.args = self.parseOptions(args)

        # Handle immediate commands
        if self.options.printStatus:
            self.printStatus()
            sys.exit(0)
        if self.options.report is not None:
            self.report(os.path.join(self.options.output,
                self.options.report, "run", "run.log"))
            sys.exit(0)
        if self.options.listRuns:
            self.listRuns(self.options.listRuns)
            sys.exit(0)
        if self.options.listInputs:
            self.listInputs()
            sys.exit(0)
        if self.options.linkLatest is not None:
            self.linkLatest(self.options.linkLatest)
            sys.exit(0)
        if self.options.kill is not None:
            self.kill(self.options.kill)
            sys.exit(0)
        if self.options.hosts is not None:
            self.hosts()
            sys.exit(0)

        if self.arch is None:
            if self.options.arch is None:
                raise RuntimeError("Architecture is required")
            self.arch = self.options.arch

        if re.search(r'[^a-zA-Z0-9_]', self.options.runType):
            raise RuntimeError("Run type '%s' must be one word" %
                    (self.options.runType,))

        self.collectionName = re.sub(r'\.', '_', RunConfiguration.collection)
        runIdProperties = dict(
                user=self.user,
                dbUser=self.dbUser,
                coll=self.collectionName,
                runType=self.options.runType,
                datetime=self.datetime)
        self.runId = RunConfiguration.runIdPattern % runIdProperties
        runIdProperties['runid'] = self.runId
        dbNamePattern = "%(dbUser)s_%(coll)s_u_%(runid)s"
        self.dbName = dbNamePattern % runIdProperties

        self.inputBase = os.path.join(RunConfiguration.inputBase,
                self.options.input)
        self.inputDirectory = os.path.join(self.inputBase,
                RunConfiguration.collection)
        self.outputDirectory = os.path.join(self.options.output, self.runId)
        self.outputDirectory = os.path.abspath(self.outputDirectory)
        if os.path.exists(self.outputDirectory):
            raise RuntimeError("Output directory %s already exists" %
                    (self.outputDirectory,))
        os.mkdir(self.outputDirectory)
        self.pipeQaUrl = RunConfiguration.pipeQaBase + self.dbName + "/"

        self.eupsPath = os.environ['EUPS_PATH']
        e = eups.Eups(readCache=False)
        self.setups = dict()
        for product in e.getSetupProducts():
            if product.name != "eups":
                self.setups[product.name] = \
                        re.sub(r'^LOCAL:', "-r ", product.version)

        # TODO -- load policy and apply overrides
        self.options.override = None

    def hosts(self):
        machines = set()
        for machineSet in RunConfiguration.machineSets.itervalues():
            for machine in machineSet:
                machine = re.sub(r':.*', "", machine)
                machines.update([machine])
        for machine in machines:
            subprocess.check_call(["ssh", machine, "/bin/true"])

    def printStatus(self):
        machineSets = RunConfiguration.machineSets.keys()
        machineSets.sort()
        for k in machineSets:
            lockFile = self._lockName(k)
            if os.path.exists(lockFile):
                print "*** Machine set", k, str(RunConfiguration.machineSets[k])
                self.report(lockFile)

    def report(self, logFile):
        with open(logFile, "r") as f:
            for line in f:
                print line,
                if line.startswith("Run:"):
                    runId = re.sub(r'Run:\s+', "", line.rstrip())
                if line.startswith("Output:"):
                    outputDir = re.sub(r'Output:\s+', "", line.rstrip())
        e = eups.Eups()
        if not e.isSetup("mysqlpython"):
            print >>sys.stderr, "*** mysqlpython not setup, skipping log analysis"
        else:
            print self.orcaStatus(runId, outputDir)

    def orcaStatus(self, runId, outputDir):
        result = ""
        tailLog = False
        try:
            status = self.analyzeLogs(runId, inProgress=True)
            result += status
            tailLog = (status == "No log entries yet\n")
        except NoMatchError:
            result += "\tDatabase not yet created\n"
            tailLog = True

        if tailLog:
            logFile = os.path.join(outputDir, "run", "unifiedPipeline.log")
            with open(logFile, "r") as log:
                try:
                    log.seek(-500, 2)
                except:
                    pass
                result += "(last 500 bytes)... " + log.read(500) + "\n"

        return result

    def listInputs(self):
        for path in sorted(os.listdir(RunConfiguration.inputBase)):
            if os.path.exists(os.path.join(RunConfiguration.inputBase, path,
                RunConfiguration.collection)):
                print path

    def listRuns(self, partialId):
        for path in sorted(glob.glob(os.path.join(self.options.output,
            "*" + partialId + "*"))):
            if not os.path.exists(os.path.join(path, "run")):
                continue
            runId = os.path.basename(path)
            print runId

    def check(self):
        for requiredPackage in ['ctrl_orca', 'datarel', 'astrometry_net_data']:
            if not self.setups.has_key(requiredPackage):
                raise RuntimeError(requiredPackage + " is not setup")
        if self.setups['astrometry_net_data'].find('imsim') == -1:
            raise RuntimeError("Non-imsim astrometry_net_data is setup")
        if not self.setups.has_key('testing_pipeQA'):
            print >>sys.stderr, "testing_pipeQA not setup, will skip pipeQA"
            self.options.doPipeQa = False
        if not self.setups.has_key('testing_displayQA'):
            print >>sys.stderr, "testing_displayQA not setup, will skip pipeQA"
            self.options.doPipeQa = False

        _checkReadable(self.inputDirectory)
        _checkReadable(os.path.join(self.inputDirectory, "bias"))
        _checkReadable(os.path.join(self.inputDirectory, "dark"))
        _checkReadable(os.path.join(self.inputDirectory, "flat"))
        _checkReadable(os.path.join(self.inputDirectory, "raw"))
        _checkReadable(os.path.join(self.inputDirectory, "refObject.csv"))
        self.registryPath = os.path.join(self.inputDirectory, "registry.sqlite3")
        _checkReadable(self.registryPath)

        if self.options.ccdCount is None:
            conn = sqlite.connect(self.registryPath)
            self.options.ccdCount = conn.execute(
                    """SELECT COUNT(DISTINCT visit||':'||raft||':'||sensor)
                    FROM raw;""").fetchone()[0]
        if self.options.ccdCount < 2:
            raise RuntimeError("Must process at least two CCDs")

        _checkWritable(self.outputDirectory)
        result = os.statvfs(self.outputDirectory)
        availableSpace = result.f_bavail * result.f_bsize
        minimumSpace = int(RunConfiguration.spacePerCcd * self.options.ccdCount)
        if availableSpace < minimumSpace:
            raise RuntimeError("Insufficient disk space in output filesystem:\n"
                    "%d available, %d needed" %
                    (availableSpace, minimumSpace))

    def run(self):
        self.runInfo = """Version: %d
Run: %s
RunType: %s
User: %s
DB User: %s
Pipeline: %s
EUPS_PATH: %s
Input: %s
CCD count: %d
Output: %s
Database: %s
Overrides: %s
""" % (RunConfiguration.version,
        self.runId, self.options.runType, self.user, self.dbUser,
        self.options.pipeline, os.environ["EUPS_PATH"],
        self.options.input, self.options.ccdCount, self.outputDirectory,
        self.dbName, str(self.options.override))

        self.lockMachines()
        try:
            os.chdir(self.outputDirectory)
            os.mkdir("run")
            os.chdir("run")
            self._log("Run directory created")
            self.generatePolicy()
            self._log("Policy created")
            self.generateInputList()
            self._log("Input list created")
            self.generateEnvironment()
            self._log("Environment created")
            self._sendmail("[drpRun] Start on %s: run %s" %
                    (self.machineSet, self.runId),self.runInfo)
            self._log("Orca run started")
            self.doOrcaRun()
            self._log("Orca run complete")
            self._sendmail("[drpRun] Orca done: run %s" % (self.runId,),
                    self.runInfo + "\n" + self.analyzeLogs(self.runId))

            if self.checkForKill():
                self._sendmail("[drpRun] Orca killed: run %s" % (self.runId,),
                        self.runInfo)
                self.unlockMachines()
                return
            if not self.checkForResults():
                self._log("*** Insufficient results after Orca")
                self._sendmail("[drpRun] Insufficient results: run %s" %
                        (self.runId,),
                        self.runInfo + "\n" + self.analyzeLogs(self.runId))
                self.unlockMachines()
                return

            self.setupCheck()
            self.doAdditionalJobs()
            self._log("SourceAssociation and ingest complete")
            if self.options.doPipeQa:
                self._sendmail("[drpRun] pipeQA start: run %s" %
                        (self.runId,), "pipeQA link: %s" % (self.pipeQaUrl,))
                self.doPipeQa()
                self._log("pipeQA complete")
            if not self.options.testOnly:
                self.doLatestLinks()
            if self.options.doPipeQa:
                self._sendmail("[drpRun] Complete: run %s" %
                        (self.runId,), "pipeQA link: %s " % (self.pipeQaUrl,))
            else:
                self._sendmail("[drpRun] Complete: run %s" %
                        (self.runId,), self.runInfo)

        except Exception, e:
            self._log("*** Exception in run:\n" + str(e))
            self._sendmail("[drpRun] Aborted: run %s" % (self.runId,),
                    self.runInfo + "\n" + str(e))
            raise

        finally:
            self.unlockMachines()

###############################################################################
# 
# General utilities
# 
###############################################################################

    def _sendmail(self, subject, body, toStderr=True):
        print >>sys.stderr, subject
        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = self.fromAddress
        msg['To'] = self.options.toAddress

        mail = subprocess.Popen([RunConfiguration.sendmail,
            "-t", "-f", self.fromAddress], stdin=subprocess.PIPE)
        try:
            print >>mail.stdin, msg
        finally:
            mail.stdin.close()

    def _lockName(self, machineSet):
        return os.path.join(RunConfiguration.lockBase, machineSet)

    def _log(self, message):
        with open(self._lockName(self.machineSet), "a") as lockFile:
            print >>lockFile, time.asctime(), message
        print >>sys.stderr, time.asctime(), message

###############################################################################
# 
# Generate input files
# 
###############################################################################

    def generatePolicy(self):
        with open("joboffice.paf", "w") as policyFile:
            print >>policyFile, """#<?cfg paf policy ?>
execute: {
  shutdownTopic: "workflowShutdown"
  eventBrokerHost: "lsst8.ncsa.uiuc.edu"
}
framework: {
  exec: "$DATAREL_DIR/pipeline/PT1Pipe/joboffice-ImSim.sh"
  type: "standard"
  environment: unused
}
"""

        with open("platform.paf", "w") as policyFile:
            print >>policyFile, """#<?cfg paf policy ?>
dir: {
    defaultRoot: """ + self.options.output + """
    runDirPattern:  "%(runid)s"
    work:     work
    input:    input
    output:   output
    update:   update
    scratch:  scr
}

hw: {
    nodeCount:  4
    minCoresPerNode:  2
    maxCoresPerNode:  8
    minRamPerNode:  2.0
    maxRamPerNode: 16.0
}

deploy:  {
    defaultDomain:  ncsa.illinois.edu
"""
            first = True
            for machine in RunConfiguration.machineSets[self.machineSet]:
                if first:
                    processes = int(re.sub(r'.*:', "", machine)) + 1
                    jobOfficeMachine = re.sub(r':.*', "", machine)
                    print >>policyFile, "            nodes: ", \
                            jobOfficeMachine + ":" + str(processes)
                    first = False
                else:
                    print >>policyFile, "            nodes: ", machine
            print >>policyFile, "}"

        subprocess.check_call(
                "cp $DATAREL_DIR/pipeline/%s ." % (self.options.pipeline,),
                shell=True)

        if self.options.pipeline.find("/") != -1:
            components = self.options.pipeline.split("/")
            dir = os.path.join(*components[0:-1])
            policy = components[-1]
            subprocess.check_call(
                    "ln -s $DATAREL_DIR/pipeline/%s ." % (dir,),
                    shell=True)
        else:
            policy = self.options.pipeline

        with open("orca.paf", "w") as policyFile:
            print >>policyFile, """#<?cfg paf policy ?>
shortName:           DataRelease
eventBrokerHost:     lsst8.ncsa.uiuc.edu
repositoryDirectory: .
productionShutdownTopic:       productionShutdown

database: {
    name: dc3bGlobal
    system: {   
        authInfo: {
            host: """ + RunConfiguration.dbHost + """
            port: """ + str(RunConfiguration.dbPort) + """
        }
        runCleanup: {
            daysFirstNotice: 7  # days when first notice is sent before run can be deleted
            daysFinalNotice: 1  # days when final notice is sent before run can be deleted
        }
    }

    configurationClass: lsst.ctrl.orca.db.DC3Configurator
    configuration: {  
        globalDbName: GlobalDB
        dcVersion: """ + self.collectionName + """
        dcDbName: DC3b_DB
        minPercDiskSpaceReq: 10   # measured in percentages
        userRunLife: 2            # measured in weeks
    }
    logger: {
        launch: true
    }
}

workflow: {
    shortName: Workflow
    platform: @platform.paf
    shutdownTopic:       workflowShutdown

    configurationClass: lsst.ctrl.orca.GenericPipelineWorkflowConfigurator
    configuration: {
        deployData: {
            dataRepository: """ + self.inputBase + """
            collection: """ + RunConfiguration.collection + """
            script: "$DATAREL_DIR/bin/runOrca/deployData.sh"
        }
        announceData: {
            script: $CTRL_SCHED_DIR/bin/announceDataset.py
            topic: RawCcdAvailable
            inputdata: ./ccdlist
        }
    }

    pipeline: {
        shortName:     joboffices
        definition:    @joboffice.paf
        runCount: 1
        deploy: {
            processesOnNode: """ + jobOfficeMachine + """:1
        }
        launch: true
    }
"""
            self.nPipelines = 0
            for machine in RunConfiguration.machineSets[self.machineSet]:
                machineName, processes = machine.split(':')
                self.nPipelines += int(processes)
                print >>policyFile, """
    pipeline: {
        shortName:     """ + machineName + """
        definition:    @""" + policy + """
        runCount: """ + processes + """
        deploy: {
            processesOnNode: """ + machineName + ":" + processes + """
        }
        launch: true
    }
"""
            print >>policyFile, "}"

    def generateInputList(self):
        with open("ccdlist", "w") as inputFile:
            print >>inputFile, ">intids visit"
            conn = sqlite.connect(self.registryPath)
            conn.text_factory = str
            cmd = "SELECT DISTINCT visit, raft, sensor " + \
                    "FROM raw ORDER BY visit, raft, sensor"
            if self.options.ccdCount is not None and self.options.ccdCount > 0:
                cmd += " LIMIT %d" % (self.options.ccdCount,)
            cursor = conn.execute(cmd)
            for row in cursor:
                print >>inputFile, "raw visit=%s raft=%s sensor=%s" % row

            for i in xrange(self.nPipelines):
                print >>inputFile, "raw visit=0 raft=0 sensor=0"

    def generateEnvironment(self):
        with open("env.sh", "w") as envFile:
            # TODO -- change EUPS_PATH based on selected architecture
            print >>envFile, "export EUPS_PATH=" + self.eupsPath
            for dir in self.eupsPath.split(':'):
                loadScript = os.path.join(dir, "loadLSST.sh")
                if os.path.exists(loadScript):
                    print >>envFile, "source", loadScript
                    break
            for pkg in sorted(self.setups.keys()):
                print >>envFile, "setup -j", pkg, self.setups[pkg]

        configDirectory = os.path.join(self.outputDirectory, "config")
        os.mkdir(configDirectory)
        subprocess.check_call("eups list --setup > %s/weekly.tags" %
                (configDirectory,), shell=True)

###############################################################################
# 
# Routines for executing production
# 
###############################################################################

    def _lockSet(self, machineSet):
        (tempFileDescriptor, tempFilename) = \
                tempfile.mkstemp(dir=RunConfiguration.lockBase)
        with os.fdopen(tempFileDescriptor, "w") as tempFile:
            print >>tempFile, self.runInfo,
        os.chmod(tempFilename, 0644)
        try:
            os.link(tempFilename, self._lockName(machineSet))
        except:
            os.unlink(tempFilename)
            return False
        os.unlink(tempFilename)
        return True

    def lockMachines(self):
        machineSets = sorted(RunConfiguration.machineSets.keys())
        for machineSet in machineSets:
            if machineSet.startswith(self.arch):
                if self._lockSet(machineSet):
                    self.machineSet = machineSet
                    return
        raise RuntimeError("Unable to acquire a machine set for arch %s" %
                (self.arch,))

    def unlockMachines(self):
        lockName = self._lockName(self.machineSet)
        if not os.access(lockName, os.R_OK):
            # Lock file no longer there...
            return
        with open(lockName, "r") as lockFile:
            for line in lockFile:
                if line.startswith("Output: "):
                    outputDirectory = re.sub(r'^Output:\s+', "", line.rstrip())
                    break
        if hasattr(self, "outputDirectory") and \
                self.outputDirectory is not None and \
                self.outputDirectory != outputDirectory:
            print >>sys.stderr, "Output directory discrepancy:", \
                    self.outputDirectory, outputDirectory
        # os.rename has problems spanning filesystems, so use shutil.move
        shutil.move(lockName, os.path.join(outputDirectory, "run", "run.log"))

    def _exec(self, command, logFile):
        try:
            subprocess.check_call(command + " >& " + logFile, shell=True)
        except subprocess.CalledProcessError:
            cmd = command.split(' ', 1)[0].split('/')[-1]
            print >>sys.stderr, "***", cmd, "failed"
            with open(logFile, "r") as log:
                try:
                    log.seek(-500, 2)
                except:
                    pass
                print >>sys.stderr, "(last 500 bytes)...", log.read(500)
            raise

    def doOrcaRun(self):
        try:
            subprocess.check_call("$CTRL_ORCA_DIR/bin/orca.py"
                    " -e env.sh"
                    " -r ."
                    " -V 30 -L 2 orca.paf " + self.runId + 
                    " >& unifiedPipeline.log",
                    shell=True, stdin=open("/dev/null", "r"))
            # TODO -- monitor orca run, looking for output changes/stalls
            # TODO -- look for MemoryErrors and bad_allocs in logs
        except subprocess.CalledProcessError:
            self._log("*** Orca failed")
            print >>sys.stderr, self.orcaStatus(self.runId,
                    self.outputDirectory)
            raise
        except KeyboardInterrupt:
            self._log("*** Orca interrupted")
            self.kill(self.runId)
            raise

    def setupCheck(self):
        tags = os.path.join(self.outputDirectory, "config", "weekly.tags")
        for env in glob.glob(os.path.join(self.outputDirectory,
            "work", "*", "eups-env.txt")):
            try:
                # Filter out SCM for buildbot
                subprocess.check_call(
                        "perl -pe 's/SCM //;' %s | diff - %s" % (tags, env),
                        shell=True)
            except subprocess.CalledProcessError:
                print >>sys.stderr, "*** Mismatched setup", env
                raise

    def doAdditionalJobs(self):
        os.mkdir("../SourceAssoc")

        self._exec("$DATAREL_DIR/bin/sst/SourceAssoc_ImSim.py"
                " -i ../output"
                " -o ../SourceAssoc"
                " -R ../output/registry.sqlite3",
                "SourceAssoc_ImSim.log")
        self._log("SourceAssoc complete")
        self._exec("$DATAREL_DIR/bin/ingest/prepareDb.py"
                " -u %s -H %s %s" %
                (self.dbUser, RunConfiguration.dbHost, self.dbName),
                "prepareDb.log")
        self._log("prepareDb complete")

        os.chdir("..")
        self._exec("$DATAREL_DIR/bin/ingest/ingestProcessed_ImSim.py"
                " -u %s -d %s"
                " output output/registry.sqlite3" %
                (self.dbUser, self.dbName),
                "run/ingestProcessed_ImSim.log")
        os.chdir("run")
        self._log("ingestProcessed complete")
        
        os.mkdir("../csv-SourceAssoc")
        self._exec("$DATAREL_DIR/bin/ingest/ingestSourceAssoc.py"
                " -m"
                " -u %s -H %s"
                " -R ../input/refObject.csv"
                " -e ../Science_Ccd_Exposure_Metadata.csv"
                " -j 1"
                " %s ../SourceAssoc ../csv-SourceAssoc" %
                (self.dbUser, RunConfiguration.dbHost, self.dbName),
                "ingestSourceAssoc.log")
        self._log("ingestSourceAssoc complete")
        self._exec("$DATAREL_DIR/bin/ingest/finishDb.py"
                " -u %s -H %s"
                " -t"
                " %s" %
                (self.dbUser, RunConfiguration.dbHost, self.dbName),
                "finishDb.log")
        self._log("finishDb complete")

    def doPipeQa(self):
        _checkWritable(RunConfiguration.pipeQaDir)
        os.environ['WWW_ROOT'] = RunConfiguration.pipeQaDir
        os.environ['WWW_RERUN'] = self.dbName
        self._exec("$TESTING_DISPLAYQA_DIR/bin/newQa.py " + self.dbName,
                "newQa.log")
        self._exec("$TESTING_PIPEQA_DIR/bin/pipeQa.py"
                " --delaySummary"
                " --forkFigure"
                " --keep"
                " --breakBy ccd"
                " " + self.dbName,
                "pipeQa.log")

    def linkLatest(self, runId):
        self.outputDirectory = os.path.join(self.options.output, runId)
        _checkReadable(self.outputDirectory)
        with open(os.path.join(self.outputDirectory, "run", "run.log")) as logFile:
            for line in logFile:
                if line.startswith("RunType:"):
                    self.options.runType = re.sub(r'^RunType:\s+', "",
                            line.rstrip())
                if line.startswith("Database:"):
                    self.dbName = re.sub(r'^Database:\s+', "", line.rstrip())
        self.doLatestLinks()

    def doLatestLinks(self):
        # TODO -- remove race conditions
        _checkWritable(self.options.output)
        latest = os.path.join(self.options.output,
                "latest_" + self.options.runType)
        if os.path.exists(latest + ".bak"):
            os.unlink(latest + ".bak")
        if os.path.exists(latest):
            os.rename(latest, latest + ".bak")
        os.symlink(self.outputDirectory, latest)
# TODO -- linkDb.py needs to be extended to take more run types
#        self._exec("$DATAREL_DIR/bin/ingest/linkDb.py"
#                " -u %s -H %s"
#                " -t %s"
#                " %s" % (self.dbUser, RunConfiguration.dbHost,
#                    self.options.runType, self.dbName), "linkDb.log")
        latest = os.path.join(RunConfiguration.pipeQaDir,
                "latest_" + self.options.runType)
        qaDir = os.path.join(RunConfiguration.pipeQaDir, self.dbName)
        if os.path.exists(qaDir):
            if os.path.exists(latest + ".bak"):
                os.unlink(latest + ".bak")
            if os.path.exists(latest):
                os.rename(latest, latest + ".bak")
            os.symlink(qaDir, latest)

    def findMachineSet(self, runId):
        for lockFileName in os.listdir(RunConfiguration.lockBase):
            with open(os.path.join(RunConfiguration.lockBase, lockFileName),
                    "r") as lockFile:
                for line in lockFile:
                    if line == "Run: " + runId + "\n":
                        return os.path.basename(lockFileName)
        return None

    def kill(self, runId):
        e = eups.Eups()
        if not e.isSetup("ctrl_orca"):
            print >>sys.stderr, "ctrl_orca not setup, using default version"
            e.setup("ctrl_orca")
        self.machineSet = self.findMachineSet(runId)
        if self.machineSet is None:
            raise RuntimeError("No current run with runId " + runId)
        self._log("*** orca killed")
        subprocess.check_call("$CTRL_ORCA_DIR/bin/shutprod.py 1 " + runId,
                shell=True)
        print >>sys.stderr, "waiting for production shutdown"
        time.sleep(10)
        print >>sys.stderr, "killing all remote processes"
        for machine in RunConfiguration.machineSets[self.machineSet]:
            machine = re.sub(r':.*', "", machine)
            processes = subprocess.Popen(["ssh", machine, "/bin/ps", "-o",
                "pid:6,command"], stdout=subprocess.PIPE)
            for line in processes.stdout:
                if line.find(runId) != -1:
                    pid = line[0:6].strip()
                    subprocess.check_call(["ssh", machine, "/bin/kill", pid])
            processes.wait()
        time.sleep(5)
        print >>sys.stderr, "unlocking machine set"
        self.unlockMachines()

    def checkForKill(self):
        with open(self._lockName(self.machineSet), "r") as lockFile:
            for line in lockFile:
                if line.find("orca killed") != -1:
                    return True
        return False

    def checkForResults(self):
        calexps = glob.glob(os.path.join(self.outputDirectory,
            "output", "calexp", "v*", "R*", "S*.fits"))
        if len(calexps) < 2:
            return False
        srcs = glob.glob(os.path.join(self.outputDirectory,
            "output", "src", "v*", "R*", "S*.boost"))
        return len(srcs) >= 2


###############################################################################
# 
# Analyze logs during/after run
# 
###############################################################################

    def analyzeLogs(self, runId, inProgress=False):
        import MySQLdb
        jobStartRegex = re.compile(
                r"Processing job:"
                r"(\s+raft=(?P<raft>\d,\d)"
                r"|\s+sensor=(?P<sensor>\d,\d)"
                r"|\s+type=calexp"
                r"|\s+visit=(?P<visit>\d+)){4}"
        )

        host = RunConfiguration.dbHost
        port = RunConfiguration.dbPort
        with MySQLdb.connect(
                host=host,
                port=port,
                user=self.dbUser,
                passwd=DbAuth.password(host, str(port))) as conn:
            runpat = '%' + runId + '%'
            conn.execute("SHOW DATABASES LIKE %s", (runpat,))
            ret = conn.fetchall()
            if ret is None or len(ret) == 0:
                raise NoMatchError("No match for run %s" % (runId,))
            elif len(ret) > 1:
                raise RuntimeError("Multiple runs match:\n" +
                        str([r[0] for r in ret]))
            dbName = ret[0][0]

        result = ""
        try:
            conn = MySQLdb.connect(
                host=host,
                port=port,
                user=self.dbUser,
                passwd=DbAuth.password(host, str(port)),
                db=dbName)

            cursor = conn.cursor()
            cursor.execute("""SELECT TIMESTAMP, timereceived FROM Logs
                WHERE id = (SELECT MIN(id) FROM Logs)""")
            row = cursor.fetchone()
            if row is None:
                if inProgress:
                    return "No log entries yet\n"
                else:
                    return "*** No log entries written\n"
            startTime, start = row
            result += "First orca log entry: %s\n" % (start,)
    
            cursor = conn.cursor()
            cursor.execute("""SELECT TIMESTAMP, timereceived FROM Logs
                WHERE id = (SELECT MAX(id) FROM Logs)""")
            stopTime, stop = cursor.fetchone()
            result += "Last orca log entry: %s\n" % (stop,)
            elapsed = long(stopTime) - long(startTime)
            elapsedHr = elapsed / 3600 / 1000 / 1000 / 1000
            elapsed -= elapsedHr * 3600 * 1000 * 1000 * 1000
            elapsedMin = elapsed / 60 / 1000 / 1000 / 1000
            elapsed -= elapsedMin * 60 * 1000 * 1000 * 1000
            elapsedSec = elapsed / 1.0e9
            result += "Orca elapsed time: %d:%02d:%06.3f\n" % (elapsedHr,
                    elapsedMin, elapsedSec)
    
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(DISTINCT workerid) FROM
                    (SELECT workerid FROM Logs LIMIT 10000) AS sample""")
            nPipelines = cursor.fetchone()[0]
            result += "%d pipelines used\n" % (nPipelines,)
    
            cursor = conn.cursor()
            cursor.execute("""
                SELECT CASE gid
                    WHEN 1 THEN 'pipeline shutdowns seen'
                    WHEN 2 THEN 'CCDs attempted'
                    WHEN 3 THEN 'src writes'
                    WHEN 4 THEN 'calexp writes'
                END AS descr, COUNT(*) FROM (
                    SELECT CASE
                        WHEN COMMENT LIKE 'Processing job:% visit=0'
                        THEN 1
                        WHEN COMMENT LIKE 'Processing job:%'
                            AND COMMENT NOT LIKE '% visit=0'
                        THEN 2
                        WHEN COMMENT LIKE 'Ending write to BoostStorage%/src%'
                        THEN 3
                        WHEN COMMENT LIKE 'Ending write to FitsStorage%/calexp%'
                        THEN 4
                        ELSE 0
                    END AS gid
                    FROM Logs
                ) AS stats WHERE gid > 0 GROUP BY gid""")
            nShutdown = 0
            for d, n in cursor.fetchall():
                result += "%d %s\n" % (n, d)
                if d == 'pipeline shutdowns seen':
                    nShutdown = n
            if nShutdown != nPipelines:
                if not inProgress:
                    if nShutdown == 0:
                        result += "\n*** No pipelines were shut down properly\n"
                    else:
                        result += "\n*** Shutdowns do not match pipelines\n"
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT workerid, COMMENT
                    FROM Logs JOIN
                    (SELECT MAX(id) AS last FROM Logs GROUP BY workerid) AS a
                    ON (Logs.id = a.last)""")
                for worker, msg in cursor.fetchall():
                    if inProgress:
                        result += "Pipeline %s last status: %s\n" % (worker,
                                msg)
                    else:
                        result += "Pipeline %s ended with: %s\n" % (worker, msg)
    
            cursor = conn.cursor()
            cursor.execute("""
SELECT COUNT(*) FROM Logs
WHERE
(
    	COMMENT LIKE '%rror%'
	OR COMMENT LIKE '%xception%'
	OR COMMENT LIKE '%arning%'
	OR COMMENT LIKE 'Fail'
	OR COMMENT LIKE 'fail'
)
AND COMMENT NOT LIKE '%failureStage%'
AND COMMENT NOT LIKE '%failure stage%'
AND COMMENT NOT LIKE 'failSerialName%'
AND COMMENT NOT LIKE 'failParallelName%'
AND COMMENT NOT LIKE 'Distortion fitter failed to improve%'
AND COMMENT NOT LIKE '%magnitude error column%'
AND COMMENT NOT LIKE '%errorFlagged%'
AND COMMENT NOT LIKE 'Skipping process due to error'
            """)
            result += "%s failures seen\n" % cursor.fetchone()

            cursor = conn.cursor(MySQLdb.cursors.DictCursor)
            cursor.execute("""
                SELECT * FROM Logs
                WHERE COMMENT LIKE 'Processing job:%'
                    OR (
                        (
                            COMMENT LIKE '%rror%'
                            OR COMMENT LIKE '%xception%'
                            OR COMMENT LIKE '%arning%'
                            OR COMMENT LIKE '%Fail%'
                            OR COMMENT LIKE '%fail%'
                        )
                        AND COMMENT NOT LIKE '%failureStage%'
                        AND COMMENT NOT LIKE '%failure stage%'
                        AND COMMENT NOT LIKE 'failSerialName%'
                        AND COMMENT NOT LIKE 'failParallelName%'
                        AND COMMENT NOT LIKE 'Distortion fitter failed to improve%'
                        AND COMMENT NOT LIKE '%magnitude error column%'
                        AND COMMENT NOT LIKE '%errorFlagged%'
                        AND COMMENT NOT LIKE 'Skipping process due to error'
                    )
                ORDER BY id;""")
            jobs = dict()
            for d in cursor.fetchall():
                match = jobStartRegex.search(d['COMMENT'])
                if match:
                    jobs[d['workerid']] = "Visit %s Raft %s Sensor %s" % (
                            match.group("visit"), match.group("raft"),
                            match.group("sensor"))
                elif not d['COMMENT'].startswith('Processing job:'):
                    if jobs.has_key(d['workerid']):
                        job = jobs[d['workerid']]
                    else:
                        job = "unknown"
                    result += "\n*** Error in %s in stage %s on %s:\n" % (
                                job, d['stagename'], d['workerid'])
                    lines = d['COMMENT'].split('\n')
                    i = len(lines) - 1
                    message = lines[i].strip()
                    # Skip blank lines at end
                    while i > 0 and message == "":
                        i -= 1
                        message = lines[i].strip()
                    # Go back until we find a traceback line with " in "
                    while i > 0 and lines[i].find(" in ") == -1:
                        i -= 1
                        message = lines[i].strip() + "\n" + message
                    result += message + "\n"

        finally:
            conn.close()

        outputDir = os.path.join(self.options.output, runId)
        logFile = os.path.join(outputDir, "run", "unifiedPipeline.log")
        with open(logFile, "r") as log:
            try:
                log.seek(-500, 2)
            except:
                pass 
            tail = log.read(500)
            if not tail.endswith("logger handled...and...done!\n"):
                result += "\n*** Unified pipeline log file\n"
                result += "(last 500 bytes)... " + tail + "\n"

        for logFile in glob.glob(
                os.path.join(outputDir, "work", "*", "launch.log")):
            with open(logFile, "r") as log:
                try:
                    log.seek(-500, 2)
                except:
                    pass
                tail = log.read(500)
                if not re.search(r"harness.runPipeline: workerid \w+$", tail) \
                        and not re.search(r"Applying aperture", tail) \
                        and tail != "done. Now starting job office\n":
                    result += "\n*** " + logFile + "\n"
                    result += "(last 500 bytes)... " + tail + "\n"

        return result

###############################################################################
# 
# Parse command-line options
# 
###############################################################################

    def parseOptions(self, args):
        parser = OptionParser("""%prog [options]

Perform an integrated production run.

Uses the current stack and setup package versions.""")

        parser.add_option("-t", "--runType", metavar="WORD",
                help="one-word ('_' allowed) description of run type (default: %default)")

        parser.add_option("-p", "--pipeline", metavar="PAF",
                help="master pipeline policy in DATAREL_DIR/pipeline"
                " (default: %default)")
        # TODO -- allow overrides of policy parameters
        # parser.add_option("-D", "--define", dest="override",
        #         metavar="KEY=VALUE",
        #         action="append",
        #         help="overrides for policy items (repeatable)")
        
        archs = set()
        self.arch = None
        machineName = self.hostname.split('.')[0]
        for machineSet, machines in RunConfiguration.machineSets.iteritems():
            a = re.sub(r'-.*', "", machineSet)
            archs.add(a)
            for machine in machines:
                if machineName == re.sub(r':.*', "", machine):
                    self.arch = a
                    break
        archs = sorted(list(archs))

        if self.arch is None:
            parser.add_option("-a", "--arch", type="choice",
                    choices=archs,
                    help="machine architecture [" + ', '.join(archs) + "]")

        parser.add_option("-L", "--listRuns", metavar="PARTIALRUNID",
                help="list available runs matching partial id and exit")
        parser.add_option("-S", "--status", dest="printStatus",
                action="store_true",
                help="print current run status and exit")
        parser.add_option("-R", "--report", metavar="RUNID",
                help="print report for RUNID and exit")
        parser.add_option("-k", "--kill", metavar="RUNID",
                help="kill Orca processes and exit")
        
        parser.add_option("-i", "--input", metavar="DIR",
                help="input dataset path (default: %default)")
        parser.add_option("-I", "--listInputs", action="store_true",
                help="list available official inputs and exit")
        parser.add_option("-n", "--ccdCount", metavar="N", type="int",
                help="run only first N CCDs (default: all)")

        parser.add_option("-o", "--output", metavar="DIR",
                help="output dataset base path (default: %default)")

        parser.add_option("-x", "--testOnly", action="store_true",
                help="do NOT link run results as the latest of its type")
        parser.add_option("-l", "--linkLatest", metavar="RUNID",
                help="link previous run result as the latest of its type and exit")
        parser.add_option("--skipPipeQa", dest="doPipeQa",
                action="store_false",
                help="skip running pipeQA")

        parser.add_option("-m", "--mail", dest="toAddress",
                metavar="ADDR",
                help="E-mail address for notifications (default: %default)")

        parser.add_option("-H", "--hosts", action="store_true",
                help="test ssh connectivity to all hosts and exit")

        input = None
        for entry in sorted(os.listdir(RunConfiguration.inputBase),
                reverse=True):
            if entry.startswith("obs_imSim"):
                input = entry
                break

        parser.set_defaults(
                runType=self.user,
                pipeline=RunConfiguration.pipelinePolicy,
                input=input,
                output=RunConfiguration.outputBase,
                doPipeQa=True,
                toAddress=RunConfiguration.toAddress)

        return parser.parse_args(args)

def main():
    configuration = RunConfiguration(sys.argv)
    configuration.check()
    configuration.run()

if __name__ == "__main__":
    main()
