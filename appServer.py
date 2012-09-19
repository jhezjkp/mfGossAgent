#encoding=utf-8

import subprocess
import threading
import os

import pyinotify

from constants import *

'''
应用服相关
'''


class AppServer:
    '''应用服'''

    def __init__(self, id, name, category, jar, vindicateJar, mainDb, statDb, path, type, pid, status, configStatus):
        #status: 0-停止 1-运行 2-维护
        self.id = id
        self.name = name
        self.category = category
        self.jar = jar
        self.vindicateJar = vindicateJar
        self.mainDb = mainDb
        self.statDb = statDb
        self.path = path
        self.type = type
        self.pid = pid
        self.status = status
        self.configStatus = configStatus
        self.error = False
        if pid <= 0:
            #清空输出文件
            subprocess.Popen('cat /dev/null > ' + os.path.join(self.path, 'app.out'), cwd=self.path, stdout=None, shell=True)
        self.logWatcher = LogWatcher(self)
        self.logWatcher.setDaemon(True)
        self.logWatcher.start()

    def __str__(self):
        return '[%s %s %s,%s,%s,%s,%s]' % (self.id, self.name, self.path, self.jar, self.type, self.status, self.pid)

    def start(self):
        '''启动应用'''
        if self.pid <= 0:
            self.logWatcher.resetWatch()
            #subprocess.Popen('cat /dev/null > ' + os.path.join(self.path, 'app.out'), cwd=self.path, stdout=None, shell=True)
            #print 'cat /dev/null > ' + os.path.join(self.path, 'app.out')
            #time.sleep(1)
            #清除异常状态
            self.error = False
            subprocess.Popen(os.path.join(self.path, 'startup.sh'), cwd=self.path, stdout=None, shell=True, close_fds=True)
        return None

    def restart(self):
        self.stop()
        self.start()

    def stop(self):
        '''停止应用'''
        if self.pid <= 0:
            return None
        else:
            subprocess.Popen(os.path.join(self.path, 'shutdown.sh'), cwd=self.path, stdout=subprocess.PIPE, shell=True)
            self.pid = -1

    def vindicate(self):
        '''运行维护程序'''
        if self.type != SERVER_GAME or self.pid > 0:
            return ILEGAL_OPERATE
        else:
            self.logWatcher.resetWatch()
            subprocess.Popen(os.path.join(self.path, 'vindicate.sh'), cwd=self.path, stdout=None, shell=True, close_fds=True)
            return SUCCESS

    def switchSyncConfig(self, configStatus):
        '''切换同步配置'''
        if self.type != SERVER_LOGIN:
            return ILEGAL_OPERATE
        cmd = "sed  -i 's/<cleanMode>[0-9]<\/cleanMode>/<cleanMode>" + str(configStatus) + "<\/cleanMode>/' " + os.path.join(os.path.join(self.path, 'conf'), 'game_config.xml')
        subprocess.check_output(["/bin/bash", "-c", cmd])
        return SUCCESS

    def getLogContent(self):
        '''获取控制台日志内容'''
        logContent = self.logWatcher.getLogContent()
        if len(logContent) == 0 and self.pid <= 0:
            return 'the server is stoped'            
        else:
            return logContent

    def getErrorLog(self):
        '''获取错误日志'''
        cmd = "grep -B 5 -A 30 'Exception' " + os.path.join(self.path, 'app.out')
        output = subprocess.check_output(["/bin/bash", "-c", cmd])
        return output

###################################


class ModificationsHandler(pyinotify.ProcessEvent):
    '''
    修改变更处理器
    '''
    def __init__(self, appServer, content):
        logFile = os.path.join(appServer.path, 'app.out')
        self.content = content
        self.logFile = logFile
        self.log = open(logFile, 'r')
        self.offset = 0
        self.appServer = appServer
        fileSize = os.stat(logFile)[6]
        if fileSize / 1024 > 1024:
            #文件大于1M
            self.offset = fileSize - 1024 * 50
        self.log.seek(self.offset)
        for line in self.log.readlines():
            if len(line) > 0:
                self.content = line + '<br/>' + self.content
                if line.find('Exception') > -1:
                    self.appServer.error = True
        self.offset = self.log.tell()

    def process_IN_MODIFY(self, event):
        if self.offset <= os.path.getsize(self.logFile):
            self.log.seek(self.offset)
        else:
            self.content = ""
            self.offset = 0
            self.log.seek(0)
        for line in self.log.readlines():
            if len(line) > 0:
                self.content = line + '<br/>' + self.content
                if line.find('Exception') > -1:
                    self.appServer.error = True
        #只保留最新的一段日志
        index = self.content.find('<br/>', 50000)
        if index > 0:
            self.content = self.content[0:index]
        self.offset = self.log.tell()

    def reset(self):
        self.content = ""
        self.offset = 0        
        subprocess.Popen('cat /dev/null > ' + self.logFile, cwd=os.path.dirname(self.logFile), stdout=None, shell=True)
        self.log = open(self.logFile, 'r')


class LogWatcher(threading.Thread):
    '''
    日志监视
    '''
    def __init__(self, appServer):
        logFile = os.path.join(appServer.path, 'app.out')
        super(LogWatcher, self).__init__()
        self.content = ""
        if not os.path.exists(logFile):
            subprocess.Popen('touch ' + logFile, cwd=os.path.dirname(logFile), stdout=subprocess.PIPE, shell=True)
        mask = pyinotify.IN_MODIFY
        self.handler = ModificationsHandler(appServer, self.content)
        wm = pyinotify.WatchManager()
        self.notifier = pyinotify.Notifier(wm, self.handler)
        wm.add_watch(logFile, mask, rec=True)

    def getLogContent(self):
        return self.handler.content

    def resetWatch(self):
        self.handler.reset()

    def run(self):
        self.notifier.loop()
