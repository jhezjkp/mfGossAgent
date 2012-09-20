#!/usr/bin/env python
#encoding=utf-8

'''
goss代理
'''
import os
import sys
import time
import subprocess
import socket
import re
import logging
import xml.dom.minidom
import threading
import base64
import xmlrpclib
from SimpleXMLRPCServer import SimpleXMLRPCServer

import zerorpc

from appServer import AppServer
from constants import *

#本地ip和监听端口
agentIp = subprocess.check_output(["/bin/bash", "-c", "/sbin/ifconfig"]).split("\n")[1].split()[1][5:]   
agentPort = 10190
#中控配置
masterIp = None
masterPort = 9999
#应用部署路径
appPath = os.path.dirname(os.path.abspath(__file__))
#应用服
appServerMap = {}
#应用部署路径
appPath = os.path.dirname(os.path.abspath(__file__))
#定时刷新应用状态的线程
refreshThread = None

def initLogger():
    '''初始化日志配置'''
    logger = logging.getLogger("agent")
    fileHandler = logging.FileHandler('agent.log')
    streamHandler = logging.StreamHandler()
    fmt = logging.Formatter("%(asctime)s, %(message)s")
    fileHandler.setFormatter(fmt)    
    streamHandler.setFormatter(fmt)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(fileHandler)
    logger.addHandler(streamHandler)
    return logger

def getProcessIdByAppName(appName):
    '''根据应用名来获取程序的进程编号'''
    pid = -1
    cmd = "ps aux | grep " + appName + " | grep -v grep | awk '{print $2}'| tr -d '\n'"
    output = subprocess.check_output(["/bin/bash", "-c", cmd])
    if len(output) > 0:
        pid = int(output)
    return pid

def registerToMaster(client):
    '''向中控注册本节点监管的所有应用'''     
    apps = []
    for app in appServerMap.values():
        apps.append((app.id, app.name, app.category, app.type, app.status, app.configStatus))        
    while(True):
        try:        
            client.register(agentIp, agentPort, apps)
            logger.info("register agent and apps success!!!")
            break
        except:
            info = sys.exc_info()            
            logger.error(info[1])
            logger.error("registerToMaster retry after 30 seconds...")
            time.sleep(30)


class RefreshThread(threading.Thread):

    def __init__(self):
        super(RefreshThread, self).__init__()
        self.logger = logging.getLogger("agent.reportor")
        #确保主线程退出时，本线程也退出        
        self.daemon = True
        #reportor
        self.reportor = xmlrpclib.ServerProxy("http://" + masterIp + ":" + str(masterPort), allow_none=True)
        registerToMaster(self.reportor)

    def run(self):
        '''检查应用当前状态(定时执行)'''        
        while(True):
            statusTupleList = []
            for app in appServerMap.values():
                status = STATUS_STOP
                pid = getProcessIdByAppName(app.jar)
                if pid > 0:
                    status = STATUS_RUN
                elif app.type == SERVER_GAME:
                    #游戏服需要再检查一下是否处于维护模式
                    pid = getProcessIdByAppName(app.vindicateJar)
                    if pid > 0:
                        status = STATUS_VINDICATE
                app.pid = pid
                app.status = status
                #logger.debug("=== 【%s】%d", app.name, app.status)
                if app.type == SERVER_LOGIN:
                    #登录服再另外检查一下配置状态
                    cmd = "sed  -n '/<cleanMode>/p' " + os.path.join(os.path.join(app.path, "conf"), "game_config.xml") + " | sed s/[[:space:]]//g | cut -c 12 | tr -d '\n'"
                    output = subprocess.check_output(["/bin/bash", "-c", cmd])
                    if re.match('[0]+', output):
                        app.configStatus = SYNC_NORMAL
                    elif re.match('[2]+', output):
                        app.configStatus = SYNC_SYNC
                statusTupleList.append((app.id, app.status, app.configStatus))    
            try:                
                if self.reportor.updateAppStatus(agentIp, agentPort, statusTupleList) == AGENT_NOT_REGISTER:
                    self.logger.info("try to register agent and apps...")
                    registerToMaster(self.reportor)
            except:
                info = sys.exc_info()  
                self.logger.error(info[1])
                self.logger.error("updateAppStatus retry after 30 seconds...")
            time.sleep(30)                


class Agent:
    '''管理代理'''

    def __init__(self):
        '''应用配置'''        
        global agentPort, masterIp, masterPort, appServerMap, timer, refreshThread
        hostname = subprocess.check_output(["/bin/bash", "-c", "hostname | tr -d '\n'"])
        config = "app_config_" + hostname + ".xml"
        logger.info("load config from " + config)
        dom = xml.dom.minidom.parse(config)
        root = dom.documentElement
        agentPort = int(root.getAttribute('agentPort'))       
        masterIp = root.getAttribute('masterIp')
        masterPort = int(root.getAttribute('masterPort'))
        for node in root.getElementsByTagName('server'):
            id = int(node.getAttribute('id'))
            name = node.getAttribute('name')
            category = int(node.getAttribute('category'))
            jar = node.getAttribute('jar')
            path = node.getAttribute('path')
            type = int(node.getAttribute('type'))
            vindicateJar = None
            dbHost = None
            dbPort = None
            dbUser = None
            dbPassword = None
            mainDb = None
            statDb = None
            if type == 2:
                #游戏服
                vindicateJar = node.getAttribute('vindicateJar')
            if type == 1 or type == 2:
                #游戏服或登录服
                dbHost = node.getAttribute('dbHost')
                dbPort = int(node.getAttribute('dbPort'))
                dbUser = node.getAttribute('dbUser')
                dbPassword = node.getAttribute('dbPassword')
                mainDb = node.getAttribute('mainDb')
                statDb = node.getAttribute('statDb')
            pid = getProcessIdByAppName(jar)
            #status: 0-停止 1-运行 2-维护(仅对游戏服)
            status = 0
            configStatus = None
            pid = getProcessIdByAppName(jar)
            if pid > 0:
                status = STATUS_RUN
            elif type == SERVER_GAME:
                #游戏服需要再检查一下是否处于维护模式
                pid = getProcessIdByAppName(vindicateJar)
                if pid > 0:
                    status = STATUS_VINDICATE
            elif type == SERVER_LOGIN:
                #登录服再另外检查一下配置状态
                cmd = "sed  -n '/<cleanMode>/p' " + os.path.join(os.path.join(path, "conf"), "game_config.xml") + " | sed s/[[:space:]]//g | cut -c 12 | tr -d '\n'"
                output = subprocess.check_output(["/bin/bash", "-c", cmd])
                if re.match('[0]+', output):
                    configStatus = SYNC_NORMAL
                elif re.match('[2]+', output):
                    configStatus = SYNC_SYNC
            app = AppServer(id, name, category, jar, vindicateJar, mainDb, statDb, path, type, pid, status, configStatus)
            app.dbHost = dbHost
            app.dbPort = dbPort
            app.dbUser = dbUser
            app.dbPassword = dbPassword
            appServerMap[id] = app
        #定时检查应用状态
        refreshThread = RefreshThread()
        refreshThread.start()


    def getAppList(self):
        '''获取应用信息'''        
        appList = []
        for app in appServerMap.values():
            appList.append((app.id, app.name, app.category, app.type, app.status))
        return appList
             

    def startApp(self, id):
        '''启动应用'''
        server = appServerMap.get(id)
        if server is None:
            return SERVER_NOT_EXIST
        else:
            #检查应用当前状态
            status = 0
            pid = getProcessIdByAppName(server.jar)
            if pid > 0:
                status = 1
            elif server.type == 2:
                #游戏服需要再检查一下是否处于维护模式
                pid = getProcessIdByAppName(server.vindicateJar)
                if pid > 0:
                    status = 2
            server.pid = pid
            server.status = status

            if pid == -1:
                #未启动则启动
                if server.type != 2:
                    server.start()
                else:
                    server.start()
            return SUCCESS


    def stopApp(self, id):
        '''停止应用'''
        server = appServerMap.get(id)
        if server is None:
            return SERVER_NOT_EXIST
        else:
            if server.type != 2:                
                #检查应用当前状态
                status = 0
                pid = getProcessIdByAppName(server.jar)
                if pid > 0:
                    status = 1
                    #已启动则停止
                    server.stop()
                server.pid = pid
                server.status = status
                return SUCCESS
            else:               
                #检查应用当前状态
                status = 0
                pid = getProcessIdByAppName(server.jar)
                if pid > 0:
                    status = 1
                    #已启动则停止
                    server.stop()
                #游戏服需要再检查一下是否处于维护模式
                pid = getProcessIdByAppName(server.vindicateJar)
                if pid > 0:
                    status = 2
                server.pid = pid
                server.status = status
                return SUCCESS 


    def vindicate(self, id):
        '''维护游戏服'''
        server = appServerMap.get(id)
        if server is None:
            return SERVER_NOT_EXIST
         #检查应用当前状态
        status = 0
        pid = getProcessIdByAppName(server.jar)
        if pid > 0:
            status = 1
        elif server.type == 2:
            #游戏服需要再检查一下是否处于维护模式
            pid = getProcessIdByAppName(server.vindicateJar)
            if pid > 0:
                status = 2
        server.pid = pid
        server.status = status
        if status == 1 or status == 2:
            #游戏服还在运行或已运行了一个维护程序实例
            return ILEGAL_OPERATE        
        server.vindicate()
        return SUCCESS


    def changeSyncConfig(self, serverId, status):
        server = appServerMap.get(serverId)
        if server is None:
            return SERVER_NOT_EXIST
        if server.type != 1:
            return ILEGAL_OPERATE        
        cmd = "sed  -i 's/<cleanMode>[0-9]<\/cleanMode>/<cleanMode>" + str(status) + "<\/cleanMode>/' " + os.path.join(os.path.join(server.path, 'conf'), 'game_config.xml')
        subprocess.check_output(["/bin/bash", "-c", cmd])
        return SUCCESS


    def getConsoleLog(self, id):
        '''查看控制台日志'''
        gs = appServerMap.get(id)
        if gs is None:
            return (SERVER_NOT_EXIST,base64.b64encode("服务器不存在"))
        else:
            logContent = gs.getLogContent()
            return (SUCCESS, base64.b64encode(logContent))


    def getErrorLog(self, id):
        '''查看错误日志'''
        gs = appServerMap.get(id)
        if gs is None:
            return (SERVER_NOT_EXIST, base64.b64encode("服务器不存在"))
        else:
            return (SUCCESS, base64.b64encode(gs.getErrorLog()))

    def switchSyncConfig(self, id, configStatus):
        '''切换同步配置'''
        gs = appServerMap.get(id)
        if gs is None:
            return SERVER_NOT_EXIST
        else:
            return gs.switchSyncConfig(configStatus)

    def updateApps(self, appIdList, fileName, binary):
        '''更新程序'''
        result = []
        logger.info("update app %s with [%s]", appIdList, fileName)
        for id in appIdList:
            app = appServerMap.get(id)
            logger.info("update 【%d-%s】 with [%s]", app.id, app.name, fileName)
            result.append((id, app.updateApp(binary)))
        return (agentIp, result)                             


if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding("utf-8")    
    logger = initLogger()    
    agent = Agent()
    server = SimpleXMLRPCServer(("0.0.0.0", agentPort), allow_none=True, logRequests=False)
    logger.info("Listening on port %d...", agentPort)
    server.register_function(agent.getAppList, "getAppList")
    server.register_function(agent.startApp, "startApp")
    server.register_function(agent.stopApp, "stopApp")
    server.register_function(agent.vindicate, "vindicate")
    server.register_function(agent.getConsoleLog, "getConsoleLog")
    server.register_function(agent.switchSyncConfig, "switchSyncConfig")
    server.register_function(agent.updateApps, "updateApps")
    try:
        server.serve_forever()
    except KeyboardInterrupt:        
        logger.info("agent exit...")