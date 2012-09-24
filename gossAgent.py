#!/usr/bin/env python
#encoding=utf-8

'''
goss代理
'''
import os
import sys
import time
import datetime
import subprocess
import re
import logging
import xml.dom.minidom
import threading
import base64
import hashlib
import shutil
import xmlrpclib
from SimpleXMLRPCServer import SimpleXMLRPCServer

from appServer import AppServer
from constants import APP_VERSION, NEED_UPDATE, SERVER_LOGIN, SERVER_GAME, AGENT_NOT_REGISTER, STATUS_RUN, STATUS_STOP, STATUS_VINDICATE, SUCCESS, SYNC_NORMAL, SYNC_SYNC, SERVER_NOT_EXIST, ILEGAL_OPERATE

#本地ip和监听端口
agentIp = subprocess.check_output(["/bin/bash", "-c", "/sbin/ifconfig"]).split("\n")[1].split()[1][5:]
agentPort = 10190
#中控配置
masterIp = None
masterPort = 9999
#调用中控的rpc client
reportor = None
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


def registerToMaster():
    '''向中控注册本节点监管的所有应用'''
    apps = []
    for app in appServerMap.values():
        apps.append((app.id, app.name, app.category, app.type, app.status, app.configStatus))
    while(True):
        try:
            if reportor.register(agentIp, agentPort, APP_VERSION, apps) == NEED_UPDATE:
                #程序需要进行更新
                os.chdir(appPath)
                output = subprocess.check_output(["/bin/bash", "-c", os.path.join(appPath, "check.sh"), appPath])
                if output.endswith("merged updates\n"):
                    args = sys.argv[:]
                    args.insert(0, sys.executable)
                    os.execv(sys.executable, args)
                    sys.exit(0)
            else:
                logger.info("register agent and apps success!!!")
            break
        except:
            info = sys.exc_info()
            logger.error(info[1])
            logger.error("registerToMaster retry after 30 seconds...")
            time.sleep(30)


def hashFile(filePath):
    '''
    计算指定文件路径的hash值
    '''
    if os.path.exists(filePath) and os.path.isfile(filePath):
        sha1obj = hashlib.sha1()
        f = open(filePath)
        try:
            for line in f:
                sha1obj.update(line)
        finally:
            f.close()
        '''
        with open(filePath, 'rb') as f:
            sha1obj = hashlib.sha1()
            sha1obj.update(f.read())
        '''
    return sha1obj.hexdigest()


def getReadableSize(sizeInbyte):
    '''获取文件大小的友好文字表示'''
    kb = sizeInbyte / 1024.0
    if kb < 1024:
        return '%.2fK' % kb
    mb = kb / 1024.0
    if mb < 1024:
        return '%.2fM' % mb
    gb = mb / 1024.0
    return '%.2fG' % gb


def wrapperUpdateGameScript(srcPath, appIdList, isDeleteScript=False):
    '''更新脚本文件'''
    #格式：[(应用编号, 需要更新的脚本数, 成功更新的脚本数),]
    result = []
    #初始化需要更新的脚本列表
    scripts = []
    log = "--------------- 初始化需要更新的脚本列表 ---------------"
    logger.info(log)
    logs = log + "<br/>"
    for f in os.listdir(srcPath):
        if os.path.isfile(os.path.join(srcPath, f)):
            logger.info(f)
            logs += str(f) + "<br/>"
            scripts.append(f)
    log = "--------------- 总计有" + str(len(scripts)) + "个脚本需要更新 -----------------"
    logger.info(log)
    logs += log + "<br/>"
    appendSuffix = datetime.datetime.now().strftime('_%Y%m%d_%H%M%S.')  # 默认文件备份后缀
    #循环更新指定游戏服脚本
    for id in appIdList:
        gs = appServerMap[id]
        log = "----------- 更新【" + gs.name + "】脚本 ---------------"
        logger.info(log)
        logs += log + "<br/>"
        updateResult = updateScript(srcPath, scripts, gs.name, os.path.join(gs.path, 'data'), appendSuffix)
        result.append((id, len(scripts), updateResult[0]))
        logs += updateResult[1]
    if isDeleteScript:
        #删除更新成功的脚本
        log = "--------------- 删除更新成功的脚本 ---------------"
        logger.info(log)
        logs += log + "<br/>"
        for script in scripts:
            os.remove(srcPath + os.sep + script)
            logger.info("delete " + srcPath + os.sep + script)
            logs += "delete " + srcPath + os.sep + script + "<br/>"
        log = "------------- 更新成功的脚本清除完毕 -------------"
        logger.info(log)
        logs += log + "<br/>"
    log = "============= 脚本更新完成 ==============="
    logger.info(log)
    logs += log
    #响应格式：([(应用编号, 需要更新的脚本数, 成功更新的脚本数),], 更新日志)
    return (result, logs)


def updateScript(srcPath, scripts, gameServer, path, appendSuffix):
    '''
    更新脚本
    <参数>
            srcPath:更新源位置
            scripts:脚本列表
            gameServer:游戏服名
            path:游戏服data文件夹路径，如/home/project/game1/data
            appendSuffix:备份文件名后缀，一般为日期+时间
    <返回值>
            result:更新日志
    '''
    successCount = 0
    result = ""
    for dirpath, dirnames, filenames in os.walk(path):
        for filename in filenames:
                    #如果待更新的文件中有该文件则进行文件(hash)比较
            if filename in scripts:
                srcLastTime = time.strftime('%Y-%m-%d %X', time.localtime(os.path.getmtime(os.path.join(srcPath, filename))))
                targetLastTime = time.strftime('%Y-%m-%d %X', time.localtime(os.path.getmtime(os.path.join(dirpath, filename))))
                if hashFile(os.path.join(dirpath, filename)) != hashFile(os.path.join(srcPath, filename)):
                    #如果文件不同则备份并更新
                    try:
                        fileName = filename.split(".")[0]
                        fileSuffix = filename.split(".")[-1]
                        #备份文件名
                        bak = fileName + appendSuffix + fileSuffix
                        #切换到脚本所在目录
                        os.chdir(dirpath)
                        #备份原文件
                        os.rename(filename, bak)
                        #复制新的脚本文件到当前目录
                        shutil.copyfile(os.path.join(srcPath, filename), os.path.join(dirpath, filename))
                        logger.info("%25s %s %s %s %s", filename, srcLastTime, ">>>>>>", targetLastTime, os.path.join(dirpath, filename))
                        result += "{:<25s} {:s} >>>>>> {:s} {:s}".format(filename, srcLastTime, targetLastTime, os.path.join(path, filename)) + "<br/>"
                        successCount += 1
                    except:
                        logger.error("update [%s] to 【%s】 failed: %s", filename, gameServer, str(sys.exc_info()[1]))
                        result += "<font color=\"red\">" + str(sys.exc_info()[0]) + str(sys.exc_info()[1]) + "</font><br/>"
                else:
                    #文件哈希值一致则只记录一下日志
                    logger.info("%25s %s %s %s %s", filename, srcLastTime, "======", targetLastTime, os.path.join(dirpath, filename))
                    result += "{:<25s} {:s} ====== {:s} {:s}<br/>".format(filename, srcLastTime, targetLastTime, os.path.join(dirpath, filename))
                    successCount += 1
    return (successCount, result)


class RefreshThread(threading.Thread):

    def __init__(self):
        super(RefreshThread, self).__init__()
        global reportor
        self.logger = logging.getLogger("agent.reportor")
        #确保主线程退出时，本线程也退出
        self.daemon = True
        #reportor初始化
        reportor = xmlrpclib.ServerProxy("http://" + masterIp + ":" + str(masterPort), allow_none=True)
        registerToMaster()

    def run(self):
        '''检查系统负载(定时执行，作为心跳包发送给master)'''
        while(True):
            cmd = "cat /proc/loadavg | cut -f1-3 -d ' ' | tr -d '\n'"
            output = subprocess.check_output(["/bin/bash", "-c", cmd])
            try:
                if reportor.updateAgentStatus(agentIp, agentPort, output) == AGENT_NOT_REGISTER:
                    self.logger.info("try to register agent and apps...")
                    registerToMaster()
            except:
                info = sys.exc_info()
                self.logger.error(info[1])
                self.logger.error("updateAgentStatus retry after 30 seconds...")
            time.sleep(30)


class DatabaseBackupThread(threading.Thread):

    def __init__(self, batchId, appIdList):
        super(DatabaseBackupThread, self).__init__()
        self.batchId = batchId
        self.appIdList = appIdList

    def run(self):
        '''具体执行数据库备份'''
        #获取备份目录
        backupPath = os.path.join(appPath, 'database')
        #开始备份数据库
        prefix = self.batchId + "_"
        appendSuffix = '_.sql'  # 默认文件备份后缀
        for id in self.appIdList:
            server = appServerMap.get(id)
            logger.info('备份【' + server.name + '】数据库开始...')
            fileName = prefix + server.mainDb + appendSuffix
            cmd = "mysqldump -u" + server.dbUser + " -p'" + server.dbPassword + "' --port=" + str(server.dbPort) + " --skip-lock-tables --default-character-set=utf8 -h " + server.dbHost + " " + server.mainDb + " > " + os.path.join(backupPath, fileName)
            logger.info('备份主库...')
            os.system(cmd)
            reportor.submitBackupResult(self.batchId, agentIp, id, fileName)
            fileName = prefix + server.statDb + appendSuffix
            cmd = "mysqldump -u" + server.dbUser + " -p'" + server.dbPassword + "' --port=" + str(server.dbPort) + " --skip-lock-tables --default-character-set=utf8 -h " + server.dbHost + " " + server.statDb + " > " + os.path.join(backupPath, fileName)
            logger.info('备份统计库...')
            os.system(cmd)
            reportor.submitBackupResult(self.batchId, agentIp, id, fileName)
            logger.info('备份' + server.name + '数据库完毕')
        return SUCCESS


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

    def getAppStatusList(self):
        '''获取应用状态信息'''
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
            statusTupleList.append((app.id, app.status, app.configStatus, app.error))
        return statusTupleList

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

    def getConsoleLog(self, id):
        '''查看控制台日志'''
        gs = appServerMap.get(id)
        if gs is None:
            return (SERVER_NOT_EXIST, base64.b64encode("服务器不存在"))
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

    def getDatabaseBackupList(self):
        '''获取数据库备份文件列表'''
        #获取备份目录
        backupPath = os.path.join(appPath, 'database')
        backupFiles = []
        for f in os.listdir(backupPath):
            if f == ".gitignore" or f.endswith(".txt"):
                continue
            size = getReadableSize(os.path.getsize(os.path.join(backupPath, f)))
            backupFiles.append((f, size))
        return backupFiles

    def backupDatabase(self, batchId, appIdList):
        '''备份应用数据库'''
        logger.info("backup database for app %s with batchId [%s]", appIdList, batchId)
        DatabaseBackupThread(batchId, appIdList).start()
        return SUCCESS

    def updateApps(self, appIdList, fileName, binary):
        '''更新程序'''
        result = []
        logger.info("update app %s with [%s]", appIdList, fileName)
        for id in appIdList:
            app = appServerMap.get(id)
            logger.info("update 【%d-%s】 with [%s]", app.id, app.name, fileName)
            result.append((id, app.updateApp(binary)))
        return (agentIp, result)

    def updateScripts(self, appIdList, fileName, binary):
        '''更新脚本'''
        result = []
        logger.info("update scripts %s with [%s]", appIdList, fileName)
        #先将文件保存到一个以script_时间命名的文件夹中
        basePath = os.path.join(appPath, "update")
        folder = os.path.join(basePath, datetime.datetime.now().strftime('script_%Y%m%d_%H%M%S'))
        os.mkdir(folder)
        if fileName.endswith(".7z"):
            f = open(os.path.join(folder, 'scriptToUpdate.7z'), "wb")
            f.write(binary.data)
            f.close()
            # 7z -y -o<输出路径> x "<7z文件绝对路径>" > /dev/null
            os.system("7z -y x -o" + folder + " \"" + os.path.join(folder, "scriptToUpdate.7z") + "\" > /dev/null")
            os.remove(os.path.join(folder, 'scriptToUpdate.7z'))
        else:
            f = open(os.path.join(folder, fileName), "wb")
            f.write(binary.data)
            f.close()
        result = wrapperUpdateGameScript(folder, appIdList, False)
        return (agentIp, result)


if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding("utf-8")
    logger = initLogger()
    agent = Agent()
    server = SimpleXMLRPCServer(("0.0.0.0", agentPort), allow_none=True, logRequests=False)
    logger.info("Listening on port %d...", agentPort)
    server.register_function(agent.getAppStatusList, "getAppStatusList")
    server.register_function(agent.startApp, "startApp")
    server.register_function(agent.stopApp, "stopApp")
    server.register_function(agent.vindicate, "vindicate")
    server.register_function(agent.getConsoleLog, "getConsoleLog")
    server.register_function(agent.getErrorLog, "getErrorLog")
    server.register_function(agent.switchSyncConfig, "switchSyncConfig")
    server.register_function(agent.updateApps, "updateApps")
    server.register_function(agent.updateScripts, "updateScripts")
    server.register_function(agent.backupDatabase, "backupDatabase")
    server.register_function(agent.getDatabaseBackupList, "getDatabaseBackupList")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("agent exit...")
