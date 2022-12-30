import boto3
import requests
import logging
import subprocess, os
from datetime import datetime
import time
from os import path

class Layout:
    def __init__(self,):
        #https://docs.python.org/3/howto/logging-cookbook.html#logging-to-multiple-destinations
        format_str="%(asctime)s %(message)s"
        format_date="%Y-%m-%d %H:%M:%S"
        logging.basicConfig(filename='/var/log/svhealthcheck.log', filemode='a',format=format_str,datefmt=format_date)
        self.client = boto3.client('cloudwatch', region_name='eu-west-1')
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        '''
        self.COLLECTOR_OUT_PORT = '5502'
        self.NORMFRONT_OUT_PORT = '5505'
        self.NORMALIZER_OUT = '5503'
        self.ENRICHMENT_IN = '5540'
        '''
        self.FILEKEEPER_WIRE = 'repo_storage_in_'
        self.INDEXING_WIRE = 'repo_indexing_in_'

    def getTCPSocketQueue(self,name, PORT):
        # ss -t sport = 22 , notice spaces near =
        outArr = subprocess.check_output(['ss', '-t', 'dport', '=', PORT, 'or', 'sport', '=', PORT]).decode(
            'ascii').strip().split('\n')
        recvQAvg, sendQAvg, recvQMax, sendQMax = 0, 0, 0, 0
        if len(outArr) > 1:
            for colRow in outArr[1:]:
                cells = colRow.split()
                if len(cells) > 2:
                    recvQAvg = recvQAvg + int(cells[1])
                    sendQAvg = sendQAvg + int(cells[2])
                    recvQMax = max(recvQMax, int(cells[1]))
                    sendQMax = max(sendQMax, int(cells[2]))
            recvQAvg = recvQAvg / (len(outArr) - 1)
            sendQAvg = sendQAvg / (len(outArr) - 1)
        return {'name': name, 'port': PORT, 'recvQAvg': recvQAvg, 'sendQAvg': sendQAvg, 'recvQMax': recvQMax,
                'sendQMax': sendQMax}
    def getUnixSocketQueue(self):
        outArr = subprocess.check_output(['ss', '-x']).decode('ascii').strip().split('\n')
        outMap = dict()
        lenOutArr = len(outArr) - 1
        if len(outArr) > 1:
            for colRow in outArr[1:]:
                cells = colRow.split()
                if len(cells) <= 4:
                    continue
                cellName = cells[4]
                if self.INDEXING_WIRE in cellName and cellName not in outMap:
                    outMap[cellName] = {'name': 'idx_' + cellName.split(self.INDEXING_WIRE)[1], 'port': 'IndexSearcherIn',
                                        'recvQAvg': int(cells[2]) / lenOutArr, 'sendQAvg': int(cells[3]) / lenOutArr,
                                        'recvQMax': int(cells[2]), 'sendQMax': int(cells[3])}
                elif self.INDEXING_WIRE in cellName and cellName in outMap:
                    existingMap = outMap[cellName]
                    outMap[cellName] = {'name': 'idx_' + cellName.split(self.INDEXING_WIRE)[1], 'port': 'IndexSearcherIn',
                                        'recvQAvg': existingMap.get('recvQAvg', 0) + int(cells[2]) / lenOutArr,
                                        'sendQAvg': existingMap.get('sendQAvg', 0) + int(cells[3]) / lenOutArr,
                                        'recvQMax': max(existingMap.get('recvQMax', 0), int(cells[2])),
                                        'sendQMax': max(existingMap.get('sendQMax', 0), int(cells[3]))}

                elif self.FILEKEEPER_WIRE in cellName and cellName not in outMap:
                    outMap[cellName] = {'name': 'fk_' + cellName.split(self.FILEKEEPER_WIRE)[1], 'port': 'FileKeeperIn',
                                        'recvQAvg': int(cells[2]) / lenOutArr, 'sendQAvg': int(cells[3]) / lenOutArr,
                                        'recvQMax': int(cells[2]), 'sendQMax': int(cells[3])}
                elif self.FILEKEEPER_WIRE in cellName and cellName in outMap:
                    existingMap = outMap[cellName]
                    outMap[cellName] = {'name': 'fk_' + cellName.split(self.FILEKEEPER_WIRE)[1], 'port': 'FileKeeperIn',
                                        'recvQAvg': existingMap.get('recvQAvg', 0) + int(cells[2]) / lenOutArr,
                                        'sendQAvg': existingMap.get('sendQAvg', 0) + int(cells[3]) / lenOutArr,
                                        'recvQMax': max(existingMap.get('recvQMax', 0), int(cells[2])),
                                        'sendQMax': max(existingMap.get('sendQMax', 0), int(cells[3]))}

        print("unixQueue >> "+str(list(outMap.values())))
        return list(outMap.values())
    def CheckRepoDown(self):
        self.logger.info("Starting metric push")
        merger_config_file = '/opt/immune/etc/config/merger/config.json'
        hostfile='/etc/hostname'
        downCount = 0
        try:
            if os.path.exists(merger_config_file) and os.path.exists(hostfile):
                try:
                    op1 = subprocess.check_output(['grep', '-c', '"alive": false', '/opt/immune/etc/config/merger/config.json']).decode('ascii').strip()
                    downCount = int(op1)
                except subprocess.CalledProcessError as e:
                    downCount = int(e.output.decode('ascii'))
                try:
                    op2 = subprocess.check_output(['cat', '/etc/hostname']).decode('ascii').strip()
                    svName=str(op2)
                except subprocess.CalledProcessError as e:
                    svName = e.output.decode('ascii')
                print("svName>",svName,"downCount>",downCount)
                if downCount !=0:
                    return {'svName': svName, 'reposDown': {'downCount': 400}}
                else:
                    return {'svName': svName, 'reposDown': {'downCount': 0}}
        except Exception as e:
            self.logger.info("Exception occured while running CheckRepoDown: ",e)
    def GetInstanceId(self) -> str:
        try:
            r=requests.get('http://169.254.169.254/latest/meta-data/instance-id')
            statuscode = r.status_code
            if statuscode!=200:
                self.logger.info("Error fetching instanceid 169.254.169.254")
            instid=r.text
            return instid
        except Exception as e:
            self.logger.info("Got Exception while fetching InstanceId:",e)
            return
    def GetIngestionEPS(self):
        NORM_FRONT_PATH = "/opt/immune/var/log/benchmarker/norm_front.log"
        STORE_HANDLER_PATH = "/opt/immune/var/log/benchmarker/store_handler.log"
        BUCKET = 10 * 60
        dateFormat = '%Y-%m-%d_%H:%M'
        dateFormat2 = '%Y-%m-%d %H:%M'

        now = int(time.time())
        endTs = now - now % 60
        startTs = endTs - BUCKET

        store_handler_logs = ''
        if path.exists(STORE_HANDLER_PATH):
            store_handler_logs = open(STORE_HANDLER_PATH).read().strip().split('\n')

        norm_front_logs = ''
        if path.exists(NORM_FRONT_PATH):
            norm_front_logs = open(NORM_FRONT_PATH).read().strip().split('\n')

        if len(norm_front_logs) > 90:
            norm_front_logs = norm_front_logs[-90:]
        if len(store_handler_logs) > 90:
            store_handler_logs = store_handler_logs[-90:]

        store_handler_eps = []
        norm_front_eps = []
        for ts in range(startTs, endTs, 60):
            prefix = datetime.utcfromtimestamp(ts).strftime(dateFormat)
            prefix2 = datetime.utcfromtimestamp(ts).strftime(dateFormat2)
            for store_handler_log in store_handler_logs:
                if prefix in store_handler_log or prefix2 in store_handler_log:
                    store_handler_eps.append(int(store_handler_log.split('actual_mps=')[1].split('; doable_mps=')[0]))

            for norm_front_log in norm_front_logs:
                if prefix in norm_front_log or prefix2 in norm_front_log:
                    norm_front_eps.append(int(norm_front_log.split('actual_mps=')[1].split('; doable_mps=')[0]))

        store_handler_agv, norm_front_avg = 0, 0
        if len(store_handler_eps) > 0:
            store_handler_agv = int(sum(store_handler_eps) / len(store_handler_eps))
        if len(norm_front_eps) > 0:
            norm_front_avg = int(sum(norm_front_eps) / len(norm_front_eps))
        ingestionEPS = [{'name': 'store_handler', 'avg': store_handler_agv, 'count': len(store_handler_eps)},
                        {'name': 'norm_front', 'avg': norm_front_avg, 'count': len(norm_front_eps)}]
        print("ingestionEPS >> ",ingestionEPS)
        return ingestionEPS

    def PushIngestionEpsMetric(self,instanceid,repoinfo):
        try:
            response=self.client.put_metric_data(
                Namespace = 'RepoIssue-eu-west-1',
                MetricData = [{
                    'MetricName': 'LowIngestionEPS',
                    'Value':450,
                    'Unit':'Count',
                    'Dimensions':[{'Name':'InstanceId','Value':instanceid},{'Name':'NodeName','Value':repoinfo['svName']},{'Name':'EpsData','Value':"LowEPS"}]
                    },])
            if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                self.logger.info("Error occured in PushIngestionEpsMetric")
            else:
                self.logger.info("PushIngestionEpsMetric ran successfully")
        except Exception as e:
            self.logger.info("Exception occured while PushIngestionEpsMetric: ",e)
            return

    def PushRepoDownMetric(self,instanceid: str,repoinfo: {}):
        ###{'svName':svName,'reposDown': {'downCount': downCount} }
        try:
            response=self.client.put_metric_data(
                Namespace= 'RepoIssue-eu-west-1',
                MetricData=[{
                    'MetricName': 'RepoDown',
                    'Value':repoinfo['reposDown']['downCount'],
                    'Unit':'Count',
                    'Dimensions':[{'Name':'InstanceId','Value':instanceid},{'Name':'NodeName','Value':repoinfo['svName']}]
                    },])
            if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                self.logger.info("Error occured in PushRepoDownMetric")
            else:
                self.logger.info("PushRepoDownMetric run successfully")
        except Exception as e:
            self.logger.info("Exception occured while PushRepoDownMetric: ",e)
            return

    def PushSocketQueueData(self, instanceid,repoinfo: {}):
        svName = "NameNotFound"
        if repoinfo['svName']:
            svName=repoinfo['svName']
        try:
            response = self.client.put_metric_data(
                Namespace='RepoIssue-eu-west-1',
                MetricData=[{
                    'MetricName': 'SocketQueueDetected',
                    'Value': 400,
                    'Unit': 'Count',
                    'Dimensions': [{'Name': 'InstanceId', 'Value': instanceid},
                                   {'Name': 'NodeName', 'Value':svName},
                                   #{'Name':'socketQueueArray','Value':str(socketQueueArr)}
                    ]
                }, ])
            if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                self.logger.info("Error occured in PushSocketQueueData")
            else:
                self.logger.info("PushSocketQueueData run successfully")
        except Exception as e:
            self.logger.info("Exception occured while running PushSocketQueueData: ", e)
            return



def main():
    COLLECTOR_OUT_PORT = '5502'
    NORMFRONT_OUT_PORT = '5505'
    NORMALIZER_OUT = '5503'
    ENRICHMENT_IN = '5540'
    MinEPS=100

    pusher = Layout()
    instid=pusher.GetInstanceId()
    repoinfo=pusher.CheckRepoDown()

    pusher.PushRepoDownMetric(instid,repoinfo) #Metric push

    colOut = pusher.getTCPSocketQueue(name='CollectorOut', PORT=COLLECTOR_OUT_PORT)
    normFrontOut = pusher.getTCPSocketQueue(name='NormFrontOut', PORT=NORMFRONT_OUT_PORT)
    normalizerOut = pusher.getTCPSocketQueue(name='NormalizerOut', PORT=NORMALIZER_OUT)
    enrichIn = pusher.getTCPSocketQueue(name='EnrichmentIn', PORT=ENRICHMENT_IN)
    socketQueueArr=[]
    socketQueueArr=pusher.getUnixSocketQueue()
    socketQueueArr.extend([colOut, normFrontOut, normalizerOut, enrichIn])
    IngestionEPS=pusher.GetIngestionEPS()
    for item in IngestionEPS:
        if item['count']<MinEPS:
            pusher.PushIngestionEpsMetric(instid,repoinfo)   #Metric push
            break
        else:
            continue
    if len(socketQueueArr) > 0:
        for ob in socketQueueArr:
            # if item in send/recv buffer is bigger than N bytes
            if ob['recvQAvg']<10000 or ob['sendQAvg']<10000:
                pusher.PushSocketQueueData(instid,repoinfo)   #Metric push
                break
            else:
                continue

if __name__ == '__main__':
    main()