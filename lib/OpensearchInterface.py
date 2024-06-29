import asyncio
from opensearchpy import OpenSearch, ConnectionPool, Connection
from lib import \
    UnitFunctions as unit,\
    Config,\
    Logger

conf = Config.conf
log = Logger.log()

class OpensearchInterface:
    readyDatas = []
    lock = asyncio.Lock()
    def __init__(self, workerClientStatusQueue:asyncio.Queue = None, workerEventQueue:asyncio.Queue = None, workerMinerStatusQueue:asyncio.Queue = None, workerStartQueue:asyncio.Queue = None):
        self.os = OpenSearch(
            hosts=[{"host": conf.OS_host, "port": conf.OS_port}],
            verify_certs=False
        )
        if workerClientStatusQueue:
            self.workerClientStatusQueue = workerClientStatusQueue

        if workerEventQueue:
            self.workerEventQueue = workerEventQueue

        if workerMinerStatusQueue:
            self.workerMinerStatusQueue = workerMinerStatusQueue

        if workerStartQueue:
            self.workerStartQueue = workerStartQueue
        
    def recvWCS(self):
        log.info("WorkerClientStatusConsumer started.")
        while True:
            if not self.workerClientStatusQueue.empty():
                data = self.workerClientStatusQueue.get_nowait()
                self.putData(data)

    def recvWE(self):
        log.info("WorkerEventConsumer started.")
        while True:
            if not self.workerEventQueue.empty():
                data = self.workerEventQueue.get_nowait()
                self.putData(data)

    def recvWMS(self):
        log.info("WorkerMinerStatusConsumer started.")
        while True:
            if not self.workerMinerStatusQueue.empty():
                data = self.workerMinerStatusQueue.get_nowait()
                self.putData(data)

    def recvWS(self):
        log.info("WorkerStartConsumer started.")
        while True:
            if not self.workerStartQueue.empty():
                data = self.workerStartQueue.get_nowait()
                self.putData(data)

    def putData(self, data):
        try:
            # print(data)
            self.os.index(index=data['index'], body=data['datas'])
            # log.info("Successful UDP data transfer")
        except KeyError as k:
            log.error("\"{}\" KeyError".format(k))

        except Exception as e:
            log.error("Error inserting data into OpenSearch: {}".format(e))

    # def recvData(self):
    #     log.info("OpensearchQueueConsumer started.")
    #     while True:
    #         if not self.dataQueue.empty():
    #             data = self.dataQueue.get_nowait()
    #             # log.info("OpensearchQueue {}".format(data))
    #             try:
    #                 self.os.index(index=data['index'], body=data['datas'])
    #                 # log.info("Successful UDP data transfer")
    #             except KeyError as k:
    #                 log.error("\"{}\" KeyError".format(k))

    #             except Exception as e:
    #                 log.error("Error inserting data into OpenSearch: {}".format(e))

    async def sendGetQuery(self, query, index="worker_client_status", type="search"):
        if type=="search":
            response = self.os.search(
                body=query,
                index=index
            )
        elif type=="count":
            response = self.os.count(
                body=query,
                index=index
            )
        return response


    def getOS_Object(self):
        return self.os