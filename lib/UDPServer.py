import asyncio
import time
from json import loads
from lib import \
    UnitFunctions as unit,\
    Config,\
    Logger

conf = Config.conf
log = Logger.log()

class UDPProtocol:
    def __init__(self, workerClientStatusQueue:asyncio.Queue, workerEventQueue:asyncio.Queue, workerMinerStatusQueue:asyncio.Queue, workerStartQueue:asyncio.Queue, on_con_lost):
        self.on_con_lost = on_con_lost
        self.workerClientStatusQueue = workerClientStatusQueue
        self.workerEventQueue = workerEventQueue
        self.workerMinerStatusQueue = workerMinerStatusQueue
        self.workerStartQueue = workerStartQueue

    def connection_made(self, transport):
        self.transport = transport
        log.info('UDP Connection Open')

    def datagram_received(self, data, address):
        routeQueue = ["worker_client_status", "worker_event", "worker_miner_status", "worker_start"]
        parsedJson = loads(data)
        parsedData = {}
        parsedData["index"] = parsedJson["topic"].lower()
        parsedData["datas"] = unit.modifyData(parsedJson["senddata"])
        parsedData["datas"]["timestamp"] = int(time.time()*1000.0)
        parsedData["datas"]["worker_id"] = "{}_{}".format(parsedData["datas"]["Miner Info"]["Farm"],parsedData["datas"]["Miner Info"]["Worker"])

        if parsedData["index"] == "worker_client_status":
            self.workerClientStatusQueue.put_nowait(parsedData)
        elif parsedData["index"] == "worker_event":
            self.workerEventQueue.put_nowait(parsedData)
        elif parsedData["index"] == "worker_miner_status":
            self.workerMinerStatusQueue.put_nowait(parsedData)
        elif parsedData["index"] == "worker_start":
            self.workerStartQueue.put_nowait(parsedData)


    def connection_lost(self, exc):
        log.info('UDP Connection Lost | {}'.format(exc))
        self.on_con_lost.set_result(True)
