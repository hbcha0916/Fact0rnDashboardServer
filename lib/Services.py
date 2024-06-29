import asyncio
import json
import time

import time
from json import loads, dumps

from web import InitWebServer
from lib import \
    UnitFunctions as unit,\
    Config,\
    Logger,\
    Pooler,\
    OpensearchInterface as osi, \
    UDPServer


conf = Config.conf
log = Logger.log()

class Services:
    dataQueue = asyncio.Queue()
    workerClientStatusQueue = asyncio.Queue()
    workerEventQueue = asyncio.Queue()
    workerMinerStatusQueue = asyncio.Queue()
    workerStartQueue = asyncio.Queue()
    openSearchQueue = asyncio.Queue()

    def __init__(self):
        self.opensearchUnit = osi.OpensearchInterface(
            workerClientStatusQueue = self.workerClientStatusQueue,
            workerEventQueue = self.workerEventQueue,
            workerMinerStatusQueue = self.workerMinerStatusQueue,
            workerStartQueue = self.workerStartQueue
            )

    async def run(self) -> None:
        tasks = []
        thread_tasks = []

        # VER 1
        try:
            tasks.append(asyncio.create_task(self.udpSocketListener()))
            tasks.append(asyncio.create_task(InitWebServer.InitWebServer().run()))
            # thread_tasks.append(asyncio.to_thread(self.dataQueueConsumeAndMakeData))
            thread_tasks.append(asyncio.to_thread(self.opensearchUnit.recvWCS))
            thread_tasks.append(asyncio.to_thread(self.opensearchUnit.recvWE))
            thread_tasks.append(asyncio.to_thread(self.opensearchUnit.recvWMS))
            thread_tasks.append(asyncio.to_thread(self.opensearchUnit.recvWS))
            await asyncio.gather(*thread_tasks)
            await asyncio.gather(*tasks)

        finally:
            for task in tasks:
                task.cancel()


    # def dataQueueConsumeAndMakeData(self) -> None:
    #     log.info("DataQueueConsumer started.")
    #     while True:
    #         if not self.dataQueue.empty():
    #             parsedData = {}
    #             data = self.dataQueue.get_nowait()
    #             #
    #             parsedData["index"] = data["topic"].lower()
    #             parsedData["datas"] = unit.modifyData(data["senddata"])
    #             parsedData["datas"]["timestamp"] = int(time.time()*1000.0)
    #             parsedData["datas"]["worker_id"] = "{}_{}".format(parsedData["datas"]["Miner Info"]["Farm"],parsedData["datas"]["Miner Info"]["Worker"])
    #             # log.info("DATA | {}".format(parsedData))
    #             self.openSearchQueue.put_nowait(parsedData)
    #             # putDataTask = asyncio.create_task(self.opensearchUnit.putReadyData(data))
    #             # putDataTask.add_done_callback(lambda fun: asyncio.create_task(self.opensearchUnit.sendData()))
    #         else:
    #             continue

    async def udpSocketListener(self) -> None:
        log.info("UDP Listener started.")
        # VER 2
        loop = asyncio.get_running_loop()
        while True:
            on_con_lost = loop.create_future()
            transport, protocol = await loop.create_datagram_endpoint(
                lambda: UDPServer.UDPProtocol(self.workerClientStatusQueue, self.workerEventQueue, self.workerMinerStatusQueue, self.workerStartQueue, on_con_lost), local_addr=(conf.UDP_socket_host, conf.UDP_socket_port))
            try:
                await on_con_lost
            finally:
                transport.close()
                log.info("UDP Reconnection..")


    async def webService(self):
        log.info("WebServer started.")
        app = Flask(__name__, template_folder='templates')
        CORS(app)
        ApiServer.ApiServer(app,self)

        app.run(host=conf.WebServer_host, port=conf.WebServer_port, debug=True if conf.DEV_MODE == "Y" else False)
