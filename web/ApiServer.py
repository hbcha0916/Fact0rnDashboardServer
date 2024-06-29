from datetime import datetime
import subprocess
from quart import Quart, render_template, request, redirect
import threading
import time
from lib import \
    UnitFunctions as unit,\
    Config,\
    Logger,\
    OpensearchInterface,\
    Pooler

conf = Config.conf
log = Logger.log()

class ApiServer:
    os = OpensearchInterface.OpensearchInterface()
    def __init__(self, app:Quart):
        log.info("API Server started.")
        self.pooler = Pooler.Pooler()
        threading.Thread(target=self.pooler.poolingGetBlockFN, args=()).start()
        # 현재 워커들을 출력(keyword가 worker_id) <삭제예정(유일키는 worker 이어야 함, node의 그룹을 변경할 경우 죽은 노드로됨.)
        threading.Thread(target=self.pooler.poolingFindBlockAndSuccess, args=()).start()

        @app.route("/api/getWorkers", methods=["GET"])
        async def getWorkers():
            query = {
                "size": 0,
                "aggs": {
                    "unique_worker_ids": {
                        "terms": {
                            "field": "worker_id",
                            "size": 10000  # 고유한 worker_id의 최대 개수를 지정합니다. 필요에 따라 조정할 수 있습니다.
                        }
                    }
                }
            }
            response = await self.os.sendGetQuery(query)
            unique_worker_ids = [bucket["key"] for bucket in response["aggregations"]["unique_worker_ids"]["buckets"]]
            unique_worker_ids.sort()
            return {"result": unique_worker_ids}

        # 현재 워커들을 출력(keyword가 worker)
        @app.route("/api/getWorkerNodes", methods=["GET"])
        async def getWorkerNodes():
            query = {
                "size": 0,
                "aggs": {
                    "unique_worker_ids": {
                        "terms": {
                            "field": "worker_id",
                            "size": 10000  # 고유한 worker_id의 최대 개수를 지정합니다. 필요에 따라 조정할 수 있습니다.
                        }
                    }
                }
            }
            response = await self.os.sendGetQuery(query)
            unique_worker_ids = [bucket["key"] for bucket in response["aggregations"]["unique_worker_ids"]["buckets"]]
            unique_worker_ids.sort()
            return {"result": unique_worker_ids}

        # 워커Detail을 받아와 구조화로 데이터 변경
        @app.route("/api/struData", methods=["POST"])
        async def struData():
            payload = await request.get_json()
            data = await unit.workersDetail_to_StruDetail(payload)
            return {"result": data}

        # 워커ID를 링크에 입력하면 해당 워커ID의 상세정보 출력
        @app.route("/api/getWorkerDetails/<string:workerID>", methods=["GET"])
        async def getWorkerDetail(workerID):
            returnData = {}
            query = {
                "size": 1,
                "query": {
                    "bool": {
                        "must": [
                            {"match": {"worker_id": workerID}}
                            # (구){"match": {"worker_id": worker}}
                        ]
                    }
                },
                "sort": [
                    {"timestamp": {"order": "desc"}}
                ]
            }
            response = await self.os.sendGetQuery(query)
            returnData[workerID] = response["hits"]["hits"][0]["_source"]
            return {"result": returnData}

        # 워커ID들을 리스트로 받아 모든 워커ID의 상세정보 출력
        @app.route("/api/getWorkersDetails", methods=["POST"])
        async def getWorkersDetails():
            payload = await request.get_json()
            data = {}

            for worker in payload:
                returnData = {}
                query = {
                    "size": 1,
                    "query": {
                        "bool": {
                            "must": [
                                {"match": {"worker_id": worker}}
                                # (구){"match": {"worker_id": worker}}
                            ]
                        }
                    },
                    "sort": [
                        {"timestamp": {"order": "desc"}}
                    ]
                }
                response = await self.os.sendGetQuery(query)
                returnData[worker] = response["hits"]["hits"][0]["_source"]
                data.update(returnData)
            return {"result": data}

        # 현재 Fact0rnBlockCount 출력
        # @app.route("/api/getBlockCount", methods=["GET"])
        # async def getBlockCount():
        #     data = Fact0rnAPISender.Fact0rnAPISender().getBlockCount()
        #     return jsonify(dict({"result": data}))

        # 현재 서버 시작 이후에 블럭이 바뀌면 바뀐 이후의 경과시간
        @app.route("/api/getCurrentBlockInfo", methods=["GET"])
        async def getCurrentBlockInfo():
            blockCount = self.pooler.getBlockCount()
            overTime = time.time() - self.pooler.getChangeTime()
            if overTime < 0:
                overTime = "The block hasn't changed since the server started.\nKeep the server in the on state."
            data = dict({"blockCount": blockCount, "overTime": overTime})
            return {"result": data}

        @app.route("/api/getFindBlockAndSuccess", methods=["GET"])
        async def getFindBlockAndSuccess():
            data = self.pooler.getFindBlockAndSuccess()
            return {"result": data}

        @app.route("/api/getEventLogs", methods=["GET"])
        async def getEventlogsAll():
            query = {
                "_source": ["Message", "worker_id", "timestamp"],

                "sort": [
                    {"timestamp": {"order": "desc"}}
                ],
                "size": 100
            }
            logs = []
            response = await self.os.sendGetQuery(query, index="worker_event")
            try:
                for data in response['hits']['hits']:
                    dt_object = datetime.fromtimestamp(data['_source']['timestamp'] / 1000.0)
                    formatted_date = dt_object.strftime('%Y/%m/%d %H:%M:%S.%f')[:-3]
                    logs.append("[{}] {} | {}".format(formatted_date, data['_source']['worker_id'],
                                                      data['_source']['Message']))
                return {"result": logs, "lastTime": response['hits']['hits'][0]['_source']['timestamp']}
            except KeyError:
                return {"result": ["No events yet!"], "lastTime": 0}
            except IndexError:
                return {"result": ["No events yet!"], "lastTime": 0}

        @app.route("/api/getEventLogs/<string:timestamp>", methods=["GET"])
        async def getEventlogs(timestamp):
            query = {
                "_source": ["Message", "worker_id", "timestamp"],
                "query": {
                    "bool": {
                        "filter": [
                            {
                                "range": {
                                    "timestamp": {
                                        "gt": timestamp
                                    }
                                }
                            }
                        ]
                    }
                },
                "sort": [
                    {"timestamp": {"order": "desc"}}
                ],
                "size": 100
            }
            response = await self.os.sendGetQuery(query, index="worker_event")
            logs = []
            try:
                for data in response['hits']['hits']:
                    dt_object = datetime.fromtimestamp(data['_source']['timestamp'] / 1000.0)
                    formatted_date = dt_object.strftime('%Y/%m/%d %H:%M:%S.%f')[:-3]
                    logs.append(
                        "[{}] {} | {}".format(formatted_date, data['_source']['worker_id'],
                                              data['_source']['Message']))

                return {"result": logs, "lastTime": response['hits']['hits'][0]['_source']['timestamp']}
            except KeyError:
                return {"result": [], "lastTime": 0}
            except IndexError:
                return {"result": [], "lastTime ": 0}
            
        @app.route("/api/update", methods=["POST"])
        async def updateServer():
            command = 'git pull origin master'
            subprocess.run([command], cwd="/opt/Fact0rnDashboardServer2/fact0rnDashboardServer")
            exit(0)
            return {"result": True}