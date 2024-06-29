import requests
import asyncio
import time
from opensearchpy import OpenSearch

from lib import \
    UnitFunctions as unit,\
    Config,\
    Logger,\
    OpensearchInterface

conf = Config.conf
log = Logger.log()
class Pooler:
    url = "https://explorer.fact0rn.io/api/getblockcount"
    blockCount = None
    changeTime = 99999999999
    findBlockAndSuccess = {"msg": "ready"}
    def __init__(self):
        response = requests.get(self.url)
        self.blockCount = response.json()
        self.os : OpenSearch = OpensearchInterface.OpensearchInterface().getOS_Object()

    def poolingGetBlockFN(self):
        while True:
            response = requests.get(self.url)
            if self.blockCount != response.json():
                self.blockCount = response.json()
                self.changeTime = time.time()
                log.info("ChangedBlock\'{}\'".format(self.blockCount))
            time.sleep(5)

    def getBlockCount(self):
        return self.blockCount

    def getChangeTime(self):
        return self.changeTime

    def getFindBlockAndSuccess(self):
        return self.findBlockAndSuccess

    def poolingFindBlockAndSuccess(self) -> None:
        while True:
            events = ["Complete Factorization", "Find block"]
            eventsMaxSizes = {}
            # get MaxSize
            for event in events:
                query = {
                    "query": {
                        "bool": {
                            "must": [
                                {"match": {"Event": event}}
                            ]
                        }
                    }
                }
                response = self.os.count(
                    body=query,
                    index="worker_event"
                )

                eventsMaxSizes[event] = response["count"]


            returnData = {'complete': {}, "find": {}}
            terms = [1, 7, 30]
            # get Total
            for event in events:
                query = {
                    "query": {
                        "bool": {
                            "must": [
                                {"match": {"Event": event}}
                            ]
                        }
                    },
                    "sort": [
                        {"timestamp": {"order": "desc"}}
                    ],
                    "size": eventsMaxSizes[event]
                }
                response = self.os.search(
                    body=query,
                    index="worker_event"
                )

                if event == "Complete Factorization":
                    tmp = []
                    for i in response['hits']['hits']:
                        tmp.append(float(i['_source']['Message'].split("Factorization ")[-1].split("/")[-1]))

                    try:
                        returnData['complete']['total'] = {"count": len(tmp), "avg": round(sum(tmp) / len(tmp), 2)}
                    except ZeroDivisionError:
                        returnData['complete']['total'] = {"count": 0, "avg": 0}

                elif event == "Find block":
                    tmp = []
                    returnData['find']['total'] = []
                    failCount = 0
                    for i in response['hits']['hits']:
                        tmp.append(i['_source']['Message'])
                        raceStatus = unit.isRaceSuccess(
                            i['_source']['Message'].split("Height: ")[-1].split(" ")[0],
                            i['_source']['Message'].split("Nonce: ")[-1].split(" ")[0]
                        )
                        if "failNonce" in raceStatus:
                            failCount += 1

                        returnData['find']['total'].append(
                            {
                                "Worker": i['_source']['Miner Info']['Worker'],
                                "Height": i['_source']['Message'].split("Height: ")[-1].split(" ")[0],
                                "Nonce": i['_source']['Message'].split("Nonce: ")[-1].split(" ")[0],
                                "RaceStatus": raceStatus
                            }
                        )

                    returnData['find']['total'].append(
                        {
                            "info": {
                                "FindCount": len(tmp),
                                "SuccessCount": len(tmp) - failCount,
                                "FailCount": failCount
                            }
                        }
                    )
            # get Term
            for term in terms:
                for event in events:
                    query = {
                        "query": {
                            "bool": {
                                "must": [
                                    {"match": {"Event": event}}
                                ],
                                "filter": [
                                    {
                                        "range": {
                                            "timestamp": {
                                                "gte": "now-"+str(term)+"d/d",
                                                "lte": "now/d"
                                            }
                                        }
                                    }
                                ]
                            }
                        },
                        "sort": [
                            {"timestamp": {"order": "desc"}}
                        ],
                        "size": eventsMaxSizes[event]
                    }
                    response = self.os.search(
                        body=query,
                        index="worker_event"
                    )

                    if event == "Complete Factorization":
                        tmp = []
                        for i in response['hits']['hits']:
                            tmp.append(float(i['_source']['Message'].split("Factorization ")[-1].split("/")[-1]))
                        try:
                            returnData['complete']["{}d".format(term)] = {"count": len(tmp), "avg": round(sum(tmp)/len(tmp),2)}
                        except ZeroDivisionError:
                            returnData['complete']["{}d".format(term)] = {"count": 0,
                                                                          "avg": 0}
                    elif event == "Find block":
                        tmp = []
                        returnData['find']["{}d".format(term)] = []
                        failCount = 0
                        for i in response['hits']['hits']:
                            tmp.append(i['_source']['Message'])
                            raceStatus = unit.isRaceSuccess(
                                        i['_source']['Message'].split("Height: ")[-1].split(" ")[0],
                                        i['_source']['Message'].split("Nonce: ")[-1].split(" ")[0]
                                    )
                            if "failNonce" in raceStatus:
                                failCount+=1

                            returnData['find']["{}d".format(term)].append(
                                {
                                    "Worker": i['_source']['Miner Info']['Worker'],
                                    "Height": i['_source']['Message'].split("Height: ")[-1].split(" ")[0],
                                    "Nonce": i['_source']['Message'].split("Nonce: ")[-1].split(" ")[0],
                                    "RaceStatus": raceStatus
                                }
                            )
                        returnData['find']["{}d".format(term)].append(
                            {
                                "info":{
                                    "FindCount": len(tmp),
                                    "SuccessCount": len(tmp) - failCount,
                                    "FailCount": failCount
                                }
                            }
                        )
            self.findBlockAndSuccess = returnData
            time.sleep(30)
