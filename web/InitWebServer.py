import asyncio

from quart import Quart, render_template, request, redirect
from quart_cors import cors
from web import ApiServer, ViewServer
from lib import \
    UnitFunctions as unit,\
    Config,\
    Logger

conf = Config.conf
log = Logger.log()
class InitWebServer:
    def __init__(self):
        pass

    async def run(self):
        app = Quart(__name__, template_folder='templates')
        app = cors(app, allow_origin="*")
        ApiServer.ApiServer(app)
        ViewServer.ViewServer(app)

        await app.run_task(host=conf.WebServer_host, port=conf.WebServer_port)
