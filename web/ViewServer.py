from quart import Quart, render_template, request, redirect
from lib import \
    UnitFunctions as unit,\
    Config,\
    Logger

conf = Config.conf
log = Logger.log()

class ViewServer:
    def __init__(self, app:Quart):
        log.info("View Server started.")

        @app.route("/")
        @app.route("/dashboard")
        async def rootPage():
            return await render_template("rootPage.html", ip_addr=conf.IP_ADDR,
                                   web_port=str(conf.WebServer_port))

        @app.route("/dashboard-detail/<string:workerID>")
        async def dashboardDetail(workerID):
            return await render_template("dashboardDetail.html", node=workerID, ip_addr=conf.IP_ADDR,
                                   web_port=str(conf.WebServer_port))
