import asyncio
from lib import \
    UnitFunctions as unit,\
    Config,\
    Logger, \
    Services as service
conf = Config.conf
log = Logger.log()

if __name__ == "__main__":
    welcome = r"""
  __               _    _____              
 / _|             | |  |  _  |             
| |_   __ _   ___ | |_ | |/' | _ __  _ __  
|  _| / _` | / __|| __||  /| || '__|| '_ \ 
| |  | (_| || (__ | |_ \ |_/ /| |   | | | |
|_|   \__,_| \___| \__| \___/ |_|   |_| |_|                     
"""
    log.info(welcome)
    unit.ckeckConfig()
    unit.checkOpenSearch()
    asyncio.run(service.Services().run())
