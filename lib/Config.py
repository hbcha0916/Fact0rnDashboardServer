import yaml
import urllib.request
from pathlib import Path
from types import SimpleNamespace
import os

confOri = yaml.safe_load(Path('/opt/Fact0rnDashboardServer2/conf.yml').read_text())
conf = {}
for confN in confOri.keys():
    tmpName = confN.upper()
    tmp = os.getenv(tmpName, confOri[confN])
    conf[confN] = tmp
    confOri[confN] = tmp

if confOri["DEV_MODE"] == "N":
    external_ip = conf['MY_DDNS']

    conf['IP_ADDR'] = external_ip
else:
    conf['IP_ADDR'] = "127.0.0.1"

conf = SimpleNamespace(**conf)