import requests
import time
import hashlib
from bs4 import BeautifulSoup
from tinydb import TinyDB, Query
import langid
from win32com.client import Dispatch

thunder = Dispatch('ThunderAgent.Agent64.1')
TinyDB.DEFAULT_TABLE_KWARGS = {'cache_size': 0}
db = TinyDB('db.json')

charset = "gb2312"
url_host = "https://www.dytt8.net"
url_sub = ["/html/gndy/china/index.html", "/html/gndy/oumei/index.html"]
scan_count = 10  # scan only the top 5 movie of each page
movies = []
time_sleep = 3600*4

while True:
    for url in url_sub:
        try:
            page = requests.get(url_host + url)
        except requests.ConnectionError:
            continue
        listpage = BeautifulSoup(page.content.decode(
            charset, 'ignore'), 'html.parser')

        scan_counter = 0
        for linkmovie in listpage.select("div.co_content8 table a.ulink"):
            if linkmovie.get('href')[-10:] == "index.html":
                continue
            scan_counter += 1
            if scan_counter > scan_count:
                break
            print("processing : ", linkmovie.text)
            movie = {}
            tlink = str(linkmovie.get('href'))
            movie["dyttid"] = tlink[tlink.rfind("/")+1:tlink.rfind(".")]
            movie["stat_down"] = 0
            if len(db.search(Query().dyttid == movie["dyttid"])) > 0:
                print("dyttid:", movie["dyttid"], "already in database.")
                continue

            try:
                moviepage = requests.get(
                    url_host + linkmovie.get('href'))
            except requests.ConnectionError:
                continue

            moviepage = BeautifulSoup(moviepage.content.decode(
                charset, 'ignore'), 'html.parser')

            for movietag in str(moviepage.select("div#Zoom p")[0]).replace("<br />", "<br>").replace("<br/>", "<br>").split("<br>"):
                if movietag.find("译") != -1 and movietag.find("名") != -1 and movietag.find("名") - movietag.find("译") < 4 and movietag.find("名") - movietag.find("译") > 1 and not ("name_chn" in movie):
                    movie["name_chn"] = movietag[movietag.find("名")+1:].strip()
                    if movie["name_chn"].find("/") != -1:
                        movie["name_chn"] = movie["name_chn"].split("/")
                    else:
                        movie["name_chn"] = [movie["name_chn"]]
                if movietag.find("片") != -1 and movietag.find("名") != -1 and movietag.find("名") - movietag.find("片") < 4 and movietag.find("名") - movietag.find("片") > 1 and not ("name" in movie):
                    movie["name"] = movietag[movietag.find("名")+1:].strip()
                    if movie["name"].find("/") != -1:
                        movie["name"] = movie["name"].split("/")
                    else:
                        movie["name"] = [movie["name"]]
                if movietag.find("年") != -1 and movietag.find("代") != -1 and movietag.find("代") - movietag.find("年") < 4 and movietag.find("代") - movietag.find("年") > 1 and not ("year" in movie):
                    movie["year"] = int(
                        movietag[movietag.find("代")+1:].strip())
                if movietag.find("href") != -1 and movietag.find("magnet") != -1 and not ("magnet" in movie):
                    href = BeautifulSoup(movietag, "html.parser")
                    movie["magnet"] = href.find("a").get("href")
                    if movie["magnet"].find("&") != -1:
                        movie["magnet"] = movie["magnet"][:movie["magnet"].find(
                            "&")]

            if ("name_chn" in movie) and (langid.classify(movie["name_chn"][0])[0] != 'zh') and (langid.classify(movie["name"][0])[0] == 'zh'):
                # switch chinese and english name if a opposite name dict is detected
                print("switching name_chn and name.")
                tmp = movie["name"]
                movie["name"] = movie["name_chn"]
                movie["name_chn"] = tmp

            movie["downlink"] = moviepage.select(
                "div#Zoom p table a")[0].get("href")
            movie["name_folder"] = movie["name"][0].replace(
                " ", "_").replace(
                "#", "_").replace(
                ":", "_").replace(
                "~", "_") + "(" + str(movie["year"]) + ")"

            print(movie)
            db.insert(movie)

        print("Page search complete! start downloading")
        for task in db.search(Query().stat_down == 0):
            print(task)
            thunder.AddTask(task["downlink"])
            thunder.CommitTasks()
            db.update({"stat_down": 1}, Query().dyttid == task["dyttid"])
            time.sleep(2)
    print("all scan finished. wait for ", time_sleep, "s")
    time.sleep(time_sleep)
