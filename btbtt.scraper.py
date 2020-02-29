import hashlib
import time
import re
import ssl
import json
import os
import sys
import socket
import requests
import eventlet
from datetime import datetime, date
from bs4 import BeautifulSoup as bs
from modules.db_handler import db_handler
from config import list_pages, dburl
from pprint import pprint

# define static vars
sleep_time = 3600*4
socket_timout = 10  # wait for 10 seconds before saying http timeout.
quit_scrap_num_in_db = 5
mode_debug = True

tmdb_api = "c746414134916b74f0666bd9f7704b44"
#tmdb_url = "https://api.themoviedb.org/3/movie/"
tmdb_url = "https://z4vrpkijmodhwsxzc.stoplight-proxy.io/3/movie/"
db = db_handler(dburl)
user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'
headers = {'User-Agent': user_agent}
eventlet.monkey_patch()

def geturl(url, retry=5, raw = False, decode = True):
    if retry == 0:
        return False
    content = False
    try:
        with eventlet.Timeout(10):
            content = requests.get(url, verify=False, headers=headers)
            if(raw):
                return content.raw
            else:
                if(decode):
                    return content.content.decode()
                else:
                    return content.content
    except:
        print("open and read error of url:", url, ". try again.")
        return geturl(url, retry-1)


# do a look to search
while True:

    # get the right host url from btbtt.com (btbtt.com always change the host address due to know reason)
    #btbtt_url = bs(geturl("http://www.btbtt.com/"), 'html.parser').find_all("form")[0].get("action")
    tmp_content = geturl("https://www.ddos4.com:3601/?u=http://www.btbtt.com/")
    tmp_pos = tmp_content.find("var strU")
    tmp_pos2 = tmp_content.find("baidu",tmp_content.find("var strU2"))
    tmp_script = "function add(){ "+tmp_content[tmp_pos:tmp_pos2]+" return strU;}"
    import execjs
    ctx = execjs.compile(tmp_script)

    btbtt_url = ctx.call("add")+"/"

    if mode_debug == True:
        print("btbtt.com url is :", btbtt_url)

    for list_page in list_pages: 
        # this is a new page with a list of movie
        print("scraping url:",btbtt_url+list_page["url"])
        list_html = bs(geturl(btbtt_url+list_page["url"]), 'html.parser')
        in_db_count = 0
        for list_content in list_html.select("table[tid][lastpost]"):
            # this is a new movie in the list, need to scrap detailed information page for more info
            list_content = bs(str(list_content), 'html.parser')
            btbtt_id = list_content.table["tid"]
            print("processing tid:", btbtt_id)
            movie = {
                "platform": "btbtt.com",
                "pid": btbtt_id
            }
            movie["imdb"] = False
            
            if(db.InDB(movie)<0):
                in_db_count+=1
                print("already in db by platform name. skipping time",in_db_count)
                if(in_db_count > quit_scrap_num_in_db) :
                    print("found",quit_scrap_num_in_db,"times in db. skipping this url.")
                    break
            try:
                movie["year"] = int(re.compile('[^0-9]').sub('', list_content.select(
                "a.subject_type")[0].text.replace("[", "").replace("]", "")[:4]))
            except:
                print("movie year parse failed.")
                continue
                
            if "filter" in list_page and "year" in list_page["filter"] and movie["year"] < list_page["filter"]["year"]:
                if mode_debug == True:
                    print("filter year not match. skipping this movie.")
                continue
            movie["url"] = list_content.select("a.subject_link")[0]["href"]
            movie_content = bs(geturl(movie["url"]), 'html.parser')

            mcs = movie_content.select("td.post_td")

            outflag = False
            for mc in mcs:
                if len(mc.select("div.message")) == 0:
                    continue

                title_arr = re.compile(
                    '\]\s*\[').sub('|', mc.select("h2")[0].text.replace(r'\s*', '')).split("|")
                movie["name_chn"] = title_arr[4]

                # print("imdb:",re.compile('^tt[0-9]+').search(mc.text).group(0))
                imdb = re.search('tt[0-9]+', mc.text)
                if (imdb is None) and ("imdb" in list_page["filter"]) and (list_page["filter"]["imdb"] == True):
                    if mode_debug == True:
                        print("No IMDB for this movie, skipping")
                    outflag = True
                    break
                else:
                    if imdb is None:
                        if mode_debug == True:
                            print("no IMDB for this movie.")
                        movie["imdb"] = False
                        movie["name"] = movie["name_chn"]
                        if movie["year"] < 1900:
                            movie["year"] += 1900
                    else:
                        movie["imdb"] = imdb.group(0)
                        if mode_debug == True:
                            print("IMDB:", movie["imdb"])
                        imdb_info = geturl(tmdb_url +
                                           movie["imdb"]+"?language=en-US&api_key="+tmdb_api)
                        if imdb_info == False:
                            outflag = True
                            continue

                        tmdb_rlt = json.loads(imdb_info)
                        if "status_code" in tmdb_rlt and tmdb_rlt["status_code"]==34:
                            if list_page["filter"]["imdb"] == True:
                                if mode_debug == True:
                                    print("No IMDB info from tmdb website for this movie, skipping")
                                outflag = True
                                continue
                            movie["imdb_info"] = {}
                            movie["imdb"] = False
                            movie["name"] = movie["name_chn"]
                            if movie["year"] < 1900:
                                movie["year"] += 1900
                        else:
                            if "genres" in list_page["filter"] :
                                if list_page["filter"]["genres"]["type"]=="skip":
                                    skipflag=False
                                    for skip in list_page["filter"]["genres"]["list"]:
                                        for genre in tmdb_rlt["genres"]: 
                                            if skip == genre["id"]:
                                                skipflag=True
                                                break
                                        if skipflag==True: break
                                    if skipflag==True: 
                                        outflag = True
                                        continue
                                elif list_page["filter"]["genres"]["type"]=="contain":
                                    skipflag=True
                                    for contain in list_page["filter"]["genres"]["list"]:
                                        for genre in tmdb_rlt["genres"]: 
                                            if contain == genre["id"]:
                                                skipflag=False
                                                break
                                        if skipflag==False: break
                                    if skipflag==True: 
                                        outflag = True
                                        continue

                            movie["imdb_info"] = tmdb_rlt
                            movie["year"] = int(movie["imdb_info"]["release_date"][:4])
                            movie["name"] = movie["imdb_info"]["title"]
                            
            if outflag == True:
                continue

            # start to download torrent file.
            dla = movie_content.select("a.ajaxdialog[href*='attach-dialog']")
            if len(dla) == 0:
                if mode_debug == True:
                    print("No torrent link found, skip this movie")
                continue
            file_name = dla[0].text
            file_tmp_url = dla[0]["href"]
            file_url = btbtt_url+"attach-download-" + \
                re.search(r'fid\-[0-9]+\-aid\-[0-9]+',
                          file_tmp_url).group(0)+".htm"
            file_content = geturl(file_url,decode = False)
            movie["torrent"] = {
                "name": file_name,
                "url": file_url,
                "content": file_content
            }

            if mode_debug == True:
                print("finish for movie ", btbtt_id)
            rlt = db.InsertMovie(movie)
            if rlt == -1:
                in_db_count+=1
                print("same movie from platform id found in db.")
            elif rlt == -2:
                print("same movie from imdb id found in db.")
            elif rlt == 1:
                print("movie insert to db successfully.")
                in_db_count = 0
            time.sleep(1)

        if outflag == True:
            continue

    print("All scan finished, sleep for", sleep_time, "seconds")
    time.sleep(sleep_time)


# todo
# 1. query before scraping imdb --done
# 2. if 5 movie already in, quit this circle. --done
# 3. timeout for urllib --- done
