import hashlib
import time
import re
import ssl
import json
import os
import sys
import socket
from datetime import date
from bs4 import BeautifulSoup as bs
from modules.db_handler import db_handler

# define static vars
sleep_time = 3600*4
socket_timout = 10  # wait for 10 seconds before saying http timeout.
quit_scrap_num_in_db = 5
mode_debug = True
list_pages = [{
    "url": "forum-index-fid-1-typeid1-1-typeid2-0-typeid3-0-typeid4-0.htm",
    "filter": {
        "year": 0,
        "imdb": True
    },
    "name": "US/UK",
},
    {
    "url": "forum-index-fid-1-typeid1-9-typeid2-0-typeid3-0-typeid4-0.htm",
    "filter": {
        "year": 2000,
        "imdb": True
    },
    "name": "CN ML",
},
    {
    "url": "forum-index-fid-1-typeid1-3-typeid2-0-typeid3-0-typeid4-0.htm",
    "filter": {
        "year": 2000,
        "imdb": True
    },
    "name": "CN HK",
}]
tmdb_api = "c746414134916b74f0666bd9f7704b44"
db = db_handler("192.168.7.201")
user_agent = 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)'


def geturl(url):
    socket.setdefaulttimeout(socket_timout)
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    from urllib.request import urlopen

    try:
        content = urlopen(url, context=ctx).read()
        return content
    except:
        print("open and read error of url:", url, ". try again.")
        return geturl(url)


# do a look to search
while True:

    # get the right host url from btbtt.com (btbtt.com always change the host address due to know reason)
    btbtt_url = bs(geturl("http://www.btbtt.com/"), 'html.parser').find_all(
        "form")[0].get("action")
    if btbtt_url.find("http://") != -1:
        btbtt_url = btbtt_url.replace("http://", "")
    elif btbtt_url.find("https://") != -1:
        btbtt_url = btbtt_url.replace("https://", "")
    else:
        print("get btbtt URL failed")
        exit()
    btbtt_url = "http://"+btbtt_url[:btbtt_url.find("/")+1]
    if mode_debug == True:
        print("btbtt.com url is :", btbtt_url)

    for list_page in list_pages: 
        #this is a new page with a list of movie
        print("scraping url:",btbtt_url+list_page["url"])
        list_html = bs(geturl(btbtt_url+list_page["url"]), 'html.parser')
        in_db_count = 0
        for list_content in list_html.select("table[tid][lastpost]"):
            #this is a new movie in the list, need to scrap detailed information page for more info
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

            movie["year"] = int(re.compile('[^0-9]').sub('', list_content.select(
                "a.subject_type")[0].text.replace("[", "").replace("]", "")[:4]))
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
                        imdb_info = geturl("https://api.themoviedb.org/3/find/" +
                                           movie["imdb"]+"?external_source=imdb_id&api_key="+tmdb_api)
                        tmdb_rlt = json.loads(imdb_info)["movie_results"]
                        if len(tmdb_rlt) > 0:
                            movie["imdb_info"] = tmdb_rlt[0]
                            movie["year"] = int(date.fromisoformat(
                                movie["imdb_info"]["release_date"]).year)
                            movie["name"] = movie["imdb_info"]["title"]
                        else:
                            if list_page["filter"]["imdb"] == True:
                                if mode_debug == True:
                                    print(
                                        "No IMDB info from tmdb website for this movie, skipping")
                                outflag = True
                                continue
                            movie["imdb_info"] = {}
                            movie["imdb"] = False
                            movie["name"] = movie["name_chn"]
                            if movie["year"] < 1900:
                                movie["year"] += 1900
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
            file_content = geturl(file_url)
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
