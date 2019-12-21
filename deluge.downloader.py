import time
import hashlib
import os, stat, sys
from modules.db_handler import db_handler
from datetime import datetime

db = db_handler("192.168.7.201")
sleep_time = 60
tdir = "/mnt/download/torrents/" # dir for torrent
fdir = "/mnt/data/TempDownload/" # dir for downloaded files
idir = "/mnt/download/incomplete/" # dir
ndir = "/mnt/data/share/public/Entertain/Movies/MonthlyCategory/"
while True:
    for task in db.FindMovie(downstat=0):
        if task["platform"] == "btbtt.com":
            fn = tdir+task["torrent"]["name"]
            print(fn)
            f=open(fn,"wb")
            f.write(task["torrent"]["content"])
            f.close()
        elif task["platform"] == "dytt8.net":
            print("implementing...")
        
        db.UpdateMovie(task["_id"],{"$set":{"downstat":1}})

    print("add movie task done. check add status.")
    for task in db.FindMovie(downstat=1):
        fn = tdir+task["torrent"]["name"]
        if not os.path.exists(fn):
            print(task["torrent"]["name"],"already added.")
            db.UpdateMovie(task["_id"],{"$set":{"downstat":2}})
    
    print("check add status done. check downloading files")
    for task in db.FindMovie(downstat=2):
        fn = idir+task["torrent"]["name"][:task["torrent"]["name"].rfind(".")]
        if os.path.exists(fn):
            print(task["torrent"]["name"],"is being downloaded.")
            db.UpdateMovie(task["_id"],{"$set":{"downstat":3}})
    
    print("check downloading files done. check downloaded files")
    for task in db.FindMovie(downstat=3):
        fn = fdir+task["torrent"]["name"][:task["torrent"]["name"].rfind(".")]
        if os.path.exists(fn):
            print(task["torrent"]["name"],"is already downloaded.")
            folder_name = task["name"].replace(" ","_").replace(":","_").replace("?","_").replace("*","_").replace("?","_").replace(">","_").replace("<","_")+"_("+str(task["year"])+")"
            file_name = folder_name+fn[fn.rfind("."):]

            current_month = datetime.now().strftime('%m')
            current_year_full = datetime.now().strftime('%Y')
            folder_name = current_year_full+current_month+"/"+folder_name
            if not os.path.exists(ndir+folder_name): os.makedirs(ndir+folder_name)
            os.rename(fn, ndir+folder_name+"/"+file_name)
            os.chmod(ndir+folder_name, stat.S_IRWXO)
            print("file moved to target folder.")
            db.UpdateMovie(task["_id"],{"$set":{"foldername":folder_name, "downstat":4}})

    print("all task added. sleep for", sleep_time)
    time.sleep(sleep_time)