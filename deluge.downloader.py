import time
import hashlib
import difflib
import os, stat, sys
from modules.db_handler import db_handler
from datetime import datetime
from config import tdir,fdir,idir,ndir,dburl

db = db_handler(dburl)
sleep_time = 60


#downstat status:
#0, scraped
#1, torrent added to folder
#2, torrent task add to deluge
#3, incoming folder file existed, download started.
#4, download finished, file moved to downloaded folder


while True:
    #move all files in forlder level to first level and rename them.
    f = []
    for (dirpath, dirnames, filenames) in os.walk(fdir):
        if fdir == dirpath: continue
        if len(filenames)!=1: continue
        ext = filenames[0][filenames[0].rfind("."):]
        os.rename(dirpath+"/"+filenames[0], dirpath + ext)
        os.rmdir(dirpath)
    print("move files inside folder to parent level.")

    for task in db.FindMovie(downstat=0):
        if task["platform"] == "btbtt.com":
            fn = tdir+task["torrent"]["name"]
            print(fn)
            f=open(fn,"wb")
            f.write(task["torrent"]["content"])
            f.close()
        elif task["platform"] == "dytt8.net":
            print("implementing...")
        
        db.UpdateMovie(task["_id"],{"$set":{"downstat":1,"datelog.scrap": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}})

    print("add movie task done. check add status.")
    for task in db.FindMovie(downstat=1):
        fn = tdir+task["torrent"]["name"]
        if not os.path.exists(fn):
            print(task["torrent"]["name"],"already added.")
            db.UpdateMovie(task["_id"],{"$set":{"downstat":2,"datelog.taskadd": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}})
    
    print("check add status done. check downloading files")
    for task in db.FindMovie(downstat=2):
        fn = task["torrent"]["name"][:task["torrent"]["name"].rfind(".")]
        for r, d, f in os.walk(idir):
            for item in f:
                try:
                    ratio = difflib.SequenceMatcher(None, fn, item).quick_ratio()
                    if ratio > 0.9: 
                        print(task["torrent"]["name"],"is being downloaded.")
                        db.UpdateMovie(task["_id"],{"$set":{"downstat":3,"datelog.downloading":datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"torrent.real_fn":item}})
                except:
                    continue

    print("check downloading files done. check downloaded files")
    for task in db.FindMovie(downstat={ "$in": [2, 3] } ):
        fn = task["torrent"]["name"][:task["torrent"]["name"].rfind(".")]
        fn2 = task["torrent"]["name"][:task["torrent"]["name"].find(".")]
        for r, d, f in os.walk(fdir):
            for item in f:
                ratio = difflib.SequenceMatcher(None, fn, item).quick_ratio()
                ratio2 = difflib.SequenceMatcher(None, fn2, item[:item.rfind(".")]).quick_ratio()
                #print(fn,fn2,item,ratio,ratio2)
                if ratio > 0.9 or ratio2 > 0.9: 
                    print(task["torrent"]["name"],"is already downloaded.")
                    fn = item
                    folder_name = task["name"].replace(" ","_").replace(":","_").replace("?","_").replace("*","_").replace("?","_").replace(">","_").replace("<","_")+"_("+str(task["year"])+")"

                    if task["imdb"] != False: file_name = task["imdb"]+fn[fn.rfind("."):]
                    else: file_name = folder_name+fn[fn.rfind("."):]

                    folder_name = datetime.now().strftime("%Y%m")+"/"+folder_name
                    if not os.path.exists(ndir+folder_name): os.makedirs(ndir+folder_name)
                    try:
                        os.rename(fdir+fn, ndir+folder_name+"/"+file_name)
                    except:
                        continue
                    os.chmod(ndir+folder_name, stat.S_IRWXO)
                    print("file moved to target folder.")
                    db.UpdateMovie(task["_id"],{"$set":{"downstat":4,"datelog.downloaded":datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"torrent.real_fn":item}})
                    break

    print("all task added. sleep for", sleep_time)
    time.sleep(sleep_time)