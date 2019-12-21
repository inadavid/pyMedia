import time
import hashlib
import os
from modules.db_handler import db_handler
from win32com.client import Dispatch

db = db_handler("192.168.7.201")
thunder = Dispatch('ThunderAgent.Agent64.1')
sleep_time = 60
tmp_dir = "c:\\temp\\"
while True:
    for task in db.FindMovie(downstat=0):
        if task["platform"] == "btbtt.com":
            fn = tmp_dir+task["torrent"]["name"]
            print(fn)
            f=open(fn,"wb")
            f.write(task["torrent"]["content"])
            f.close()
            thunder.AddTask(fn)
            thunder.CommitTasks()
            #os.remove(fn)
            exit()
        elif task["platform"] == "dytt8.net":
            print("implementing...")
        
        time.sleep(2)

    print("all task added. sleep for", sleep_time)
    time.sleep(sleep_time)