import pymongo

class db_handler:
    def __init__(self, host, port=27017, dbn = "pyMedia"):
        client = pymongo.MongoClient("mongodb://"+host+":"+str(port)+"/")
        self.db = client[dbn]
        
    def InsertMovie(self, movie):
        if(self.InDB(movie)<0): return self.InDB(movie)
        movie["downstat"] = 0
        self.db.movie.insert_one(movie)
        return 1
    
    def InDB(self,movie):
        if self.db.movie.count_documents({"platform":movie["platform"],"pid":movie["pid"]}) > 0:
            return -1 # already existed by platform id
        if movie["imdb"] != False and self.db.movie.count_documents({"imdb":movie["imdb"]}) > 0:
            return -2 # already existed by imdb id
        return 1
    
    def FindMovie(self, downstat=-1, imdb=False, name=False):
        if downstat != -1 :
            return self.db.movie.find({
                "downstat": downstat
            })
        
        if imdb != False :
            return self.db.movie.find({
                "imdb": imdb
            })
        
        if name != False :
            return self.db.movie.find({"$or": [{"name" : {"$regex" : ".*"+name+".*"}},{"name_chn" : {"$regex" : ".*"+name+".*"}}]})