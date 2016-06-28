#!/bin/python
import pymongo
from pymongo import MongoClient
from bson.objectid import ObjectId
from optparse import OptionParser
from pymongo import ReadPreference
try:
    from collections import OrderedDict
except ImportError:
    # python 2.6 or earlier, use backport
    from ordereddict import OrderedDict
import sys
import time
import datetime
def get_cli_options():
    dt=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())) 
    
    parser = OptionParser(usage="usage: python %prog [options]",
                          description=""" MONGODB DATA  MIGRATE""")
   
    parser.add_option("--H1", "--from_host",
                      dest="s_host",
                      default="localhost",
                      metavar="host:port"
                     )
    
    
    parser.add_option("--H2", "--to_host",
                      dest="d_host",
                      default="localhost",
                      metavar="host:port"
                      )
    
    parser.add_option("--C1", "--from_coll",
                      dest="s_coll",
                      default="test",
                      metavar="source_collection"
                      )
    
    parser.add_option("--C2", "--to_coll",
                      dest="d_coll",
                      default="test",
                      metavar="dest_collection"
                      )
    
    parser.add_option("--D1", "--from_db",
                      dest="s_db",
                      default="test",
                      metavar="source_database"
                     )
 
    parser.add_option("--D2", "--to_db",
                      dest="d_db",
                      default="test",
                      metavar="dest_database"
                      )

    parser.add_option("--T1", "--s_date",
                      dest="s_date",
                      default="2015-04-30 00:00:00",
                      metavar="start_date"
                      )

    parser.add_option("--T2", "--e_date",
                      dest="e_date",
                      default=dt,
                      metavar="end_date"
                     )
    
    parser.add_option("--I2", "--col",
                      dest="col_nm",
                      default="mtime",
                      metavar="mtime"
                     )
    parser.add_option("--I3", "--is_inr",
                      dest="is_inr",
                      default="N",
                      metavar="is_inr"
                      )
    (options, args) = parser.parse_args()
    
 
    return options

def objectid (day):
    start_date = str(day)
    date_1 = datetime.datetime.strptime(start_date,"%Y-%m-%d %H:%M:%S")
    end_date = date_1 + datetime.timedelta(hours=1)

    s=datetime.datetime(date_1.year,date_1.month,date_1.day,date_1.hour)
    d=datetime.datetime(end_date.year,end_date.month,end_date.day,end_date.hour)

    objmin =ObjectId.from_datetime(s)
    objmax =ObjectId.from_datetime(d)
    #print str(date_1),str(end_date),objmin,objmax
    return objmin,objmax,end_date

def from_client():

    options = get_cli_options()
    client  = MongoClient(options.s_host,document_class=OrderedDict)
    client['admin'].authenticate('admin','admin')
    db=client[options.s_db]
    collection=db[options.s_coll]
    return collection

def to_client():
    options = get_cli_options()
    client  = MongoClient(options.d_host)
    client['admin'].authenticate('admin','admin')
    db=client[options.d_db]
    collection=db[options.d_coll]
    return collection

def timestamp_from_objectid(objectid):
  result = 0
  try:
    timeStamp = time.mktime(objectid.generation_time.timetuple())
    timeArray = time.localtime(timeStamp)
    createtime=time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
  except:
    pass
  print  createtime


def mrigate_date():
    options = get_cli_options()
    stday=options.s_date
    enday=options.e_date
    return str(stday),str(enday)


def inr_mrigate_data():
    count=0
    coll_s=from_client()
    coll_d=to_client()
    options = get_cli_options()
    var_date = datetime.datetime.strptime(options.e_date,"%Y-%m-%d %H:%M:%S")
    end_date = (var_date + datetime.timedelta(hours=-8))
    v_end_dt=datetime.datetime(end_date.year,end_date.month,end_date.day,end_date.hour,end_date.minute,end_date.second)
    iter = coll_s.find({options.col_nm:{"$gte":v_end_dt}})
    try:
       for doc in iter:
           dt=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
           count=count+1
           userObjectId=coll_d.replace_one({"_id":doc['_id']},doc,upsert=True)
           if count%20000==0:
                  print "%d increment commited" %(count),dt
           else:
                  pass
    except  Exception as e:
       print e
       print "exit..."
       print
       sys.exit()
    print "sum  %d  increment commited" %(count)
def mrigate_data():
   batch=10000
   sum=0
   count=0
   coll_s=from_client()
   coll_d=to_client()  
   stday,endate=mrigate_date()
   while str(stday)<=str(endate):
        operations = []
   	objmin,objmax,endat=objectid(stday)
        stday=endat
        iter = coll_s.find({"_id":{"$gte":objmin, "$lt":objmax}})
        try:
           doc=OrderedDict()
           for doc in iter:
               operations.append(doc)
               if (len(operations)==batch):
            	    coll_d.insert_many(operations,ordered=False)
           	    operations = []
                    count=count+1
                    if count%10==0:
                       dt=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
                       print "%d commited" %(count*batch),dt
                    else:
                        pass
           if (len(operations)> 0):
              coll_d.insert_many(operations,ordered=False)
              sum=sum+len(operations)
           else:
              pass
        
        except KeyboardInterrupt:
              print "exit .."
              print 
              print "%d  commit" %(count),dt
              print 
              sys.exit()
        if str(stday)>str(endate):
             dt=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
             print "%s  %d  commited " %(endat,count*batch+len(operations)+sum)  
        else:
             pass 

def main():
    options = get_cli_options()
    is_inr=options.is_inr
    if is_inr=='N' or is_inr=='n':
       mrigate_data()
    elif is_inr=="y":
       inr_mrigate_data()
    elif is_inr=="Y":
       mrigate_data()
       inr_mrigate_data()
    else:
        pass

if __name__ == '__main__':
    main()

