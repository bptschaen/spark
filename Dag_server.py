from flask import Flask
from flask import request
from flask import abort
import urllib

app = Flask(__name__)


"""
example url: /newjob/0/%24RDD+%22MapPartitionsRDD%22+%281%29+StorageLevel%3A+StorageLevel%28false%2C+false%2C+false%2C+false%2C+1%29%3B+CachedPartitions%3A+0%3B+TotalPartitions%3A+10%3B+MemorySize%3A+0.0+B%3B+ExternalBlockStoreSize%3A+0.0+B%3B+DiskSize%3A+0.0+B%23%23%23RDD+%22ParallelCollectionRDD%22+%280%29+StorageLevel%3A+StorageLevel%28false%2C+false%2C+false%2C+false%2C+1%29%3B+CachedPartitions%3A+0%3B+TotalPartitions%3A+10%3B+MemorySize%3A+0.0+B%3B+ExternalBlockStoreSize%3A+0.0+B%3B+DiskSize%3A+0.0+B

url2: $id=1 name=MapPartitionsRDD pid=0,###id=0 name=ParallelCollectionRDD pid=

curl -i http://localhost:5000/newjob/0/%24sid%3D0%23id%3D1+name%3DMapPartitionsRDD+pid%3D0%2C%23%23%23id%3D0+name%3DParallelCollectionRDD+pid%3D

"""


@app.route('/')
def index():
    return "Still Running"

@app.route( '/newjob/<int:jobid>/<string:dag>',  methods=['POST',  'GET'] )
def newJob( jobid,  dag ):
    dag =  urllib.unquote(dag).decode('utf8')
    stages = dag.split("$")
    for i in range(len(stages)):
        stageParse( stages[i] )
        
    printDags( stages )
    return "TODO: config suggestion for job " + str(jobid)

def printDags( stages ):
    for i,  stage in enumerate(stages):
        print "Stage " , i
        #for rdd in stage:
            #print "RDD: " + str(rdd)

def stageParse( stage ):
    stageDict = {}
    #stageDict['stageId'] = 
    if not "sid=" in stage:
        return stageDict
    print "stages[i]: ",  stage
    stageDict['stageId'] =  int( stage.split("sid=", 1)[1].split("#",1)[0] )
    rdds = stage.split("#",1)[1].split("###")
    print "split stage: ",  stage
    stageDict['RDDs'] = rddParse( rdds )
    print "stageDict: ",  stageDict

def rddParse( rdds ):
    rddFullList = []
    for rdd in rdds:
        rddDict = {}
        rddList = rdd.split( "+" )
        if( len(rddList) < 2 ):
            return {}
        rddDict['id'] = rddList[0].split("=")[1]
        rddDict['name'] = rddList[1].split("=")[1]
        rddDict['parentIds'] = rddList[2].split("=")[1]
        #TODO: add more info about rdd here if necessary
        rddFullList.append( rddDict )
    print "RDDs: ",  rddFullList
    return rddDict

@app.route( '/jobcompletion/<int:jobid>/<int:runtime>',  methods=['POST', 'GET'] )
def updateWarehouse( jobid,  runtime ):
    print "updating warehouse with performance information"
    print "job ", jobid, " took ", runtime/1e9, " seconds."
    return "added to warehouse"
    #TODO: actually update warehouse

if __name__ == '__main__':
    app.run(debug=True)
