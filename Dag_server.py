from flask import Flask
from flask import request
from flask import abort
import urllib
import json

app = Flask(__name__)


"""
EXAMPLE CURL CALL:
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
    return

def stageParse( stage ):
    stageDict = {}
    #stageDict['stageId'] = 
    if not "sid=" in stage:
        return stageDict
    stageDict['stageId'] =  int( stage.split("sid=", 1)[1].split("#",1)[0] )
    rdds = stage.split("#",1)[1].split("###")
    stageDict['RDDs'] = rddParse( rdds )
    print json.dumps( stageDict )

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
    return  rddFullList

@app.route( '/jobcompletion/<int:jobid>/<int:runtime>',  methods=['POST', 'GET'] )
def updateWarehouse( jobid,  runtime ):
    print "updating warehouse with performance information"
    print "job ", jobid, " took ", runtime/1e9, " seconds."
    return "added to warehouse"
    #TODO: actually update warehouse

if __name__ == '__main__':
    app.run(host='0.0.0.0',  debug=True)
