from flask import Flask
from flask import request
from flask import abort
import urllib
import networkx as nx
from networkx.readwrite import json_graph
import json
import MySQLdb

app = Flask(__name__)


"""
EXAMPLE CURL CALL:
curl -i http://localhost:5000/newjob/local-1461777587369/Spark+Pi/0/%24sid%3D0%23id%3D1+name%3DMapPartitionsRDD+pid%3D0%2C%23%23%23id%3D0+name%3DParallelCollectionRDD+pid%3D

"""


@app.route('/')
def index():
    return "Still Running"

@app.route( '/newjob/<string:appid>/<string:appname>/<int:jobid>/<string:dag>',  methods=['POST',  'GET'] )
def newJob( appid, appname, jobid, dag ):
    dag =  urllib.unquote(dag).decode('utf8')
    stages = dag.split("$")
    G = nx.DiGraph()
    for i in range(len(stages)):
        stageParse( stages[i],  G )
    printDags( G )
    submitToWarehouse( appid,  appname,  jobid,  G )
    return "TODO: config suggestion for job " + str(jobid)

def printDags( G ):
    print "JSON: ",  json.dumps( json_graph.node_link_data(G) )
    return

def submitToWarehouse( appid,  appname,  jobid,  G ):
    db = MySQLdb.connect(host="mother",    # your host, usually localhost
                     user="",         # your username
                     passwd="",  # your password
                     db="warehouse")        # name of the data base
    cur = db.cursor()
    query = "INSERT INTO application (app_id, app_name, app_dag) VALUES ('" + appid + "','" + appname + "','" + json.dumps( json_graph.node_link_data(G) ) + "');"
    cur.execute( query )
    db.commit()
    db.close()

def stageParse( stage,  G ):
    if not "sid=" in stage:
        return
    #stageDict['stageId'] =  int( stage.split("sid=", 1)[1].split("#",1)[0] )
    rdds = stage.split("#",1)[1].split("###")
    rddParse( rdds,  G )

def rddParse( rdds,  G ):
    rddFullList = []

    #create nodes
    for rdd in rdds:
        rddDict = {}
        rddList = rdd.split( "+" )
        if( len(rddList) < 2 ):
            return {}
        id = int( rddList[0].split("=")[1] )
        rddName = rddList[1].split("=")[1]
        parentIds = map( int,  filter(None, rddList[2].split("=")[1].split(",")) )
        G.add_node( id,  name=rddName )
        rddDict['id'] = id
        rddDict['parentIds'] = parentIds
        #TODO: add more info about rdd here if necessary
        rddFullList.append( rddDict )

    #connect nodes
    for rdd in rddFullList:
        id = rdd['id']
        parentIds = rdd['parentIds']
        for parentId in parentIds:
            G.add_edge( parentId, id )
    return  rddFullList

@app.route( '/jobcompletion/<int:jobid>/<int:runtime>',  methods=['POST', 'GET'] )
def updateWarehouse( jobid,  runtime ):
    print "updating warehouse with performance information"
    print "job ", jobid, " took ", runtime/1e9, " seconds."
    return "added to warehouse"
    #TODO: actually update warehouse

if __name__ == '__main__':
    app.run(host='0.0.0.0',  debug=True)
