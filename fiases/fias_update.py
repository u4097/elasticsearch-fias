import fiases.fias_data
from time import sleep
from fiases.address import import_address
from fiases.houses import import_houses
from fiases.fias_delta_update import update
from fiases.fias_data import ES
from fiases.snapshot import createFullSnapshot
from elasticsearch.client import IndicesClient
from elasticsearch.client import SnapshotClient
from elasticsearch.client import TasksClient

ADDR_UPDATE_CNT=0

def importFull():
    houses = fiases.fias_data.Houses()
    import_houses(houses=houses)
    refreshIndex()

    address = fiases.fias_data.Address()
    import_address(address=address)
    refreshIndex()

def getSnapshotStatus():
    sn =  SnapshotClient(ES) 
    status = sn.get(repository="fias",snapshot="fias_full")
    return status;

def getStatus():
    SNAP_STATUS="INIT"
    SNAP_STATUS = ES.indices.recovery(index="houses")
    SNAP_STATUS = SNAP_STATUS['houses']['shards'][0]['stage']
    return SNAP_STATUS


def getRestoreStatus():
    SNAP_STATUS="INIT"
    while SNAP_STATUS!="DONE":
        sleep(5)
        SNAP_STATUS = ES.indices.recovery(index="houses")
        SNAP_STATUS = SNAP_STATUS['houses']['shards'][0]['stage']
        if SNAP_STATUS !="DONE": 
            print(". ", end="")
        else:
            print("Finish!")
            break



def updateFias():
    if not ES.indices.exists(address.INDEX):
        print("No ADDR index found. Start import full...")
        importFull()
    else:
        ADDR_UPDATE_CNT=update(isDebug=True)
    createFullSnapshot()
    refreshIndex()
    fiases.fias_data.createTmpDir();
    return ADDR_UPDATE_CNT


def refreshIndex():
    IndicesClient(ES).refresh()
    IndicesClient(ES).flush()
    IndicesClient(ES).forcemerge()
# updateFias()
