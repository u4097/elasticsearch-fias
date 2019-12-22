import fiases.fias_data
from fiases.address import import_address
from fiases.houses import import_houses
from fiases.fias_delta_update import update
from fiases.fias_data import ES
from fiases.snapshot import createFullSnapshot
from elasticsearch.client import IndicesClient

ADDR_UPDATE_CNT=0

def importFull():
    houses = fiases.fias_data.Houses()
    import_houses(houses=houses)
    refreshIndex()

    address = fiases.fias_data.Address()
    import_address(address=address)
    refreshIndex()


def updateFias():
    if not ES.indices.exists(fiases.fias_data.ADDRESS_INDEX):
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
