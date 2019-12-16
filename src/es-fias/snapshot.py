from elasticsearch.client import SnapshotClient
from fiasData import ES
import fiasData


def createSnapshot(repository, indexName):
    sn = SnapshotClient(ES)
    snapshot = sn.get(repository=repository, snapshot=indexName)
    if snapshot:
        sn.delete(repository=repository, snapshot=indexName)
    else:
        pass

    sn_body = {
        "indices": indexName,
        "ignore_unavailable": "true",
        "include_global_state": "false",
        "metadata": {
            "taken_by": "fias",
            "taken_because": "backup before update"
        }
    }
    sn.create(repository=repository, snapshot=indexName, body=sn_body)


def createFullSnapshot(repository):
    sn = SnapshotClient(ES)
    try:
        sn.delete(repository=repository, snapshot="fias_full")
    except(Exception):
        pass

    sn_body = {
        "indices": [fiasData.ADDRESS_INDEX, fiasData.HOUSE_INDEX],
        "ignore_unavailable": "true",
        "include_global_state": "false",
        "metadata": {
            "taken_by": "fias",
            "taken_because": "backup before update"
        }
    }
    sn.create(repository=repository, snapshot="fias_full", body=sn_body)


# from initDb import createConnection
# es = createConnection(host='localhost', timeout=20)
# createSnapshot(repository='fias', indexName='address', elasticsearch=es)
# sn = SnapshotClient(es)
# print(sn.get(repository="fias", snapshot="address"))
