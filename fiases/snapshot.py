from elasticsearch.client import SnapshotClient
from fiases.fias_data import ES
import fiases.fias_data



def createFullSnapshot():
    sn = SnapshotClient(ES)
    try:
        sn.delete(repository="fias", snapshot="fias_full")
    except(Exception):
        pass

    sn_body = {
        "indices": [fiases.fias_data.ADDRESS_INDEX, fiases.fias_data.HOUSE_INDEX],
        "ignore_unavailable": "true",
        "include_global_state": "false",
        "metadata": {
            "taken_by": "fias",
            "taken_because": "backup before update"
        }
    }
    sn.create(repository="fias", snapshot="fias_full", body=sn_body)


