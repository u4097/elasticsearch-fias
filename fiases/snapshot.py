from elasticsearch.client import SnapshotClient
from fiases.fias_data import ES
import fiases.fias_data

sn = SnapshotClient(ES)

def register(location="/usr/share/elasticsearch/snapshots"):
    sn_body = {
      "type": "fs",
      "settings": {
        "compress": "true",
            "location": location
        } 
    }
    sn.create_repository(repository="fias", body=sn_body)

def restore():
    ES.indices.delete(index=address.INDEX, ignore=[400, 404])
    ES.indices.delete(index=houses.INDEX, ignore=[400, 404])

    sn.restore(repository="fias",
           snapshot="fias_full",
           body={
               "indices": [address.INDEX, houses.INDEX]
           })

def restoreIfNotExist():
    if not ES.indices.exists(address.INDEX):
        sn.restore(repository="fias",
               snapshot="fias_full",
               body={
                   "indices": [address.INDEX, houses.INDEX]
               })
    else:
        pass


def createFullSnapshot():
    try:
        sn.delete(repository="fias", snapshot="fias_full")
    except(Exception):
        pass

    sn_body = {
        "indices": [address.INDEX, houses.INDEX],
        "ignore_unavailable": "true",
        "include_global_state": "false",
        "metadata": {
            "taken_by": "fias",
            "taken_because": "backup before update"
        }
    }
    sn.create(repository="fias", snapshot="fias_full", body=sn_body)


