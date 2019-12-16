from elasticsearch_dsl.connections import connections
from elasticsearch import Elasticsearch

IS_DEBUG = False


def createConnection(host, timeout):
    connections.configure(
        default={'hosts': host, 'timeout': timeout},
    )
    es = Elasticsearch()
    return es
