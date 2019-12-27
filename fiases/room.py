#! /usr/bin/env python
# -*- coding: utf-8 -*-
from tqdm import tqdm
from lxml import etree
from pathlib import Path
from fiases.fias_data import ES
from xml.dom import pulldom
from xml.dom.pulldom import parse
from elasticsearch.helpers import parallel_bulk
from elasticsearch.client import IndicesClient

# Local modules:
import fiases.fias_data
from fiases.fias_info import getUpdateVersion
from fiases.fias_download import downloadFull, unRarFull


def import_room(room):

    # fiases.fias_data.createTmpDir()

    # 1. версия
    getUpdateVersion()

    # 2. загрузка
    # downloadFull()

    # 3. распаковка
    unRarFull(room)

    # 4. маппинг
    if (ES.indices.exists(room.INDEX)):
        ES.indices.delete(index=room.INDEX)

    SHARDS_NUMBER = "1"
    SETTINGS = {
        "index": {
            "number_of_shards": SHARDS_NUMBER,
            "number_of_replicas": "0",
            "refresh_interval": "-1",
            "requests": {
                "cache": {
                    "enable": "true"
                }
            },
            "blocks": {
                "read_only_allow_delete": "false"
            }
        }
    }
    PROPERTIES = {
        "live_status": {
            "type": "keyword",
        },
        "room_guid": {
            "type": "keyword",
        },
        "flat_num": {
            "type": "keyword",
        },
        "room_num": {
            "type": "keyword",
        },
        "room_type": {
            "type": "keyword",
        },
        "flat_type": {
            "type": "keyword",
        },
        "postal_code": {
            "type": "keyword"
        },
        "counter": {
            "type": "keyword"
        },
        "end_date": {
            "type": "date"
        },
        "start_date": {
            "type": "date"
        },
        "bazis_finish_date": {
            "type": "date"
        },
        "bazis_create_date": {
            "type": "date"
        },
        "bazis_update_date": {
            "type": "date"
        },
        "update_date": {
            "type": "date"
        },
        "cad_num": {
            "type": "keyword"
        }
    }
    ES.indices.create(index=room.INDEX,
                      body={
                          'mappings': {
                              "dynamic": False,
                              "properties": PROPERTIES
                          },
                          "settings": SETTINGS
                      })

    # 6. препроцессор
    room.createPreprocessor()

    # 7. импорт
    doc = parse(fiases.fias_data.WORK_DIR + room.xml_file)

    def importFull():
        counter = 0
        for event, node in doc:
            if event == pulldom.START_ELEMENT \
               and node.tagName == room.TAG:
                yield {
                    "_index": room.INDEX,
                    "_type": "_doc",
                    "_op_type": fiases.fias_data.INDEX_OPER,
                    'pipeline': room.PIPELINE,
                    "_id": node.getAttribute("ROOMID"),
                    "room_guid": node.getAttribute("ROOMGUID"),
                    "house_guid": node.getAttribute("HOUSEGUID"),
                    "flat_num": node.getAttribute("FLATNUMBER"),
                    "room_num": node.getAttribute("ROOMNUMBER"),
                    "room_type": node.getAttribute("ROOMTYPE"),
                    "flat_type": node.getAttribute("FLATTYPE"),
                    "live_status":node.getAttribute("LIVESTATUS"),
                    "region_code": node.getAttribute("REGIONCODE"),
                    "postal_code": node.getAttribute("POSTALCODE"),
                    "start_date": node.getAttribute("STARTDATE"),
                    "end_date": node.getAttribute("ENDDATE"),
                    "update_date": node.getAttribute("UPDATEDATE"),
                    "norm_doc": node.getAttribute("NORMDOC"),
                    "cad_num": node.getAttribute("CADNUM"),
                    "bazis_create_date": fiases.fias_data.CREATE_DATE_ZERO,
                    "bazis_update_date": fiases.fias_data.UPDATE_DATE_ZERO,
                    "update_date": node.getAttribute("UPDATEDATE"),
                    "bazis_finish_date": node.getAttribute("ENDDATE")
                }

    for ok, info in tqdm(parallel_bulk(ES,
                                       importFull(),
                                       raise_on_error=False,
                                       raise_on_exception=False),
                         unit=' адрес',
                         desc=' загружено',
                         total=room.COUNT):
        if (not ok):
            print(ok, info)

    IndicesClient(ES).refresh()
    IndicesClient(ES).flush()
    IndicesClient(ES).forcemerge()




room = fiases.fias_data.Room()
import_room(room=room)
