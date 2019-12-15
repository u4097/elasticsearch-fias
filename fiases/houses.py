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
from fiases.fias_download import downloadFull, unRarFullHouses


def import_houses(houses):

    # 1. версия
    getUpdateVersion()

    # 2. загрузка
    downloadFull()

    # 3. распаковка
    unRarFullHouses(houses)

    # 4. маппинг
    if (ES.indices.exists(fiases.fias_data.HOUSE_INDEX)):
        ES.indices.delete(index=fiases.fias_data.HOUSE_INDEX)

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
        "ao_guid": {
            "type": "keyword"
        },
        "build_num": {
            "type": "keyword",
        },
        "house_num": {
            "type": "keyword",
        },
        "str_num": {
            "type": "keyword",
        },
        "ifns_fl": {
            "type": "keyword"
        },
        "ifns_ul": {
            "type": "keyword"
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
        },
        "terr_ifns_fl": {
            "type": "keyword"
        },
        "terr_ifns_ul": {
            "type": "keyword"
        },
        "okato": {
            "type": "keyword"
        },
        "oktmo": {
            "type": "keyword"
        }
    }
    ES.indices.create(index=fiases.fias_data.HOUSE_INDEX,
                      body={
                          'mappings': {
                              "dynamic": False,
                              "properties": PROPERTIES
                          },
                          "settings": SETTINGS
                      })

    # 6. препроцессор
    houses.createPreprocessor()

    # 7. импорт
    doc = parse(fiases.fias_data.WORK_DIR + houses.housesFullXmlFile)

    def importFull():
        counter = 0
        for event, node in doc:
            if event == pulldom.START_ELEMENT \
               and node.tagName == fiases.fias_data.HOUSES_OBJECT_TAG:
                yield {
                    "_index": fiases.fias_data.HOUSE_INDEX,
                    "_type": "_doc",
                    "_op_type": fiases.fias_data.INDEX_OPER,
                    'pipeline': fiases.fias_data.HOUSES_PIPELINE_ID,
                    "_id": node.getAttribute("HOUSEID"),
                    "ao_guid": node.getAttribute("AOGUID"),
                    "region_code": node.getAttribute("REGIONCODE"),
                    "start_date": node.getAttribute("STARTDATE"),
                    "end_date": node.getAttribute("ENDDATE"),
                    "update_date": node.getAttribute("UPDATEDATE"),
                    "div_type": node.getAttribute("DIVTYPE"),
                    "postal_code": node.getAttribute("POSTALCODE"),
                    "okato": node.getAttribute("OKATO"),
                    "oktmo": node.getAttribute("OKTMO"),
                    "ifns_fl": node.getAttribute("IFNSFL"),
                    "ifns_ul": node.getAttribute("IFNSUL"),
                    "terr_ifns_fl": node.getAttribute("TERRIFNSFL"),
                    "terr_ifns_ul": node.getAttribute("TERRIFNSUL"),
                    "norm_doc": node.getAttribute("NORMDOC"),
                    "house_num": node.getAttribute("HOUSENUM"),
                    "build_num": node.getAttribute("BUILDNUM"),
                    "str_num": node.getAttribute("STRUCNUM"),
                    "counter": node.getAttribute("COUNTER"),
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
                         total=fiases.fias_data.HOUSES_COUNT):
        if (not ok):
            print(ok, info)

    IndicesClient(ES).refresh()
    IndicesClient(ES).flush()
    IndicesClient(ES).forcemerge()




# houses = fiases.fias_data.Houses()
# import_houses(houses=houses)
