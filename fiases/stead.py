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


def import_stead(stead):

    # fiases.fias_data.createTmpDir()

    # 1. версия
    getUpdateVersion()

    # 2. загрузка
    # downloadFull()

    # 3. распаковка
    unRarFull(stead)

    # 4. маппинг
    if (ES.indices.exists(stead.INDEX)):
        ES.indices.delete(index=stead.INDEX)

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
        "parent_guid": {
            "type": "keyword"
        },
        "build_num": {
            "type": "keyword",
        },
        "live_status": {
            "type": "keyword",
        },
        "oper_status": {
            "type": "keyword",
        },
        "stead_guid": {
            "type": "keyword",
        },
        "stead_num": {
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
    ES.indices.create(index=stead.INDEX,
                      body={
                          'mappings': {
                              "dynamic": False,
                              "properties": PROPERTIES
                          },
                          "settings": SETTINGS
                      })

    # 6. препроцессор
    stead.createPreprocessor()

    # 7. импорт
    doc = parse(fiases.fias_data.WORK_DIR + stead.xml_file)

    def importFull():
        counter = 0
        for event, node in doc:
            if event == pulldom.START_ELEMENT \
               and node.tagName == stead.TAG:
                yield {
                    "_index": stead.INDEX,
                    "_type": "_doc",
                    "_op_type": fiases.fias_data.INDEX_OPER,
                    'pipeline': stead.PIPELINE,
                    "_id": node.getAttribute("STEADID"),
                    "parent_guid": node.getAttribute("PARENTGUID"),
                    "stead_guid": node.getAttribute("STEADGUID"),
                    "oper_status":node.getAttribute("OPERSTATUS"),
                    "live_status":node.getAttribute("LIVESTATUS"),
                    "region_code": node.getAttribute("REGIONCODE"),
                    "postal_code": node.getAttribute("POSTALCODE"),
                    "start_date": node.getAttribute("STARTDATE"),
                    "end_date": node.getAttribute("ENDDATE"),
                    "update_date": node.getAttribute("UPDATEDATE"),
                    "div_type": node.getAttribute("DIVTYPE"),
                    "okato": node.getAttribute("OKATO"),
                    "oktmo": node.getAttribute("OKTMO"),
                    "ifns_fl": node.getAttribute("IFNSFL"),
                    "ifns_ul": node.getAttribute("IFNSUL"),
                    "terr_ifns_fl": node.getAttribute("TERRIFNSFL"),
                    "terr_ifns_ul": node.getAttribute("TERRIFNSUL"),
                    "norm_doc": node.getAttribute("NORMDOC"),
                    "stead_num": node.getAttribute("NUMBER"),
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
                         total=stead.COUNT):
        if (not ok):
            print(ok, info)

    IndicesClient(ES).refresh()
    IndicesClient(ES).flush()
    IndicesClient(ES).forcemerge()




stead = fiases.fias_data.Stead()
import_stead(stead=stead)
