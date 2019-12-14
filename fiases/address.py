#! /usr/bin/env python
# -*- coding: utf-8 -*-
from tqdm import tqdm
from lxml import etree
from xml.dom import pulldom
from xml.dom.pulldom import parse
from elasticsearch.helpers import parallel_bulk

# Local modules:
from fiases.fias_download import downloadUpdate, uprarUpdateAdddr, clearWorkDir
import fiases.fias_data
from fiases.init_db import createConnection, IS_DEBUG
from fiases.snapshot import createSnapshot
from fiases.fias_info import getUpdateVersion
from fiases.fias_download import downloadFull, unRarFullAdddr


def updateAddress(isDebug,address):
    # address = fias_data.Address()
    IS_DEBUG = isDebug
    es = createConnection(host=fiases.fias_data.HOST, timeout=fiases.fias_data.TIME_OUT)

    # 1. версия
    getUpdateVersion()
    # if IS_DEBUG:
        # print('Версия: ', fias_data.VERSION_DATE)

    # 2. загрузка
    # downloadFull()

    # 3. распаковка
    unRarFullAdddr(address)

    # 4. маппинг
    if (es.indices.exists(fiases.fias_data.ADDRESS_INDEX)):
        es.indices.delete(index=fiases.fias_data.ADDRESS_INDEX)

    SHARDS_NUMBER = "1"
    ANALYSIS = {
        "filter": {
            "autocomplete_filter": {
                "type": "edge_ngram",
                "min_gram": 2,
                "max_gram": 20
            },
            "fias_word_delimiter": {
                "type": "word_delimiter",
                "preserve_original": "true",
                "generate_word_parts": "false"
            },
            "fias_stop_filter": {
                "type": "stop",
                "remove_trailing": "true",
                "stopwords_path": "stopwords.txt"
            }
        },
        "analyzer": {
            "autocomplete": {
                "type": "custom",
                "tokenizer": "standard",
                "filter": ["autocomplete_filter"]
            },
            "stop_analyzer": {
                "type": "custom",
                "tokenizer": "whitespace",
                "filter": ["lowercase", "fias_stop_filter", "fias_word_delimiter"]
            }
        }
    }
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
            },
            "analysis": ANALYSIS
        }
    }

    PROPERTIES = {
        "street_address_suggest": {
            "type": "text",
            "analyzer": "autocomplete",
            "search_analyzer": "stop_analyzer"
        },
        "full_address": {
            "type": "keyword"
        },
        "district_full": {
            "type": "keyword"
        },
        "settlement_full": {
            "type": "keyword"
        },
        "street_full": {
            "type": "keyword"
        },
        "formal_name": {
            "type": "keyword"
        },
        "short_name": {
            "type": "keyword",
        },
        "off_name": {
            "type": "keyword",
        },
        "curr_status": {
            "type": "integer"
        },
        "oper_status": {
            "type": "integer"
        },
        "act_status": {
            "type": "integer",
        },
        "live_status": {
            "type": "integer"
        },
        "cent_status": {
            "type": "integer"
        },
        "ao_guid": {
            "type": "keyword",
        },
        "parent_guid": {
            "type": "keyword",
        },
        "ao_level": {
            "type": "keyword"
        },
        "area_code": {
            "type": "keyword"
        },
        "auto_code": {
            "type": "keyword"
        },
        "city_ar_code": {
            "type": "keyword"
        },
        "city_code": {
            "type": "keyword"
        },
        "street_code": {
            "type": "keyword"
        },
        "extr_code": {
            "type": "keyword"
        },
        "sub_ext_code": {
            "type": "keyword"
        },
        "place_code": {
            "type": "keyword"
        },
        "plan_code": {
            "type": "keyword"
        },
        "plain_code": {
            "type": "keyword",
        },
        "code": {
            "type": "keyword",
        },
        "postal_code": {
            "type": "keyword",
        },
        "region_code": {
            "type": "keyword"
        },
        "street": {
            "type": "keyword",
        },
        "district": {
            "type": "keyword",
        },
        "district_type": {
            "type": "keyword",
        },
        "street_type": {
            "type": "keyword",
        },
        "settlement": {
            "type": "keyword",
        },
        "settlement_type": {
            "type": "keyword",
        },
        "okato": {
            "type": "keyword",
        },
        "oktmo": {
            "type": "keyword",
        },
        "ifns_fl": {
            "type": "keyword",
        },
        "ifns_ul": {
            "type": "keyword",
        },
        "terr_ifns_fl": {
            "type": "keyword",
        },
        "terr_ifns_ul": {
            "type": "keyword",
        },
        "norm_doc": {
            "type": "keyword",
        },
        "start_date": {
            "type": "date"
        },
        "end_date": {
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
        "houses": {
            "type": "nested",
            "properties": {
                "houseId": {
                    "type": "keyword",
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
        }
    }
    es.indices.create(index=fiases.fias_data.ADDRESS_INDEX,
                      body={
                          'mappings': {
                              "dynamic": False,
                              "properties": PROPERTIES
                          },
                          "settings": SETTINGS
                      })

    # 6. препроцессор
    address.createPreprocessor(es)

    # 7. импорт
    doc = parse(fiases.fias_data.WORK_DIR + address.addressFullXmlFile)

    def importAddress():
        counter = 0
        for event, node in doc:
            if event == pulldom.START_ELEMENT \
               and node.tagName == fiases.fias_data.ADDR_OBJECT_TAG:
                yield {
                    "_index": fiases.fias_data.ADDRESS_INDEX,
                    "_type": "_doc",
                    "_op_type": fiases.fias_data.INDEX_OPER,
                    'pipeline': fiases.fias_data.ADDR_PIPELINE_ID,
                    "_id": node.getAttribute("AOID"),
                    "ao_guid": node.getAttribute("AOGUID"),
                    "parent_guid": node.getAttribute("PARENTGUID"),
                    "formal_name": node.getAttribute("FORMALNAME"),
                    "off_name": node.getAttribute("OFFNAME"),
                    "short_name": node.getAttribute("SHORTNAME"),
                    "ao_level": node.getAttribute("AOLEVEL"),
                    "area_code": node.getAttribute("AREACODE"),
                    "city_code": node.getAttribute("CITYCODE"),
                    "place_code": node.getAttribute("PLACECODE"),
                    "auto_code": node.getAttribute("AUTOCODE"),
                    "plan_code": node.getAttribute("PLANCODE"),
                    "street_code": node.getAttribute("STREETCODE"),
                    "city_ar_code": node.getAttribute("CTARCODE"),
                    "extr_code": node.getAttribute("EXTRCODE"),
                    "sub_ext_code": node.getAttribute("SEXTCODE"),
                    "code": node.getAttribute("CODE"),
                    "region_code": node.getAttribute("REGIONCODE"),
                    "plain_code": node.getAttribute("PLAINCODE"),
                    "postal_code": node.getAttribute("POSTALCODE"),
                    "okato": node.getAttribute("OKATO"),
                    "oktmo": node.getAttribute("OKTMO"),
                    "ifns_fl": node.getAttribute("IFNSFL"),
                    "ifns_ul": node.getAttribute("IFNSUL"),
                    "terr_ifns_fl": node.getAttribute("TERRIFNSFL"),
                    "terr_ifns_ul": node.getAttribute("TERRIFNSUL"),
                    "norm_doc": node.getAttribute("NORMDOC"),
                    "act_status": node.getAttribute("ACTSTATUS"),
                    "live_status": node.getAttribute("LIVESTATUS"),
                    "curr_status": node.getAttribute("CURRSTATUS"),
                    "oper_status": node.getAttribute("OPERSTATUS"),
                    "start_date": node.getAttribute("STARTDATE"),
                    "end_date": node.getAttribute("ENDDATE"),
                    "bazis_create_date": fiases.fias_data.CREATE_DATE_ZERO,
                    "bazis_update_date": fiases.fias_data.UPDATE_DATE_ZERO,
                    "update_date": node.getAttribute("UPDATEDATE"),
                    "bazis_finish_date": node.getAttribute("ENDDATE")

                }

    # if IS_DEBUG:
        # print("Загрузка...")
        # print()
        # print(address.addressFullXmlFile)
        # print()
    ADDR_CNT = 0
    for ok, info in tqdm(parallel_bulk(es,
                                       importAddress(),
                                       raise_on_error=False,
                                       raise_on_exception=False),
                         unit=' адрес',
                         desc=' загружено',
                         total=fias_data.ADDRESS_COUNT):
        if (not ok):
            print(ok, info)
        ADDR_CNT = ADDR_CNT + 1
    # if IS_DEBUG:
        # print()
        # print('Загружено: ', ADDR_CNT, ' записей.')


# 6. снэпшот
# createSnapshot(repository=fias_data.REPOSITORY,
               # indexName=fias_data.ADDRESS_INDEX, elasticsearch=es)

# 7. очистка
# clearWorkDir()


# updateAddress(isDebug=True)
