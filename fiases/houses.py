#! /usr/bin/env python
# -*- coding: utf-8 -*-
import re
import os
import rarfile
import dateutil.parser as parser
import xml.etree.ElementTree as ET
from datetime import datetime
from tqdm import trange, tqdm
from hurry.filesize import size, si
from urllib import request
from lxml import etree
from xml.dom import pulldom
from xml.dom.pulldom import parse
from elasticsearch.client import IngestClient, IndicesClient, SnapshotClient
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan, parallel_bulk, streaming_bulk
from elasticsearch_dsl import Q, Search, Index, Document, Date, Nested, InnerDoc, Keyword, Text, Integer, Short, Long, Range
from elasticsearch_dsl.connections import connections


ADDRESS_INDEX = 'address_new'
HOUSE_INDEX = 'houses_new'
AS_ADDR_FILE = 'AS_ADDROBJ_*'
UPDATE_DATE_ZERO = '2000-01-01T00:00:00Z'
CREATE_DATE_ZERO = '2019-12-07T00:00:38Z'
AS_DEL_ADDR_FILE = 'AS_DEL_ADDROBJ_*'
OBJECT_TAG = 'Object'
PIPELINE_ID = 'addr_drop_pipeline'
INDEX_OPER = 'index'
DELETE_OPER = 'delete'
FIAS_XML_RAR = 'fias_xml.rar'
FIAS_DELTA_XML_RAR = 'fias_delta_xml.rar'
WORK_DIR = '/Volumes/DATA/downloads/'
FIAS_URL = 'https://fias.nalog.ru/Public/Downloads/Actual/'
VERSION_TXT = 'VerDate.txt'
DATE_TIME_ZONE = 'T00:00:00Z'

ADDR_INFO_DIC = {
    'versionDate': '',
    'dateTimeZone': DATE_TIME_ZONE,
    'housesIndex': 'houses',
    'housesObjectTag': 'House',
    'housesPipeLineId': 'house_drop_pipeline',
    'as_house_file': 'AS_HOUSE_*',
    'as_del_house_file': 'AS_DEL_HOUSE_*',
    'url': FIAS_URL + FIAS_XML_RAR,
    'workDir': WORK_DIR,
    'housesFullXmlFile': '',
    'housesFullXmlSize': 0,
    'housesFullXmlRecCount': 4000000,
    'housesFullXmlNewRecCount': 0,
    'housesDELFullXMLFile': '',
    'housesDELFullXmlSize': 0,
    'housesDELFullXmlRecCount': 0,
}


def getUpdateVersion():
    global VERSION_DATE
    global VERSION_DATE_REPORT
    url = FIAS_URL + 'VerDate.txt'

    request.urlretrieve(url, WORK_DIR + 'VerDate.txt')
    with open(WORK_DIR + 'VerDate.txt', 'r') as f:
        date = f.read()
        VERSION_DATE_REPORT = date
        VERSION_DATE = parser.parse(date)

        VERSION_DATE = VERSION_DATE.strptime(
            str(VERSION_DATE)[:10], '%Y-%m-%d').date()
        ADDR_INFO_DIC['versionDate'] = VERSION_DATE
        print(VERSION_DATE, VERSION_DATE_REPORT)


# 1. версия
getUpdateVersion()


class TqdmUpTo(tqdm):

    def update_to(self, b=6500000, bsize=1, tsize=6500000):
        if tsize is not None:
            self.total = tsize
            self.update(b * bsize - self.n)  # will also set self.n = b * bsize


def downloadUpdate(url, file):
    """ Загрузка полной базы данных ФИАС в формате XML, для начальной инициации базы """
    print('Начинаем загрузку ...')
    url = url + file
    file_name = WORK_DIR + file

    with TqdmUpTo(unit='B',
                  unit_scale=True,
                  miniters=1,
                  desc=url.split('/')[-1]) as t:  # all optional kwargs
        request.urlretrieve(url,
                            filename=file_name,
                            reporthook=t.update_to,
                            data=None)
    print('Загрузка завершена.')


# 2. загрузка
# downloadUpdate(FIAS_URL, FIAS_XML_RAR)

def uprarFullAdddr():
    """ 3.Распаковываем архив с базой и удаленными записями """
    rf = rarfile.RarFile(WORK_DIR + FIAS_XML_RAR)

    houseMatcher = re.compile(ADDR_INFO_DIC['as_house_file'])
    houseDelMatcher = re.compile(ADDR_INFO_DIC['as_del_house_file'])
    print('unrar houses...')
    for f in rf.infolist():
        if houseMatcher.match(f.filename):
            print('FOUND: ' + f.filename)
            ADDR_INFO_DIC['housesFullXmlFile'] = f.filename
            ADDR_INFO_DIC['housesFullXmlSize'] = f.file_size
            print(
                'size: ' + str(size(ADDR_INFO_DIC['housesFullXmlSize'], system=si)))
        if houseDelMatcher.match(f.filename):
            print('FOUND: ' + f.filename)
            ADDR_INFO_DIC['housesDELFullXMLFile'] = f.filename
            ADDR_INFO_DIC['housesDELFullXmlSize'] = f.file_size
            print(
                'size: ' + str(size(ADDR_INFO_DIC['housesDELFullXmlSize'], system=si)))

    if (ADDR_INFO_DIC['housesFullXmlSize'] > 0):
        print('1.extracting: ' + ADDR_INFO_DIC['housesFullXmlFile'])

        rf.extract(ADDR_INFO_DIC['housesFullXmlFile'], WORK_DIR)
        print('2.extracting: ' + ADDR_INFO_DIC['housesDELFullXMLFile'])

        rf.extract(ADDR_INFO_DIC['housesDELFullXMLFile'], WORK_DIR)
        print('finished')
    else:
        print('files NOT FOUND!')


# 3. распаковка
# uprarFullAdddr()


# 4. маппинг
es = Elasticsearch()
if (es.indices.exists(ADDR_INFO_DIC['housesIndex'])):
    es.indices.delete(index=ADDR_INFO_DIC['housesIndex'])

SHARDS_NUMBER = "4"
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
es.indices.create(index=HOUSE_INDEX,
                  body={
                      'mappings': {
                          "dynamic": False,
                          "properties": PROPERTIES
                      },
                      "settings": SETTINGS
                  })

es = Elasticsearch('localhost')
sn = SnapshotClient(es)

# 5. регистрация репозитария для создания снэпшотов
# "location": "/usr/share/elasticsearch/snapshots"
body = {
    "type": "fs",
    "settings": {
        "compress": "true",
        "location": "/Volumes/DATA/elasticsearch/snapshots"
    }
}

sn.create_repository(repository="fias", body=body)

houses = Index(HOUSE_INDEX)
@houses.document
class House(Document):
    house_guid = Keyword()
    house_num = Keyword()
    build_num = Keyword()
    str_num = Keyword()
    postal_code = Keyword()
    ifns_fl = Keyword()
    ifns_ul = Keyword()
    terr_ifns_ul = Keyword()
    terr_ifns_fl = Keyword()
    okato = Keyword()
    oktmo = Keyword()
    cad_num = Keyword()
    norm_doc = Keyword()
    est_status = Keyword()
    end_date = Date()
    start_date = Date()
    bazis_create_date = Date()
    bazis_update_date = Date()
    bazis_finish_date = Date()
    update_date = Date()
    counter = Keyword()


# 6. препроцессор
# удаляем не актуальные записи, у которых дата конца (END_DATE) раньше чем текущая (NOW_DATE)

dropPipeline = {
    "description": "drop old houses",
    "processors": [
        {
            "drop": {
                "if": """
        //Получаем текущую дату из параметра в формате ISO-8601
        ZonedDateTime zdt = ZonedDateTime.parse(ctx.bazis_update_date);
        long millisDateTime = zdt.toInstant().toEpochMilli();
        ZonedDateTime nowDate =
        ZonedDateTime.ofInstant(Instant.ofEpochMilli(millisDateTime), ZoneId.of("Z")); 

        //Получаем end_date 
        ZonedDateTime endDateZDT = ZonedDateTime.parse(ctx.end_date + "T00:00:00Z");
        long millisDateTimeEndDate = endDateZDT.toInstant().toEpochMilli();
        ZonedDateTime endDate =
        ZonedDateTime.ofInstant(Instant.ofEpochMilli(millisDateTimeEndDate), ZoneId.of("Z")); 

        // Сравниваем даты
          return endDate.isBefore(nowDate)
        """
            }
        }
    ]
}

IngestClient(es).put_pipeline(id='house_drop_pipeline', body=dropPipeline)


# 7. импорт
# HOUSE_XML = "AS_HOUSE_20191205_47755db6-3e03-4424-a7ce-efdac2a5b2d4.XML"
ADDR_INFO_DIC['housesFullXmlfile'] = HOUSE_XML
# print(ADDR_INFO_DIC)
# print(ADDR_INFO_DIC['workDir'] + HOUSE_XML)
doc = parse(ADDR_INFO_DIC['workDir'] + ADDR_INFO_DIC['housesFullXmlFile'])
# doc = parse(ADDR_INFO_DIC['workDir'] + HOUSE_XML)


def genFullHouserData():
    counter = 0
    for event, node in doc:
        if event == pulldom.START_ELEMENT and node.tagName == ADDR_INFO_DIC['housesObjectTag']:
            yield {
                "_index": ADDR_INFO_DIC['housesIndex'],
                "_type": "_doc",
                "_op_type": INDEX_OPER,
                'pipeline': ADDR_INFO_DIC['housesPipeLineId'],
                "_id": node.getAttribute("HOUSEID"),
                "ao_guid": node.getAttribute("AOGUID"),
                "region_code": node.getAttribute("REGIONCODE"),
                "start_date": node.getAttribute("STARTDATE"),
                "bazis_create_date": CREATE_DATE_ZERO,
                "bazis_finish_date": node.getAttribute("ENDDATE"),
                "bazis_update_date": UPDATE_DATE_ZERO,
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
            }


print("Импорт домов...")
print(ADDR_INFO_DIC['housesFullXmlFile'])
print("size: " + str(size(ADDR_INFO_DIC['housesFullXmlSize'], system=si)))
ADDR_CNT = 0
for ok, info in tqdm(parallel_bulk(es,
                                   genFullHouserData(),
                                   raise_on_error=False,
                                   raise_on_exception=False),
                     unit=' дом',
                     desc=' импортировано',
                     total=70_000_000):
    if (not ok):
        print(ok, info)
    ADDR_CNT = ADDR_CNT + 1
print(ADDR_CNT)


# 8. снэпшот
sn = SnapshotClient(es)
try:
    sn.delete(repository="fias", snapshot="houses")
except(Exception):
    pass

sn_body = {
    "indices": "houses",
    "ignore_unavailable": "true",
    "include_global_state": "false",
    "metadata": {
        "taken_by": "fias",
        "taken_because": "backup before upgrading"
    }
}
sn.create(repository="fias", snapshot="houses", body=sn_body)

# print(sn.get(repository="fias", snapshot="houses"))


def clearWorkDir():
    for the_file in os.listdir(WORK_DIR):
        file_path = os.path.join(WORK_DIR, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except Exception as e:
            print(e)


# 9. очистка
clearWorkDir()
