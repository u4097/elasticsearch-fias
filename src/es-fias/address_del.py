#! /usr/bin/env python
# -*- coding: utf-8 -*-
from tqdm import trange, tqdm
from xml.dom import pulldom
from xml.dom.pulldom import parse
from elasticsearch.client import IngestClient, IndicesClient, SnapshotClient
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan, parallel_bulk, streaming_bulk
from elasticsearch_dsl import Q, Search, Index, Document, Date, Nested, InnerDoc, Keyword, Text, Integer, Short, Long, Range

# Local modules:
from fiasDownload import downloadUpdate, uprarFullAdddr
from fiasInfo import getUpdateVersion
from fias_data import Address
from init_db import createConnection

address = Address(workDir='/Volumes/DATA/downloads/',
                  addressIndex='address')

createConnection(host='localhost',timeout=20)
es = Elasticsearch()

# 1. версия
getUpdateVersion(address)

# print(address.versionReportDate)

# print(address.URL_FULL)

# 2. загрузка
# downloadUpdate(
# address.URL_FULL,
# address.FIAS_XML_RAR,
# workDir=address.workDir)

# 3. распаковка
uprarFullAdddr(address)


# 4. удаление
# HOUSE_XML = "AS_HOUSE_20191205_47755db6-3e03-4424-a7ce-efdac2a5b2d4.XML"
# ADDR_INFO_DIC['addressFullXmlfile'] = HOUSE_XML
# print(ADDR_INFO_DIC['workDir'] + HOUSE_XML)
# print('удаление:')
# print(address.workDir + address.addressDELFullXMLFile)
doc = parse(address.workDir + address.addressDELFullXMLFile)
# doc = parse(ADDR_INFO_DIC['workDir'] + HOUSE_XML)


def genFullHouserData():
    """Удаление не актуальных записей из индекса"""
    for event, node in doc:
        if event == \
                pulldom.START_ELEMENT and node.tagName \
                == address.ADDR_OBJECT_TAG:
            yield {
                "_index": address.IndexName,
                "_type": "_doc",
                "_op_type": address.DELETE_OPER,
                'pipeline': address.PIPELINE_ID,
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
                "bazis_create_date": address.CREATE_DATE_ZERO,
                "bazis_update_date": address.UPDATE_DATE_ZERO,
                "update_date": address.UPDATE_DATE_ZERO,
                "bazis_finish_date": node.getAttribute("ENDDATE")
            }


# print("Удаление адресов...")
# print(address.addressFullXmlFile)
# # print("size: " + str(size(ADDR_INFO_DIC['addressFullXmlSize'], system=si)))
ADDR_CNT = 0
for ok, info in tqdm(parallel_bulk(es,
                                   genFullHouserData(),
                                   raise_on_error=False,
                                   raise_on_exception=False),
                     unit=' адрес',
                     desc=' удалено',
                     total=address.ADDRESS_COUNT):
    if (not ok):
        print(ok, info)
    ADDR_CNT = ADDR_CNT + 1
print(ADDR_CNT)


# 6. снэпшот
# sn = SnapshotClient(es)
# try:
# sn.delete(repository="fias", snapshot=address.IndexName)
# except(Exception):
# pass

sn_body = {
    "indices": address.IndexName,
    "ignore_unavailable": "true",
    "include_global_state": "false",
    "metadata": {
        "taken_by": "fias",
        "taken_because": "backup before update"
    }
}
# sn.create(repository="fias", snapshot=ADDRESS_INDEX, body=sn_body)

# print(sn.get(repository="fias", snapshot="houses"))


# 7. очистка
# clearWorkDir()
