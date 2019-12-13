#! /usr/bin/env python
# -*- coding: utf-8 -*-
from tqdm import trange, tqdm
from xml.dom import pulldom
from lxml import etree
from xml.dom.pulldom import parse
from elasticsearch.helpers import parallel_bulk

# Local modules:
from fiasDownload import downloadUpdate, uprarUpdateHouses, clearWorkDir
import fiasData


def housesUpdate(isDebug, houses):

    IS_DEBUG = isDebug

    # 3. распаковка
    uprarUpdateHouses(houses)

    rootDeltaXML = etree.parse(fiasData.WORK_DIR
                               + houses.housesDeltaFile).getroot()
    houses.housesDeltaRecSize = len(rootDeltaXML.getchildren())

    doc = parse(fiasData.WORK_DIR + houses.housesDeltaFile)

    def updateIndex():
        """Обновление индекса"""
        for event, node in doc:
            if event == \
                    pulldom.START_ELEMENT and node.tagName \
                    == fiasData.HOUSES_OBJECT_TAG:
                yield {
                    "_index": fiasData.HOUSE_INDEX,
                    "_type": "_doc",
                    "_op_type": fiasData.INDEX_OPER,
                    'pipeline': fiasData.HOUSES_PIPELINE_ID,
                    "_id": node.getAttribute("HOUSEID"),
                    "ao_guid": node.getAttribute("AOGUID"),
                    "region_code": node.getAttribute("REGIONCODE"),
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
                    "start_date": node.getAttribute("STARTDATE"),
                    "end_date": node.getAttribute("ENDDATE"),
                    "update_date": node.getAttribute("UPDATEDATE"),
                    "bazis_create_date": fiasData.CREATE_DATE_ZERO,
                    "bazis_update_date": fiasData.VERSION_DATE_HOUSE,
                    "update_date": node.getAttribute("UPDATEDATE"),
                    "bazis_finish_date": node.getAttribute("ENDDATE")
                }

    ADDR_CNT = 0
    if IS_DEBUG:
        for ok, info in tqdm(parallel_bulk(fiasData.ES, updateIndex(),
                                           raise_on_error=False,
                                           raise_on_exception=False),
                             unit=' дом',
                             desc='обновлено',
                             total=houses.housesDeltaRecSize):
            ADDR_CNT = ADDR_CNT + 1
            if (not ok):
                if IS_DEBUG:
                    print(ok, info)

    else:
        for ok, info in parallel_bulk(fiasData.ES, updateIndex(),
                                      raise_on_error=False,
                                      raise_on_exception=False):
            ADDR_CNT = ADDR_CNT + 1
            if (not ok):
                print(ok, info)

    # return houses.housesDeltaRecSize
    return  ADDR_CNT


# housesUpdate(isDebug=True)