#! /usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import shutil
import datetime
from pathlib import Path
from elasticsearch.client import IngestClient
from fiases.init_db import createConnection


def createTmpDir():
    TMPDIR = '/tmp/'
    WRITE_MODE = 0o777
    work_dir = str(Path.home()) + TMPDIR
    if os.path.isdir(work_dir):
        shutil.rmtree(work_dir, ignore_errors=True)
    os.makedirs(work_dir, mode=WRITE_MODE, exist_ok=False)
    return work_dir


DATE_TIME_ZONE = 'T00:00:00Z'
def getDateNow():
    return datetime.datetime.now().strftime("%Y-%m-%d") + DATE_TIME_ZONE


WORK_DIR = createTmpDir()

ADDRESS_INDEX = 'address'
HOUSE_INDEX = 'houses'
HOST = 'localhost'
TIME_OUT = 20
ES = createConnection(host=HOST, timeout=TIME_OUT)
REPOSITORY = 'fias'
INFO_INDEX = 'info'
INDEX_OPER = 'index'
DELETE_OPER = 'delete'
CREATE_DATE_ZERO = getDateNow()
UPDATE_DATE_ZERO = getDateNow()
VERSION_DATE = ''
VERSION_DATE_HOUSE = ''
VERSION_REPORT_DATE = ''
ADDRESS_COUNT = 4_000_000
AS_ADDR_FILE = 'AS_ADDROBJ_*'
AS_DEL_ADDR_FILE = 'AS_DEL_ADDROBJ_*'
ADDR_OBJECT_TAG = 'Object'
ADDR_PIPELINE_ID = 'addr_drop_pipeline'

HOUSES_COUNT = 70_000_000
AS_HOUSES_FILE = 'AS_HOUSE_*'
AS_DEL_HOUSES_FILE = 'AS_DEL_HOUSE_*'
HOUSES_OBJECT_TAG = 'House'
HOUSES_PIPELINE_ID = 'house_drop_pipeline'

FIAS_XML_RAR = 'fias_xml.rar'
FIAS_DELTA_XML_RAR = 'fias_delta_xml.rar'
FIAS_URL = 'https://fias.nalog.ru/Public/Downloads/Actual/'
VERSION_TXT_FILE = 'VerDate.txt'

URL_FULL = FIAS_URL + FIAS_XML_RAR
URL_DELTA = FIAS_URL + FIAS_DELTA_XML_RAR


class Address:

    addressFullXmlFile = ''
    addressFullXmlSize = 0

    addressDELFullXMLFile = ''
    addressDELFullXmlSize = 0

    addressDeltaFile = ''
    addressDeltaSize = 0
    addressDeltaRecSize = 0

    def createPreprocessor(self):
        dropPipeline = {
            "description":
            "drop not actulal addresses",
            "processors": [{
                "drop": {
                    "if": "ctx.curr_status  != '0' "
                }
            }, {
                "drop": {
                    "if": "ctx.act_status  != '1'"
                }
            }, {
                "drop": {
                    "if": "ctx.live_status  != '1'"
                }
            }]
        }
        IngestClient(ES).put_pipeline(id=ADDR_PIPELINE_ID,
                                      body=dropPipeline)


class Houses:

    housesFullXmlFile = ''
    housesFullXmlSize = 0

    housesDELFullXMLFile = ''
    housesDELFullXmlSize = 0

    housesDeltaFile = ''
    housesDeltaSize = 0
    housesDeltaRecSize = 0

    def createPreprocessor(self):
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
        IngestClient(ES).put_pipeline(id=HOUSES_PIPELINE_ID,
                                      body=dropPipeline)
