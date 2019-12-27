#! /usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import shutil
import datetime
from pathlib import Path
from elasticsearch.client import IngestClient
from elasticsearch import Elasticsearch, RequestsHttpConnection
from fiases.init_db import createConnection
from elasticsearch_dsl.connections import connections

addr_guid_search_template={ "script": { "lang": "mustache", "source": {
  "_source": {
    "includes": [
      "full_address",
      "address",
      "district_full",
      "settlement_full",
      "street_full",
      "postal_code",
      "region_code",
      "okato",
      "ifns_fl",
      "code",
      "ao_guid",
      "ao_level"
    ]
  }, "query": { "term": {
      "ao_guid": {
        "value": "{{guid}}" } } } } } }

def createAddrSearchTemplate(script_id="address_guid"):
    ES.put_script(id=script_id,body=addr_guid_search_template)

TMPDIR = '/tmp/'
# WORK_DIR =  str(Path.home()) + TMPDIR
#!!!! CHANGE THIS BACK !!!!!
WORK_DIR = "/Volumes/DRIVE120G" + TMPDIR

def createTmpDir():
    WRITE_MODE = 0o777
    if os.path.isdir(WORK_DIR):
        shutil.rmtree(WORK_DIR, ignore_errors=True)
    os.makedirs(WORK_DIR, mode=WRITE_MODE, exist_ok=False)
    return WORK_DIR


DATE_TIME_ZONE = 'T00:00:00Z'
def getDateNow():
    return datetime.datetime.now().strftime("%Y-%m-%d") + DATE_TIME_ZONE


HOST = 'es01'
TIME_OUT = 2000
ES = Elasticsearch(host='es01',port=9200, timeout=20000, connection_class=RequestsHttpConnection)
connections.configure(
    default={'hosts': 'es01'},
)
REPOSITORY = 'fias'
INFO_INDEX = 'info'
INDEX_OPER = 'index'
DELETE_OPER = 'delete'
CREATE_DATE_ZERO = getDateNow()
UPDATE_DATE_ZERO = getDateNow()
VERSION_DATE = ''
VERSION_DATE_HOUSE = ''
VERSION_REPORT_DATE = ''


# Fias connections data

FIAS_XML_RAR = 'fias_xml.rar'
FIAS_DELTA_XML_RAR = 'fias_delta_xml.rar'
FIAS_URL = 'https://fias.nalog.ru/Public/Downloads/Actual/'
VERSION_TXT_FILE = 'VerDate.txt'

URL_FULL = FIAS_URL + FIAS_XML_RAR
URL_DELTA = FIAS_URL + FIAS_DELTA_XML_RAR


class Address:

    INDEX = 'address'
    COUNT = 4000000
    FILE = 'AS_ADDROBJ_*'
    TAG = 'Object'
    PIPELINE = 'addr_drop_pipeline'

    xml_file = ''
    xml_file_size = 0

    xml_delta_file = ''
    xml_delta_file_size = 0
    delta_rec_size = 0

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
        IngestClient(ES).put_pipeline(id=PIPELINE,
                                      body=dropPipeline)


class Houses:
    INDEX = 'houses'
    COUNT = 70000000
    FILE = 'AS_HOUSE_*'
    AS_DEL_HOUSES_FILE = 'AS_DEL_HOUSE_*'
    TAG = 'House'
    PIPELINE = 'house_drop_pipeline'

    xml_file = ''
    xml_file_size = 0

    xml_delta_file = ''
    xml_delta_file_size = 0
    delta_rec_size = 0

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
        IngestClient(ES).put_pipeline(id=self.PIPELINE,
                                      body=dropPipeline)


class Stead:

    INDEX = 'stead'

    COUNT = 12_513_854
    FILE = 'AS_STEAD_*'
    TAG = 'Stead'
    PIPELINE = 'stead_drop_pipeline'

    DEL_FILE = 'AS_DEL_STEAD_*'

    xml_file = ''
    xml_file_size = 0

    xml_delta_file = ''
    xml_delta_file_size = 0
    delta_rec_size = 0


    def createPreprocessor(self):
        dropPipeline = {
            "description": "drop old stead",
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
        IngestClient(ES).put_pipeline(id=self.PIPELINE,
                                      body=dropPipeline)

class Room:

    INDEX = 'room'

    COUNT = 51_974_148 # 4h 20 min 
    FILE = 'AS_ROOM_*'
    TAG = 'Room'
    PIPELINE = 'room_drop_pipeline'

    DEL_FILE = 'AS_DEL_ROOM_*'

    xml_file = ''
    xml_file_size = 0

    xml_delta_file = ''
    xml_delta_file_size = 0
    delta_rec_size = 0


    def createPreprocessor(self):
        dropPipeline = {
            "description": "drop old room",
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
        IngestClient(ES).put_pipeline(id=self.PIPELINE,
                                      body=dropPipeline)

