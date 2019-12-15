#! /usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import datetime
import fiases.fias_data
from fiases.fias_data import Address
from fiases.fias_info import getUpdateVersion
from fiases.address_upd import addressUpdate
from fiases.house_upd import housesUpdate
from fiases.update_info import findInfoDoc
from fiases.fias_download import downloadUpdate 
from fiases.index_address import createIndex
from fiases.snapshot import  createFullSnapshot
from fiases.fias_data import ES


def update(isDebug=False):
    print()
    print('обновление...')
    print()

    address = fiases.fias_data.Address()
    address.createPreprocessor()
    # 1. версия
    getUpdateVersion()

    print('версия:')
    print(fiases.fias_data.VERSION_REPORT_DATE)
    print()

    infoDoc = findInfoDoc()

    now = datetime.datetime.now()
    infoDoc.update(update_date=now)

    # 2. загрузка
    downloadUpdate()

    print()
    print('дома...')
    print()
    houses = fiases.fias_data.Houses()
    houses.createPreprocessor()
    HOUSE_CNT = housesUpdate(isDebug=True, houses=houses)
    infoDoc.update(rec_upd_houses=HOUSE_CNT)

    print()
    print('адреса...')
    print()
    ADDR_CNT = addressUpdate(isDebug=True, address=address)
    infoDoc.update(rec_upd_address=ADDR_CNT)

    print()
    print('индексация...')
    print()

    createIndex(isUpdate=True)

    # снэпшот
    createFullSnapshot()


    print("завершено")


# update(isDebug=True)
