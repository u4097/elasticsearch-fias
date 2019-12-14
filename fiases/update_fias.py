#! /usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
import os
import sys
import fiases.fias_data
from fiases.fias_data import Address
from fiases.fias_info import getUpdateVersion
from fiases.address_upd import addressUpdate
from fiases.house_upd import housesUpdate
from fiases.update_info import findInfoDoc
from fiases.init_db import createConnection, IS_DEBUG
from fiases.fias_download import downloadUpdate, clearWorkDir
from fiases.index_address import index
from fiases.snapshot import createSnapshot, createFullSnapshot
from fiases.fias_data import ES


def update(isDebug=False):
    print()
    print('обновление...')
    print()

    address = fiases.fias_data.Address()
    address.createPreprocessor(ES)
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
    houses.createPreprocessor(ES)
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

    index(isUpdate=True)

    # снэпшот
    createFullSnapshot(repository=fiases.fias_data.REPOSITORY)

    #  очистка
    clearWorkDir()

    print("завершено")


update(isDebug=True)
# index(isUpdate=False)
