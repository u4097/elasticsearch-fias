#! /usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
import fiasData
from fiasData import Address
from fiasInfo import getUpdateVersion
from addressUpd import addressUpdate
from houseUpd import housesUpdate
from updateInfo import findInfoDoc
from initDb import createConnection, IS_DEBUG
from fiasDownload import downloadUpdate, clearWorkDir
from indexAddress import index
from snapshot import createSnapshot, createFullSnapshot
from fiasData import ES


def update(isDebug=False):
    print()
    print('обновление...')
    print()

    address = fiasData.Address()
    address.createPreprocessor(ES)
    # 1. версия
    getUpdateVersion()

    print('версия:')
    print(fiasData.VERSION_REPORT_DATE)
    print()

    infoDoc = findInfoDoc()

    now = datetime.datetime.now()
    infoDoc.update(update_date=now)
    
    # 2. загрузка
    downloadUpdate()

    print()
    print('дома...')
    print()
    houses = fiasData.Houses()
    houses.createPreprocessor(ES)
    HOUSE_CNT = housesUpdate(isDebug=True,houses=houses)
    infoDoc.update(rec_upd_houses=HOUSE_CNT)

    print()
    print('адреса...')
    print()
    ADDR_CNT = addressUpdate(isDebug=True,address=address)
    infoDoc.update(rec_upd_address=ADDR_CNT)

    print()
    print('индексация...')
    print()

    index(isUpdate=True)


    # снэпшот
    createFullSnapshot(repository=fiasData.REPOSITORY)

    #  очистка
    clearWorkDir()

    print("завершено")


update(isDebug=True)
# index(isUpdate=False)
# createFullSnapshot(repository=fiasData.REPOSITORY)
