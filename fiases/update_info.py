#! /usr/bin/env python
# -*- coding: utf-8 -*-
# Статистика обновления ФИАС
from elasticsearch_dsl import Q, Search, Index, Document, Date, Nested, Boolean,analyzer, tokenizer, InnerDoc, Completion, Keyword, Text, Integer

import fiases.fias_data

INDEX = 'info'

info = Index(INDEX)
@info.document
class InfoDoc(Document):
    fias_version_date = Keyword()
    update_date = Date()
    rec_upd_address = Integer()
    rec_upd_houses = Integer()

    def save(self, **kwargs):
        return super().save(**kwargs)


def findInfoDoc():
    infoDoc = InfoDoc()
    InfoDoc.init()
    s = InfoDoc.search()
    i = s.query('term', fias_version_date=fiases.fias_data.VERSION_REPORT_DATE)
    infoSearchResult = i.execute(ignore_cache=True)


    if infoSearchResult.hits.total.value:
        infoDoc = InfoDoc.get(infoSearchResult.hits.hits[0]._id)
        print()
        # print('Документ найден: ')
        # print(infoDoc.fias_version_date)
        # print()
        pass
    else:
        # print()
        # print('Документ не найден документ. создаем новый')
        print()
        infoDoc.fias_version_date = fiases.fias_data.VERSION_REPORT_DATE
        infoDoc.save()
    return infoDoc


# InfoDoc.init()

# infoDoc = InfoDoc()
# infoDoc.fias_version_date = VERSION_DATE_REPORT
# infoDoc.count_update_houses = ADDRESS_COUNT
# infoDoc.save()
