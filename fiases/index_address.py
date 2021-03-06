#! /usr/bin/env python
# -*- coding: utf-8 -*-
from tqdm import tqdm
from elasticsearch.helpers import scan
from elasticsearch_dsl import Index, \
    Document, Date, Nested, InnerDoc, Keyword, Text, Integer, Short, Long, Range

import fiases.fias_data
from fiases.fias_data import ES


def createIndex(isUpdate=True):
    # 5. Создаем класс для хранения Адресов
    address = Index(address.INDEX)
    address.close()

    houses = Index(houses.INDEX)
    @houses.document
    class House(Document, InnerDoc):
        houseId = Keyword()
        house_num = Keyword()
        build_num = Keyword()
        str_num = Keyword()
        postal_code = Keyword()
        ifns_fl = Keyword()
        ifns_ul = Keyword()
        counter = Keyword()

    @address.document
    class Address(Document):
        ao_guid = Keyword(required=True)
        parent_guid = Keyword()

        act_status = Integer()
        curr_status = Integer()
        live_status = Integer()
        oper_status = Integer()

        formal_name = Keyword()
        off_name = Keyword()
        short_name = Keyword()

        region_code = Keyword()
        ao_level = Keyword()

        area_code = Keyword()
        auto_code = Keyword()
        extr_code = Keyword()
        city_ar_code = Keyword()
        city_code = Keyword()
        street_code = Keyword()
        plan_code = Keyword()
        place_code = Keyword()
        sub_ext_code = Keyword()
        plain_code = Keyword()
        code = Keyword()

        okato = Keyword()
        oktmo = Keyword()

        postal_code = Keyword()
        terr_ifns_fl = Keyword()
        terr_ifns_ul = Keyword()
        ifns_fl = Keyword()
        ifns_ul = Keyword()
        norm_doc = Keyword()

        district = Keyword()
        district_type = Keyword()
        settlement = Keyword()
        settlement_type = Keyword()
        street = Keyword()
        street_type = Keyword()

        start_date = Date()
        end_date = Date()
        update_date = Date()

        street_address_suggest = Text(analyzer="autocomplete")
        full_address = Keyword()
        district_full = Keyword()
        settlement_full = Keyword()
        street_full = Keyword()

        houses = Nested(House)

        def add_house(self, house_num, build_num):
            self.houses.append(
                House(house_num=house_num, build_num=build_num))

        def save(self, **kwargs):
            return super().save(**kwargs)

    Address.init()

    address.open()

    queryAllStreet = {
        "query": {
            "bool": {
                "must": [{
                         "match_all": {}
                         }],
                "filter": {
                    "term": {
                        "ao_level": "7"
                    }
                }
            }
        }

    }

    queryByRegion = {
        "query": {
            "bool": {
                "must": [{
                    "term": {
                        "region_code": {
                            "value": "13"
                        }
                    }
                }],
                "filter": {
                    "term": {
                        "ao_level": "7"
                    }
                }
            }
        }
    }

    update_date = str(fiases.fias_data.VERSION_DATE) \
        + fiases.fias_data.DATE_TIME_ZONE

    queryUpdate = {
        "query": {
            "bool": {
                "must": [{
                         "term": {
                             "update_date": {
                                 "value": update_date
                             }
                         }
                         }],
                "filter": {
                    "term": {
                        "ao_level": "7"
                    }
                }
            }
        }
    }

    # Выбираем все улицы
    if isUpdate:
        print("indexing ...")
        scanResStreet = scan(ES,
                             scroll='1h',
                             query=queryUpdate,
                             index=address.INDEX)
        ADDR_UPDATE_CNT = Address.search()\
            .query("term", update_date=update_date)\
            .filter("term", ao_level="7").count()
    else:
        print("Full indexing ...")
        scanResStreet = scan(ES,
                             scroll='1h',
                             query=queryAllStreet,
                             index=address.INDEX)

        ADDR_UPDATE_CNT = Address.search()\
            .query("term", ao_level="7").count()

    print("ADDR_UPDATE_CNT: ", ADDR_UPDATE_CNT)
    # Обновляем индеск street_address_suggest
    addrSearch = Address.search()
    homeSearch = House.search()

    print("address: ", Address.search().count())
    print("houses: ", House.search().count())
    houseList = []
    for address in tqdm(scanResStreet,
                        unit=' address',
                        desc='indexed',
                        total=ADDR_UPDATE_CNT):
        # source = address['_source']
        # Получаем улицу
        street = Address.get(address['_id'])

        try:
            # Находим город
            city = addrSearch.query("match",
                                    ao_guid=street.parent_guid).execute()[0]

            if (not city.parent_guid):
                # Для Москвы, Питера и Севастополя регион равен городу.
                district = city
            else:
                # Находим регион
                district = addrSearch.query("match",
                                            ao_guid=city.parent_guid).execute()[0]
        except (Exception):
            print()
            print("Ошибка индексации: ")
            print("city: " + city)
            print()
            print("city.parent_guid: " + str((city.parent_guid == False)))
            print(address['_id'])
            print(street.short_name + "." + street.off_name.lower().strip() +
                  ", " + city.short_name + "." + city.off_name.lower().strip() +
                  ", " + district.short_name.lower().strip() + "." +
                  district.off_name.lower().strip())
            print()
            continue
        else:
            houses = homeSearch.filter("term", ao_guid=street.ao_guid)
            for house in houses.scan():
                houseList.append(house)
        try:
            if (street.postal_code):
                postal_code = street.postal_code + ', '
            else:
                postal_code = ''
            street.update(street_type=street.short_name.strip(),
                          street=street.off_name.strip(),
                          settlement=city.off_name.strip(),
                          settlement_type=city.short_name.strip(),
                          district=district.off_name.strip(),
                          district_type=district.short_name.strip(),
                          street_address_suggest=district.off_name.lower().strip()
                          + " " + city.off_name.lower().strip()
                          + " " + street.off_name.lower().strip(),
                          full_address=postal_code
                          + district.short_name
                          + ' ' + district.off_name + ', '
                          + city.short_name + ' ' + city.off_name + ', '
                          + street.short_name + ' ' + street.off_name,
                          houses=houseList
                          )
            houseList[:] = []
        except(Exception):
            print(house)
    return ADDR_UPDATE_CNT
    print("finish")



# createIndex(isUpdate=True)
