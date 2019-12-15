import re
from urllib import request
from tqdm import tqdm
from rarfile import RarFile
from hurry.filesize import size, si
import fiases.fias_data


class TqdmUpTo(tqdm):
    """ Прогресс загрузки в консоли """

    def update_full(self, b=6_500_000, bsize=1, tsize=6_500_000):
        if tsize is not None:
            self.total = tsize
            self.update(b * bsize - self.n)

    def update_delta(self, b=100_000, bsize=1, tsize=100_000):
        if tsize is not None:
            self.total = tsize
            # will also set self.n = b * bsize
            self.update(b * bsize - self.n)


def downloadUpdate():
    """ Загрузка обновления ФИАС """
    file_name = fiases.fias_data.WORK_DIR + fiases.fias_data.FIAS_DELTA_XML_RAR

    request.urlretrieve(fiases.fias_data.URL_DELTA, filename=file_name)


def downloadFull():
    """ Загрузка полной базы ФИАС """
    file_name = fiases.fias_data.WORK_DIR + fiases.fias_data.FIAS_XML_RAR

    with TqdmUpTo(unit='B',
                  unit_scale=True,
                  miniters=1,
                  desc=fiases.fias_data.URL_FULL.split('/')[-1]) as t:  # all optional kwargs
        request.urlretrieve(fiases.fias_data.URL_FULL,
                            filename=file_name,
                            reporthook=t.update_full,
                            data=None)


def uprarUpdateAdddr(address):
    """Распаковка обновления """
    rf = RarFile(fiases.fias_data.WORK_DIR +
                 fiases.fias_data.FIAS_DELTA_XML_RAR)

    addressMatcher = re.compile(fiases.fias_data.AS_ADDR_FILE)
    for f in rf.infolist():
        if addressMatcher.match(f.filename):
            address.addressDeltaFile = f.filename
            address.addressDeltaSize = f.file_size

    if (address.addressDeltaSize > 0):
        rf.extract(address.addressDeltaFile, fiases.fias_data.WORK_DIR)


def uprarUpdateHouses(houses):
    """Распаковка обновления """
    rf = RarFile(fiases.fias_data.WORK_DIR +
                 fiases.fias_data.FIAS_DELTA_XML_RAR)

    housesMatcher = re.compile(fiases.fias_data.AS_HOUSES_FILE)
    for f in rf.infolist():
        if housesMatcher.match(f.filename):
            houses.housesDeltaFile = f.filename
            houses.housesDeltaSize = f.file_size

    if (houses.housesDeltaSize > 0):

        rf.extract(houses.housesDeltaFile, fiases.fias_data.WORK_DIR)
    else:
        print('files NOT FOUND!')


def uprarDelFullAdddr(address):
    """3.Распаковываем архив с удаленными записями"""
    rf = RarFile(fiases.fias_data.WORK_DIR + fiases.fias_data.FIAS_XML_RAR)
    # rf = RarFile(fle)

    addressMatcher = re.compile(fiases.fias_data.AS_ADDR_FILE)
    addressDelMatcher = re.compile(fiases.fias_data.AS_DEL_ADDR_FILE)
    for f in rf.infolist():
        if addressDelMatcher.match(f.filename):
            address.addressDELFullXMLFile = f.filename
            address.addressDELFullXmlSize = f.file_size

    if (address.addressDELFullXmlSize > 0):
        rf.extract(address.addressDELFullXMLFile, fiases.fias_data.WORK_DIR)


def unRarFullAdddr(address):
    """Распаковка адресов из полной базы ФИАС"""
    rf = RarFile(fiases.fias_data.WORK_DIR + fiases.fias_data.FIAS_XML_RAR)

    addressMatcher = re.compile(fiases.fias_data.AS_ADDR_FILE)
    print('')
    for f in rf.infolist():
        if addressMatcher.match(f.filename):
            address.addressFullXmlFile = f.filename
            address.addressFullXmlSize = f.file_size
    if (address.addressFullXmlSize > 0):
        rf.extract(address.addressFullXmlFile, fiases.fias_data.WORK_DIR)

def unRarFullHouses(houses):
    """Распаковка домов из полной базы ФИАС"""
    rf = RarFile(fiases.fias_data.WORK_DIR + fiases.fias_data.FIAS_XML_RAR)
    housesMatcher = re.compile(fiases.fias_data.AS_HOUSES_FILE)
    for f in rf.infolist():
        if housesMatcher.match(f.filename):
            houses.housesFullXmlFile = f.filename
            houses.housesFullXmlSize = f.file_size
    if (houses.housesFullXmlSize > 0):
        rf.extract(houses.housesFullXmlFile, fiases.fias_data.WORK_DIR)
    else:
        print('Файлы не найдены')
