import re
from urllib import request
from tqdm import tqdm
from rarfile import RarFile
from hurry.filesize import size, si
import fiases.fias_data


class TqdmUpTo(tqdm):
    """ Прогресс загрузки в консоли """

    def download(self, b=6_500_000, bsize=1, tsize=6_500_000):
        if tsize is not None:
            self.total = tsize
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
                            reporthook=t.download,
                            data=None)

def unrarUpdate(fias_object):
    """Распаковка обновления """
    rf = RarFile(fiases.fias_data.WORK_DIR +
                 fiases.fias_data.FIAS_DELTA_XML_RAR)

    fias_objectMatcher = re.compile(fias_object.FILE)
    for f in rf.infolist():
        if fias_objectMatcher.match(f.filename):
            fias_object.xml_delta_file = f.filename
            fias_object.xml_delta_file_size = f.file_size

    if (fias_object.xml_delta_file_size > 0):
        rf.extract(fias_object.xml_delta_file, fiases.fias_data.WORK_DIR)



def unRarFull(fias_object):
    """Распаковка из полной базы ФИАС"""
    rf = RarFile(fiases.fias_data.WORK_DIR + fiases.fias_data.FIAS_XML_RAR)

    objectMatcher = re.compile(fias_object.FILE)
    print('')
    for f in rf.infolist():
        if objectMatcher.match(f.filename):
            fias_object.xml_file = f.filename
            fias_object.xml_file_size = f.file_size
    if (fias_object.xml_file_size > 0):
        rf.extract(fias_object.xml_file, fiases.fias_data.WORK_DIR)

