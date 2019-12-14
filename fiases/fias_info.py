from urllib import request
import dateutil.parser as parser
import fiases.fias_data


def getUpdateVersion():
    """ Получение даты обновления """
    url = fiases.fias_data.FIAS_URL + fiases.fias_data.VERSION_TXT_FILE 

    request.urlretrieve(url, fiases.fias_data.WORK_DIR + fiases.fias_data.VERSION_TXT_FILE)
    with open(fiases.fias_data.WORK_DIR + fiases.fias_data.VERSION_TXT_FILE, 'r') as f:
        date = f.read()
        fiases.fias_data.VERSION_REPORT_DATE = date
        VERSION_DATE = parser.parse(date)
        # print(fias_data.VERSION_REPORT_DATE)

        fiases.fias_data.VERSION_DATE = VERSION_DATE.strptime(str(VERSION_DATE)[:10], '%Y-%d-%m').date()
        fiases.fias_data.VERSION_DATE_HOUSE = str(VERSION_DATE.strptime(str(VERSION_DATE)[:10], '%Y-%d-%m').date()) + fiases.fias_data.DATE_TIME_ZONE
