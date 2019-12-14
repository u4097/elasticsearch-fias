from urllib import request
import dateutil.parser as parser
import fias_data


def getUpdateVersion():
    """ Получение даты обновления """
    url = fias_data.FIAS_URL + fias_data.VERSION_TXT_FILE 

    request.urlretrieve(url, fias_data.WORK_DIR + fias_data.VERSION_TXT_FILE)
    with open(fias_data.WORK_DIR + fias_data.VERSION_TXT_FILE, 'r') as f:
        date = f.read()
        fias_data.VERSION_REPORT_DATE = date
        VERSION_DATE = parser.parse(date)
        # print(fias_data.VERSION_REPORT_DATE)

        fias_data.VERSION_DATE = VERSION_DATE.strptime(str(VERSION_DATE)[:10], '%Y-%d-%m').date()
        fias_data.VERSION_DATE_HOUSE = str(VERSION_DATE.strptime(str(VERSION_DATE)[:10], '%Y-%d-%m').date()) + fias_data.DATE_TIME_ZONE


