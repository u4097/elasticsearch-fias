from urllib import request
import dateutil.parser as parser
import fiasData


def getUpdateVersion():
    """ Получение даты обновления """
    url = fiasData.FIAS_URL + fiasData.VERSION_TXT_FILE 

    request.urlretrieve(url, fiasData.WORK_DIR + fiasData.VERSION_TXT_FILE)
    with open(fiasData.WORK_DIR + fiasData.VERSION_TXT_FILE, 'r') as f:
        date = f.read()
        fiasData.VERSION_REPORT_DATE = date
        VERSION_DATE = parser.parse(date)
        # print(fiasData.VERSION_REPORT_DATE)

        fiasData.VERSION_DATE = VERSION_DATE.strptime(str(VERSION_DATE)[:10], '%Y-%d-%m').date()
        fiasData.VERSION_DATE_HOUSE = str(VERSION_DATE.strptime(str(VERSION_DATE)[:10], '%Y-%d-%m').date()) + fiasData.DATE_TIME_ZONE


