import fiases.fias_data
from fiases.address import import_address
from fiases.houses import import_houses
from fias_delta_update import update
from fiases.fias_data import ES
from snapshot import createFullSnapshot


def importFull():
    houses = fiases.fias_data.Houses()
    import_houses(houses=houses)

    address = fiases.fias_data.Address()
    import_address(address=address)


def updateFias():
    if not ES.indices.exists(fiases.fias_data.ADDRESS_INDEX):
        importFull()
    else:
        update(isDebug=True)


updateFias()
createFullSnapshot()
