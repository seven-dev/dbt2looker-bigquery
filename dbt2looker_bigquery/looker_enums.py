from enum import Enum

# Lookml types
class LookerMeasureType(str, Enum):
    number = 'number'
    string = 'string'
    average = 'average'
    average_distinct = 'average_distinct'
    count = 'count'
    count_distinct = 'count_distinct'
    list = 'list'
    max = 'max'
    median = 'median'
    median_distinct = 'median_distinct'
    min = 'min'
    sum = 'sum'
    sum_distinct = 'sum_distinct'

class LookerValueFormatName(str, Enum):
    decimal_0 = 'decimal_0'
    decimal_1 = 'decimal_1'
    decimal_2 = 'decimal_2'
    decimal_3 = 'decimal_3'
    decimal_4 = 'decimal_4'
    usd_0 = 'usd_0'
    usd = 'usd'
    gbp_0 = 'gbp_0'
    gbp = 'gbp'
    eur_0 = 'eur_0'
    eur = 'eur'
    id = 'id'
    percent_0 = 'percent_0'
    percent_1 = 'percent_1'
    percent_2 = 'percent_2'
    percent_3 = 'percent_3'
    percent_4 = 'percent_4'

