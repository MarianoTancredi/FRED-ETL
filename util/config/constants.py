#<--------------------------------------------------------------- Download Info ----------------------------------------------------------------------------->
API_KEY = ''
URL_TEMPLATE = 'https://api.stlouisfed.org/fred/series/observations?series_id={SeriesId}&frequency={Frequency}&units={Type}&api_key={API_KEY}&file_type=json'

FREQUENCY_TRANSLATIONS  = {
                                'Daily': 'd',
                                'Weekly': 'w',
                                'Biweekly': 'bw',
                                'Monthly': 'm',
                                'Quarterly': 'q',
                                'Semiannual': 'sa',
                                'Annual': 'a'
                            }
    
UNIT_TRANSLATIONS = {
                        'Index': 'lin',
                        'Change': 'chg',
                        'Change from Year Ago': 'ch1',
                        'Percent': 'pch',
                        'Percent from Year Ago': 'pc1',
                        'Compounded Annual Rate of Change': 'pca',
                        'Continuously Compounded Rate of Change': 'cch',
                        'Continuously Compounded Annual Rate of Change': 'cca',
                        'Natural Log': 'log'
                    }

#<--------------------------------------------------------------- Parser Info ----------------------------------------------------------------------------->
START_DATE = '2000-01-01'

#<--------------------------------------------------------------- Database Info ----------------------------------------------------------------------------->
TABLE_NAME = ''
DATE_COLUMN = ''
DRIVER_NAME = ''
SERVER_NAME = ''
DATABASE_NAME = ''
SQL_ENGINE = ''