import urllib
import urllib.request, json
from datetime import datetime, timedelta
import operator
from typing import Dict, Any

try:
    import httplib
except:
    import http.client as httplib

from urllib.error import HTTPError
import pandas as pd
from helperFunctions import stocksMOEX as stocks


def getForeignStocks(dfForeignStocks=None, cellToUpdate = 'priceHour'):

    marketChange: Dict[Any, Any] = {}
    errors = []
    bigMarketChange = []

    for ticker in stocks:
        query_url = f'https://query1.finance.yahoo.com/v6/finance/quote?region=US&lang=en&symbols={ticker}'
        try:
            with urllib.request.urlopen(query_url) as url:
                parsed = json.loads(url.read().decode())
                if parsed['quoteResponse']['result']:
                    if len(parsed['quoteResponse']['result'][0]) > 30:
                        if dfForeignStocks.loc[f'{ticker}', cellToUpdate] != 0:
                            oldPrice = dfForeignStocks.loc[f'{ticker}', cellToUpdate]
                        else:
                            oldPrice = 1 # просто рандомное число, лишь бы не 0 (иначе в словарь падает inf значения)
                        # print('old Price', oldPrice)
                        if parsed['quoteResponse']['result'][0]['ask'] != 0:
                            nowPrice = parsed['quoteResponse']['result'][0]['ask']
                        else:
                            nowPrice = oldPrice
                        # print('now Price', nowPrice)
                        change = round(100 * ((nowPrice - oldPrice) / oldPrice), 2)
                        # print('change val', change)
                        marketChange[ticker] = change
                        dfForeignStocks.loc[f'{ticker}', cellToUpdate] = nowPrice

                        if abs(change) > 4:
                            try:
                                temp = [parsed['quoteResponse']['result'][0]['shortName'], ticker,
                                        parsed['quoteResponse']['result'][0]['averageAnalystRating'],
                                        parsed['quoteResponse']['result'][0]['earningsTimestamp'],
                                        change]
                                bigMarketChange.append(temp)
                            except KeyError as e:
                                if e.args[0] == 'averageAnalystRating':
                                    try:
                                        temp = [parsed['quoteResponse']['result'][0]['shortName'], ticker,
                                                'No info', parsed['quoteResponse']['result'][0]['earningsTimestamp'], change]
                                    except KeyError:
                                        temp = [parsed['quoteResponse']['result'][0]['shortName'], ticker,
                                                'No info', 'No info', change]

                                bigMarketChange.append(temp)

                    else:
                        errors.append(ticker)
                else:
                    errors.append(ticker)
        # except HTTPError:
        #     errors.append(ticker)
        #     continue
        except:
            continue

    try:
        parsingTime = datetime.utcfromtimestamp(int(parsed['quoteResponse']['result'][0]['preMarketTime']))
    except:
        parsingTime = datetime.utcfromtimestamp(int(parsed['quoteResponse']['result'][0]['regularMarketTime']))

    dfForeignStocks.to_csv(r'dfForeignStocks.csv', encoding='utf-8')

    ordMarketChange = sorted(bigMarketChange, key=operator.itemgetter(-1), reverse=True)
    print(bigMarketChange)
    print(ordMarketChange)
    lenOrdMarCh = len(ordMarketChange)

    nowTick = (parsingTime + timedelta(hours=3)).strftime('%d-%m-%Y %H:%M:00')  # переводим к московскому времени
    if cellToUpdate == 'priceHour':
        previousTick = (parsingTime + timedelta(hours=2)).strftime(
            '%d-%m-%Y %H:%M:00')  # переводим к московскому времени с учетом отставания в 1 час
    elif cellToUpdate == 'priceDay':
        previousTick = (parsingTime + timedelta(days=1, hours=3)).strftime(
            '%d-%m-%Y %H:%M:00')
    elif cellToUpdate == 'priceWeek':
        previousTick = (parsingTime + timedelta(weeks=1, hours=3)).strftime(
            '%d-%m-%Y %H:%M:00')

    text = f'Существенные изменения котировок иностранных акций на временном промежутке ({previousTick} | {nowTick}), ' \
           f'\n- изм.котировок,%,\n- дата ближайшего фин. отчета,\n- ср. рейтинг аналитиков:\n\n'
    for i, el in enumerate(ordMarketChange):
        if i % 20 == 0 and i != 0:
            if el[3] != 'No info':
                try:
                    el[3] = datetime.utcfromtimestamp(int(el[3]))
                    el[3] = (el[3] + timedelta(hours=3)).strftime('%d-%m-%Y %H:%M:00')  # к Москве UCT
                except ValueError:
                    continue

            text = text + f'{el[0]} :\n- {el[4]}\n- {el[3]}\n- {el[2]}\n\n;'

        else:
            if el[3] != 'No info':
                try:
                    el[3] = datetime.utcfromtimestamp(int(el[3]))
                    el[3] = (el[3] + timedelta(hours=3)).strftime('%d-%m-%Y %H:%M:00')  # к Москве UCT
                except ValueError:
                    continue

            text = text + f'{el[0]} :\n- {el[4]}\n- {el[3]}\n- {el[2]}\n\n'

    text = text.split(';')

    return text, errors, lenOrdMarCh


if __name__ == '__main__':
    dfForeignStocks = pd.read_csv(r'dfForeignStocks.csv', encoding='utf-8', index_col='company')
    if datetime.now().weekday() == 4 and datetime.now().hour > 21:
        text, errors, amtPositions = getForeignStocks(dfForeignStocks=dfForeignStocks, cellToUpdate='priceWeek')
    elif datetime.now().hour > 21 and datetime.now().weekday() != 5 and datetime.now().weekday() != 6:
        text, errors, amtPositions= getForeignStocks(dfForeignStocks=dfForeignStocks, cellToUpdate='priceDay')
    elif 21 > datetime.now().hour > 8 and datetime.now().weekday() != 5 and datetime.now().weekday() != 6:
        text, errors, amtPositions = getForeignStocks(dfForeignStocks=dfForeignStocks, cellToUpdate='priceHour')
    # text, errors = getForeignStocks(dfForeignStocks=dfForeignStocks, cellToUpdate='priceHour')
    # print(datetime.now().hour, datetime.now().weekday())
    print(text)
    print(errors)
