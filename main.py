from typing import Dict, Any

import requests
import apimoex
from datetime import datetime
from datetime import timedelta
import operator
import pytz
from helperFunctions import labels, companyNames, send_telegram
from justForTest import getForeignStocks
import pandas as pd


def getTextDay(nowTick=None):

    if nowTick.weekday() == 0:
        previousTick = str((nowTick - timedelta(days=3)).date())
    else:
        previousTick = str((nowTick - timedelta(days=1)).date())

    nowTick = str(nowTick.date())
    
    with requests.Session() as session:
        marketVals = {}
        for company in labels:
            data = apimoex.get_market_candles(session, security=company, interval=24, start=previousTick)
            marketVals[company] = data

        marketChange = {}
        for company in marketVals:
            try:
                startVal = marketVals[company][0]['close']
                endVal = marketVals[company][1]['close']
                change = (endVal - startVal) * 100 / startVal
                marketChange[company] = round(change, 2)
            except:
                continue

        ordMarketChange = sorted(marketChange.items(), key=operator.itemgetter(-1), reverse=True)

        bigMarketChange = []
        for row in ordMarketChange:
            if abs(row[1]) > 3:
                temp = [companyNames[row[0]], str(row[1]) + ' %']
                bigMarketChange.append(temp)

    text = f'Существенные изменения котировок акций на Московской бирже на временном промежутке ({previousTick} | {nowTick}):\n\n'
    for el in bigMarketChange:
        text = text + f'{el[0]} : {el[1]} \n'

    return text, len(bigMarketChange)


def getTextHour(nowTick=None):

    previousTick = nowTick - timedelta(hours=1)

    previousTick = previousTick.strftime("%Y-%m-%d %H:%M:00")

    # тк задержка ~15 минут - в отчет идут вот такие вот даты:
    nowTick = (nowTick - timedelta(minutes=17)).strftime("%Y-%m-%d %H:%M:00")

    with requests.Session() as session:
        marketVals = {}
        for company in labels:
            data = apimoex.get_market_candles(session, security=company, interval=1, start=previousTick)
            marketVals[company] = data

        marketChange: Dict[Any, Any] = {}
        for company in marketVals:
            try:
                startVal = marketVals[company][0]['close']
                endVal = marketVals[company][-1]['close']
                change = (endVal - startVal) * 100 / startVal
                marketChange[company] = round(change, 2)
            except:
                continue

        ordMarketChange = sorted(marketChange.items(), key=operator.itemgetter(-1), reverse=True)

        bigMarketChange = []
        for row in ordMarketChange:
            if abs(row[1]) > 3:
                temp = [companyNames[row[0]], str(row[1]) + ' %']
                bigMarketChange.append(temp)

    text = f'Существенные изменения котировок акций на Московской бирже на временном промежутке ({previousTick} | {nowTick}):\n\n'
    for el in bigMarketChange:
        text = text + f'{el[0]} : {el[1]} \n'

    return text, len(bigMarketChange)


if __name__ == '__main__':
    tz_Moscow = pytz.timezone('Europe/Moscow')
    nowTime = datetime.now(tz_Moscow)

    if nowTime.weekday() != 5 and nowTime.weekday() != 6:
        if nowTime.hour > 22:
            text, amtPositions = getTextDay(nowTick=nowTime)
            if amtPositions != 0:
                send_telegram(text=text)
        else:
            text, amtPositions = getTextHour(nowTick=nowTime)
            if amtPositions != 0:
                send_telegram(text=text)
    
    dfForeignStocks = pd.read_csv(r'dfForeignStocks.csv', encoding='utf-8', index_col='company')
    if datetime.now().weekday() == 4 and datetime.now().hour > 21:
        print('___week___')
        text, errors, amtPositions = getForeignStocks(dfForeignStocks=dfForeignStocks, cellToUpdate='priceWeek')
    elif datetime.now().hour >= 23 and datetime.now().weekday() != 5 and datetime.now().weekday() != 6:
        print('___day___')
        text, errors, amtPositions = getForeignStocks(dfForeignStocks=dfForeignStocks, cellToUpdate='priceDay')
    elif 23 > datetime.now().hour > 8 and datetime.now().weekday() != 5 and datetime.now().weekday() != 6:
        print('___hour___')
        text, errors, amtPositions = getForeignStocks(dfForeignStocks=dfForeignStocks, cellToUpdate='priceHour')
        
    print(type(text), len(text))
    print(amtPositions)
    if amtPositions != 0:
        for elText in text:
            # try:
            send_telegram(text=elText)
            # except:
            #     continue
    print(text)
