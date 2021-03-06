# For data manipulation
#import pandas as pd

# To extract fundamental data
from bs4 import BeautifulSoup as bs
import requests

def get_fundamental_data(df):
    for symbol in df.index:
        try:
            url = ("http://finviz.com/quote.ashx?t=" + symbol.lower())
            soup = bs(requests.get(url).content, features='html5lib') 
            for m in df.columns:                
                df.loc[symbol,m] = fundamental_metric(soup,m)                
        except Exception as e:
            print (symbol, 'not found')
    return df

def fundamental_metric(soup, metric):
    return soup.find(text = metric).find_next(class_='snapshot-td2').text

stock_list = ['AMZN','GOOG','PG','KO','IBM','DG','XOM','KO','PEP','MT','NL','ALDW','DCM','GSB','LPL']

metric = ['P/B',
'P/E',
'Forward P/E',
'PEG',
'Debt/Eq',
'EPS (ttm)',
'Dividend %',
'ROE',
'ROI',
'EPS Q/Q',
'Insider Own'
]


df = pd.DataFrame(index=stock_list,columns=metric)
df = get_fundamental_data(df)

print df.head()

#Businesses which are quoted at low valuations
df = df[(df['P/E'].astype(float)<20) & (df['P/B'].astype(float) < 3)]

#Businesses which have demonstrated earning power
df['EPS Q/Q'] = df['EPS Q/Q'].map(lambda x: x[:-1])
df = df[df['EPS Q/Q'].astype(float) > 10]

#Businesses earning good returns on equity while employing little or no debt
df['ROE'] = df['ROE'].map(lambda x: x[:-1])
df = df[(df['Debt/Eq'].astype(float) < 1) & (df['ROE'].astype(float) > 10)]

#Management having substantial ownership in the business
df['Insider Own'] = df['Insider Own'].map(lambda x: x[:-1])
df = df[df['Insider Own'].astype(float) > 30]

print df.head()
