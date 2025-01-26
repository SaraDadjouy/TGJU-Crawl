# import packages
import requests
import pandas as pd
import datetime
import calendar
import jdatetime
from bs4 import BeautifulSoup
from lxml import etree

# main functions
def get_main_symbols():
    """
        symbols in main page
    """
    
    url = 'https://www.tgju.org'
    r = requests.get(url)
    soup = BeautifulSoup(r.content, "html.parser")
    dom = etree.HTML(str(soup))
    href = (dom.xpath('//div[@class = "nav-links"]/div[2]/ul/li/div/div/ul/li/ul/li/a/@href'))
    symbol_Fa = (dom.xpath('//div[@class = "nav-links"]/div[2]/ul/li/div/div/ul/li/ul/li/a//text()'))
    df = pd.DataFrame({'href': href, 'symbol_Fa': symbol_Fa})
    df['count_profile'] = df['href'].apply(lambda x: x.count('profile'))
    df = df.drop_duplicates(subset = 'symbol_Fa')
    df = df[df['count_profile'] == 1].drop('count_profile', axis=1).set_index('symbol_Fa')
    df['symbol_En'] = df['href'].apply(lambda x: x.split('/')[-1])
    df.loc['طلای دست دوم', 'symbol_En'] = 'gold_mini_size'
    df['SYMBOL'] = df['symbol_En'].apply(lambda x: x.upper()) 
    df = df.drop('href', axis=1).drop_duplicates()

    # Manually add crypto_tether 
    crypto_tether_data = {'symbol_En': 'crypto-tether', 'SYMBOL': 'CRYPTO-TETHER'} 
    df.loc['تتر'] = crypto_tether_data
    
    return df
    
def get_df_of_symbols():
    df = get_main_symbols()
    df['indicator'] = df[['symbol_En', 'SYMBOL']].apply(lambda x: 1 if x['symbol_En'] == x['SYMBOL'] else 0, axis=1)
    df = df[df['indicator'] == 0].drop('indicator', axis=1)
    return df.drop_duplicates()

def get_tgju_data(symbol):
    # get symbols
    df_of_symbols = get_df_of_symbols()
    SYMBOL = df_of_symbols.loc[symbol]['SYMBOL']
    # get data
    r = requests.get(f'https://platform.tgju.org/fa/tvdata/history?symbol={SYMBOL}')
    df_data = r.json()
    df_data = pd.DataFrame({'Date':df_data['t'],'Open':df_data['o'],'High':df_data['h'],'Low':df_data['l'],'Close':df_data['c'],})
    df_data['Date'] = df_data['Date'].apply(lambda x: datetime.datetime.fromtimestamp(x))
    df_data = df_data.set_index('Date')
    df_data.index = df_data.index.to_period("D")
    df_data.index=df_data.index.to_series().astype(str)
    df_data = df_data.reset_index()
    df_data['Symbol'] = symbol
    df_data['Date'] = pd.to_datetime(df_data['Date'])
    df_data['Weekday']=df_data['Date'].dt.weekday
    df_data['Weekday'] = df_data['Weekday'].apply(lambda x: calendar.day_name[x])
    df_data['J-Date']=df_data['Date'].apply(lambda x: str(jdatetime.date.fromgregorian(date=x.date())))
    #df_data = df_data.set_index('J-Date')
    df_data=df_data[['J-Date','Date','Weekday','Open','High','Low','Close', 'Symbol']]
    return df_data

def get_tgju_metadata(symbol):
    df_of_symbols = get_df_of_symbols()
    symbol_En = df_of_symbols.loc[symbol]['symbol_En']
    url = f'https://www.tgju.org/profile/{symbol_En}'
    r = requests.get(url)
    soup = BeautifulSoup(r.content, "html.parser")
    dom = etree.HTML(str(soup))

    data = {}
    current_rate_xpath = '//div[@class="block-last-change-percentage"]'
    current_rate_divs = dom.xpath(current_rate_xpath)
    if current_rate_divs:
        current_rate_div = current_rate_divs[0]
        current_price_elements = current_rate_div.xpath('.//span[@class="price"]/text()')
        current_change_elements = current_rate_div.xpath('.//span[@class="change"]/text()')
        if current_price_elements:
            current_price = current_price_elements[0].strip()
            data["نرخ فعلی"] = current_price
        if current_change_elements:
            current_change = current_change_elements[0].strip()
            data["تغییر"] = current_change

    # Extract elements with data-target attribute within the stocks-header div
    elements = dom.xpath('//div[@class="stocks-header"]//*[@data-target]')
    for element in elements:
        label_span = element.xpath('.//span[@class="label"]/text()')
        value_span = element.xpath('.//span[@class="value"]/text()')

        if label_span and value_span:
            label = label_span[0].strip(':').strip()
            value = value_span[0].strip()
            data[label] = value
    del data['نرخ فعلی:']
    data['symbol'] = symbol
    df = pd.DataFrame([data])
    
    return df


items_symbols = [
    'طلای 24 عیار',
    'طلای 18 عیار',
    'آبشده نقدی',
    'سکه بهار آزادی',
    'سکه امامی',
    'نیم سکه',
    'ربع سکه',
    'دلار',
    'یورو',
    'پوند',
    'بیت کوین',
    'اتریوم',
    'تتر'
]

combined_df = pd.DataFrame()
combined_metadata_df = pd.DataFrame()
for items_symbol in items_symbols:
    df = get_tgju_data(symbol = items_symbol)
    metadata_df = get_tgju_metadata(symbol = items_symbol)
    combined_df = pd.concat([combined_df, df], ignore_index=True)
    combined_metadata_df = pd.concat([combined_metadata_df, metadata_df], ignore_index=True)

combined_df.to_excel("tgju_data.xlsx", index=False)
combined_metadata_df.to_excel("tgju_metadata.xlsx")
