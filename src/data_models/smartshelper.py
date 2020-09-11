import pandas as pd
import numpy as np
import datetime
import matplotlib.pyplot as plt
import matplotlib.mlab as mlab
import math
import re
import xlrd
from src.data_models.SmartsAlertsDataModel import SmartsAlertsDataModel
from src.utility.DataModelUtility import execute_query_data_frame
from src.data_models.SmartsCsvDataModel import SmartsCsvDataModel

def metrics(args):
    series = pd.Series(args.value_counts())
    pdfreqpct = pd.Series(series/series.sum()).apply(lambda x: "{0:.0f}%".format(x*100))
    result = pd.concat([series, pdfreqpct], axis=1, sort=False)
    result.columns = ('count','%')
    return(result)

def fixNum(df):
    bool1 = df.apply(pd.to_numeric, errors='coerce', axis=1).isna().apply(any,axis=0)
    bool2 = bool1.loc[bool1].index.tolist()
    
    testdf = df.drop(columns=bool2)
    bool3 = (testdf.astype(int) == testdf.astype(float)).apply(all,axis=0)
    bool4 = bool3.loc[bool3].index.tolist()

    return(pd.concat([df[bool2],testdf[bool4],testdf.drop(columns=bool4)],axis=1))

def unSMART(df):
    df.columns = df.columns.str.replace(' ','')
    df = df[['Date','Time','InstrumentCode','ShortText','AlertCode','AccountIDName','ReissueCount','LongText']]
    df['LongText'] = df['LongText'].astype(str).str.replace(',','').str.replace('\n',' ').str.replace('\(MC\) ','').str.replace('\(RP\) ','')
    df['AlertCode'] = df['AlertCode'].astype(int)
    df['Datetime'] = pd.to_datetime(df['Date'].astype(str) + ' ' + df['Time'].astype(str))
    df['month'] = pd.to_datetime(df['Datetime']).dt.month
    df['year'] = pd.to_datetime(df['Datetime']).dt.year
    df['AccountIDName'] = df['AccountIDName'].fillna(value=0).astype(int)
    df['ShortText_mod'] = df['ShortText'].str.replace('\(MC\) ','').str.replace('\(RP\) ','').str.replace('SPOOFING EOD','SPOOFING')
    print('-> type fixed, kept', df.shape[0], 'rows,', df.shape[1], 'columns')
    return(df)

def cparty(df,account):
    df2 = df[(df.iloc[:,0] == account) | (df.iloc[:,1] == account)]
    df2['cp'] = (df2.iloc[:,0] != account) * df2.iloc[:,0] + (df2.iloc[:,1] != account) * df2.iloc[:,1]
    return(df2['cp'])

def littleQA(test,correct):
    if (test == correct):
        print('passed!')
    else: 
        print('failed!')

def totalUP(df):
    hsum = df.sum(axis=0)
    df2 = pd.concat([df,df.sum(axis=1)],axis=1).rename(columns={0:'TOTAL'})
    hsum = pd.DataFrame(df2.sum(axis=0)).rename(columns={0:'TOTAL'}).apply(pd.to_numeric, errors='coerce',axis=1).transpose()
    df3 = pd.concat([df2,hsum],axis=0)
    return(df3)

def wtf(date,symbol,acc):
    
    ### Gather DB Data
    query = """ 
            SELECT account_id, trading_pair, event_id, order_id, created, order_type, side, price, quantity, remaining_quantity
            from order_fill_event
            where created::date = '{}' and trading_pair = '{}' and account_id = {}
            order by event_id
            """.format(date, symbol, acc)
    exh1 = execute_query_data_frame(query,'engine')


    ### Gather CSV Data
    # new = SmartsCsvDataModel(download_data=True).evaluate(symbol,date,date,use_db=False)[symbol]
    query2 = """
            SELECT *
            FROM ms_prod.smarts_raw_data
            WHERE data_from_date = '{}' and event_type <> 'Initial' and symbol = '{}'
            """.format(date,symbol)
    new = execute_query_data_frame(query2, 'gemrdsdb', ssh = 'interim')
    new.columns = new.columns.str.replace('_crypto','')
    new['date_time'] = new['event_date'] + pd.to_timedelta(new['event_time'].astype(str)) + new['event_millis']

    ### SUBSET by account
    datatemp2 = new.loc[(new['account_id'] == acc)].rename(columns={'date_time':'dt'})
    datatemp2['execution_options'] = datatemp2['execution_options'].fillna('missing')

    
    ### ORDERS- placed
    datatemp3 = datatemp2.loc[datatemp2['event_type'].isin(['Place','Initial'])]
    datatemp3['val'] = datatemp3['original_quantity'] * datatemp3['limit_price']
    datatemp3gb = datatemp3.groupby(['account_id','order_id','execution_options','order_type','side'])

    order_dt = datatemp3gb['dt'].apply(pd.unique).apply(lambda x: x[0])
    order_count = datatemp3gb['event_id'].count()
    order_quant = datatemp3gb['original_quantity'].sum()
    order_price = datatemp3gb['val'].sum()/datatemp3gb['original_quantity'].sum()

    table_PLACE = pd.concat([order_count,order_dt,order_quant,order_price,datatemp3gb['val'].sum()],axis=1).add_prefix('PLACE_').rename(columns={'PLACE_0':'PLACE_avg_price', 'PLACE_event_id':'PLACE_count'})
 
    
    ### ORDERS- fill
    datatemp4 = datatemp2.loc[datatemp2['event_type'] == 'Fill']
    datatemp4['val'] = datatemp4['fill_quantity'] * datatemp4['fill_price'] 
    datatemp4gb = datatemp4.groupby(['account_id','order_id','execution_options','order_type','side'])

    fill_dt = datatemp4gb['dt'].apply(pd.unique).apply(lambda x: x[0])
    fill_quant = datatemp4gb['fill_quantity'].sum()
    fill_val = datatemp4gb['val'].sum()
    fill_price = datatemp4gb['val'].sum()/datatemp4gb['fill_quantity'].sum()

    table_FILL = pd.concat([fill_dt,fill_quant,fill_price,datatemp4gb['val'].sum()],axis=1).rename(columns={0:'avg_price'}).add_prefix('FILL_')


    ### ORDERS- cancel
    datatemp5 = datatemp2.loc[datatemp2['event_type'] == 'Cancel']
    datatemp5gb = datatemp5.groupby(['account_id','order_id','execution_options','order_type','side'])

    cancel_dt = datatemp5gb['dt'].apply(pd.unique).apply(lambda x: x[0])
    cancel_quant = datatemp5gb['remaining_quantity'].min()

    table_CANCEL = pd.concat([cancel_dt,cancel_quant],axis=1).add_prefix('CANCEL_')


    ### COMBINE
    table0 = pd.concat([table_PLACE,table_FILL,table_CANCEL],axis=1,sort=True)
    table1 = table0.loc[~table0['PLACE_count'].isna()]
    table1gb = table1.reset_index().groupby(['account_id','side','execution_options','order_type'])

    table2 = table1gb.sum().drop(columns=['order_id','PLACE_avg_price','FILL_avg_price'])
    table2['PLACE_price'] = table2['PLACE_val']/table2['PLACE_original_quantity']
    table2['FILL_price'] = table2['FILL_val']/table2['FILL_fill_quantity']
    table3 = table2[['PLACE_count', 'PLACE_original_quantity', 'PLACE_price', 'FILL_fill_quantity', 'FILL_price', 'CANCEL_remaining_quantity', ]]

    
    ### PD analysis
    new2 = new.loc[new['event_type']=='Fill']
    new2['val'] = new2['fill_quantity'] * new2['fill_price']
    new3 = new2.groupby(['side','account_id']).sum()[['val','fill_quantity']]
    new3['price'] = new3['val']/new3['fill_quantity']

    new4 = pd.pivot_table(new3,index='account_id',columns='side',values='price')
    new4['spread'] = new4['sell'] - new4['buy']

    new5 = pd.pivot_table(new3,index='account_id',columns='side',values='fill_quantity')
    new6 = pd.concat([new4.add_prefix('price_'),round(new5,1).add_prefix('quantity_')],axis=1).sort_values('price_sell',ascending=False)
    
    
    ### OUTPUT
    wtfdict = {}
    wtfdict[1] = exh1.set_index(['account_id','trading_pair','order_id','event_id'])
    wtfdict[2] = new
    wtfdict[3] = table3
    wtfdict[4] = new6
    
    return(wtfdict)

def textfunc(name,institutional,retailname,val):
    if (institutional == True):
        i = 'n institutional'
        n = name
    else: 
        i = ' retail'
        n = retailname

    if (val >= 1000):
        v = round(val/1000)
        u = 'M'
    else: 
        v = val
        u = 'K'
    text = 'This account belongs to {}, a{} user who transacted roughly ${}{} in value during the Alerting Period.'.format(n,i,v,u)
    return(text)


def identities(account_id_list,begin,end):

    PD_acclist = str(account_id_list).replace('[','').replace(']','')

    query = """
            SELECT adm.exchange_account_id, adm.account_group_id, adm.user_or_account_name, adm.is_institutional, ap.institution_name
            from account_derived_metadata as adm

            left join account_group as ap
            using(account_group_id)

            where adm.exchange_account_id in ({})
            """.format(PD_acclist)

    test3 = execute_query_data_frame(query, 'gemini', ssh = 'datalab_prod')

    query3 = """ 
            SELECT account_id, firsttable.side, sum(firsttable.price * COALESCE(secondtable.open_price,1) * quantity) as val

            from  
                (SELECT account_id, event_id, side, price, quantity, trading_pair, date_trunc('minute', created) start_time, substring(trading_pair::varchar,4,6) as trunc
                from order_fill_event 
                where created::date between '{t1}' and '{t2}' and account_id in ({x})) as firsttable
            left join 
                (SELECT substring(trading_pair::varchar,1,3) as truncmatch, start_time, open_price
                from candles_1m 
                where start_time::date between '{t1}' and '{t2}' and substring(trading_pair::varchar,4,6) = 'USD') as secondtable

            on firsttable.trunc = secondtable.truncmatch and firsttable.start_time = secondtable.start_time

            group by firsttable.side, account_id;
            """.format(t1 = begin.strftime("%Y%m%d"), t2 = end.strftime("%Y%m%d"), x=PD_acclist)

    identities0 = execute_query_data_frame(query3,'engine')
    identities1 = pd.pivot_table(identities0,index='account_id',columns='side',values=['val'])/1000
    
    identities = pd.concat([test3.set_index('exchange_account_id'), identities1.fillna(0).astype(int), identities1.sum(axis=1).astype(int)], axis=1).rename(columns={0:'valTOTAL'})
    identities['text'] = identities.apply(lambda x: textfunc(x['institution_name'], x['is_institutional'], x['user_or_account_name'], x['valTOTAL']), axis=1)
    
    return(identities.reset_index()[['index','text']])


def deets(df,accoi):
    df2 = df.loc[(~df['execution_options'].isin(['auction-only', 'block', 'indication-of-interest'])) & 
                 (df['event_type']!='Initial') & (df['account_id'] == accoi),['order_id','symbol','account_id','order_type','execution_options']].reset_index()

    step1 = pd.DataFrame(df2.fillna('standard order').groupby(['account_id','symbol','order_type','execution_options'])['order_id'].apply(pd.unique).apply(len))
    step1['proportion'] = round(step1/step1.sum()*100,1).astype(str) + '%'
    return(step1)
    
def fulllist(y):
    newlist = []
    for i in y:
        if math.isnan(i) == False:
            newlist.append(i)
    return(newlist)