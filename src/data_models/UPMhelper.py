import os; os.chdir('/Users/sqian/MKTSRV')
import pandas as pd; pd.set_option('mode.chained_assignment',None)
import numpy as np
import datetime
import matplotlib.pyplot as plt
import matplotlib.mlab as mlab
import math
import re
import xlrd
from xlsxwriter.utility import xl_rowcol_to_cell
from random import sample
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.cbook as cbook
from src.data_models.SmartsCsvDataModel import SmartsCsvDataModel
from src.data_models.smartshelper import metrics, fixNum, unSMART, cparty, wtf, fulllist
from src.data_models.SmartsDataModel import SmartsDataModel
from src.utility.DataModelUtility import execute_query_data_frame


def UPMalerts(upmalerts,case,path,beginning,ending):

    ### DEFINE parameters
    os.chdir(path)
    casechoice = upmalerts['CaseNo'] == case
    os.chdir(path)
    eval_dt = upmalerts.loc[casechoice, 'Datetime'].get_values()[0]
    eval_date = upmalerts.loc[casechoice, 'Datetime'].dt.date.get_values()[0]
    eval_time = upmalerts.loc[casechoice, 'Datetime'].dt.time.get_values()[0]
    instruments = upmalerts.loc[casechoice, 'InstrumentCode'].get_values()[0]


    ### GATHER full order book data
    query2 = """
        SELECT *
        FROM ms_prod.smarts_raw_data
        WHERE data_from_date = '{}' and symbol = '{}'
        """.format(eval_date,instruments)
    test = execute_query_data_frame(query2, 'gemrdsdb', ssh = 'interim')


    ### CLEAN full order book data
    test.columns = test.columns.str.replace('_crypto','')
    test['date_time'] = pd.to_datetime(test['event_date'] + pd.to_timedelta(test['event_time'].astype(str)) + test['event_millis'])


    ### USE Alert Table for time ranges (5,10,60) minutes
    UPMtable = upmalerts
    UPMtable['LongText2'] = UPMtable['LongText'].str.extract('\D+(Price Change.+)Bid.+',expand=True)
    UPMtable['min05'] = UPMtable['LongText2'].str.findall('Price\D+(\d+\D+)\sis').apply(lambda x: '5 minutes' in x) * 5
    UPMtable['min10'] = UPMtable['LongText2'].str.findall('Price\D+(\d+\D+)\sis').apply(lambda x: '10 minutes' in x) * 10
    UPMtable['min60'] = UPMtable['LongText2'].str.findall('Price\D+(\d+\D+)\sis').apply(lambda x: '1 hour' in x) * 60
    rangeslist = UPMtable.replace({0:np.NaN}).groupby('CaseNo')[['min05', 'min10', 'min60']].apply(lambda row: list(row.get_values()[0]))
    
    
    ### EXH 1
    bool_exh1 = (test['event_date'] == eval_date) & (test['event_type'] == 'Fill')
    col_exh1 = ['event_id','side','account_id','date_time','order_type','fill_price','fill_quantity','fees']
    exh1_draft = test.loc[bool_exh1,col_exh1].sort_values('date_time').reset_index(drop=True)
    exh1_draft['fees'] = round((exh1_draft['fees']/(exh1_draft['fill_price']*exh1_draft['fill_quantity'])*100),1).astype(str) + '%'
    exh1_draft['tod'] = exh1_draft['date_time'].dt.hour + exh1_draft['date_time'].dt.minute/60

    endindex = exh1_draft.loc[exh1_draft['date_time'] >= eval_dt].index[0] + 2
    exh1_draft2_p2 = exh1_draft[(endindex-2):endindex]

    val = pd.unique(exh1_draft2_p2['fill_price']).tolist()[0]
    accoi = pd.unique(exh1_draft2_p2['account_id']).tolist()
    rangeoi = fulllist(rangeslist[case])
    fulllist(rangeslist[case])

    temp = pd.DataFrame()
    for i in rangeoi:
        temp1 = exh1_draft.loc[exh1_draft['date_time'] <= (eval_dt - np.timedelta64(int(i),'m'))]
        dt = temp1.loc[test.index == temp1.index.max(),'date_time'].get_values()[0]
        temp2 = exh1_draft.loc[exh1_draft['date_time'] == dt]
        temp2['testvalbool'] = val > temp2['fill_price']
        temp2['absdiff'] = temp2['fill_price'].apply(lambda x: abs(val-x))
        temp3 = temp2.loc[(temp2['testvalbool'] == True) & (temp2['absdiff'] == temp2.loc[temp2['testvalbool'] == True,'absdiff'].max()) | 
                          (temp2['testvalbool'] == False) & (temp2['absdiff'] == temp2.loc[temp2['testvalbool'] == False,'absdiff'].min())]
        temp = temp.append(temp3).drop_duplicates().sort_values('date_time')
    exh1 = pd.concat([temp, exh1_draft2_p2], axis=0,sort=True).dropna(axis=1)
    
    
    ### EXH 2
    query = """select event_id, order_id, account_id, trading_pair as security, side, 
            liquidity_indicator as liquidity, price, quantity, remaining_quantity
            from order_fill_event where event_id = {};""".format(exh1[-1:]['event_id'].get_values()[0])
    exh2 = execute_query_data_frame(query,'engine')


    ### EXH 3
    leadup = min(beginning - datetime.timedelta(days=30),datetime.date(2019,1,1))
    query0 = """select * from daily_conversion_rates 
                where created::date between '{}' and '{}' and trading_pair = '{}'
                order by created;""".format(leadup.strftime("%Y-%m-%d"),ending.strftime("%Y-%m-%d"),instruments)
    dailyclose = execute_query_data_frame(query0, 'engine')
    exh3col = ['order_id','account_id', 'date_time', 'event_type', 'side', 'symbol', 'fill_price', 'fill_quantity','original_quantity','remaining_quantity']
    exh3 = test.loc[test['order_id'] == exh2.loc[exh2['liquidity'] == 'maker', 'order_id'].get_values()[0]][exh3col]


    ### EXH 4
    wtfsymbol = pd.unique(exh2['security'])[0]
    wtfacc = pd.unique(exh2['account_id']).tolist()
    exh4 = pd.concat([wtf(eval_date,wtfsymbol,wtfacc[0])[3],wtf(eval_date,wtfsymbol,wtfacc[1])[3]],axis=0)
    exh4['fill_rate'] = round(exh4['FILL_fill_quantity']/exh4['PLACE_original_quantity']*100,1).astype(str) + '%'
    
    
    ### FIG 4
    plt.rcParams['figure.figsize'] = (14,4)
    plt.plot(exh1_draft['tod'],exh1_draft['fill_price'],'go--')
    plt.ylabel(case + ":Intraday Fill Price ({})".format(instruments))
    plt.grid(True)
    plt.xlim(min(exh1_draft['tod'].min()*0.80,0),min(exh1_draft['tod'].max()*1.2,24.1))
    ymin = max(exh1_draft['fill_price'].min(),0); ymax = exh1_draft['fill_price'].max()
    yrange = ymax - ymin
    plt.ylim(ymin - yrange/5,ymax + yrange/5)
    plt.title('{}- {} Intraday Price '.format(eval_date.strftime("%B, %d %Y"),instruments))
    plt.savefig("{}_{}_fig_4.png".format(beginning.strftime("%Y%b"),case))
    plt.show()


    ### FIG 5
    plt.rcParams['figure.figsize'] = (14,4)
    plt.plot_date(dailyclose['created'],dailyclose['price'],'go--')
    plt.ylabel(case + ":Fill Price ({})".format(instruments))
    plt.grid(True)
    plt.ylim(max(dailyclose['price'].min()-2,0),dailyclose['price'].max()*1.1)
    plt.title('{} to {}- {} Closing Prices'.format(leadup.strftime("%B %Y"),ending.strftime("%B %Y"),instruments))
    plt.savefig("{}_{}_fig_5.png".format(beginning.strftime("%Y%b"),case))
    plt.show()


    ### OUTPUT
    totaldict = {}
    totaldict['fig_X1'] = exh1[['event_id', 'side', 'account_id', 'date_time', 'order_type','fill_price', 'fill_quantity', 'fees']]
    totaldict['fig_X1B'] = exh1_draft
    totaldict['fig_X2'] = exh2
    totaldict['fig_X3'] = exh3
    totaldict['fig_X4'] = exh4.drop(columns='CANCEL_remaining_quantity')
    
    return(totaldict)