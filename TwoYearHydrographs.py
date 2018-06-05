# Import system modules
import sys
import pyodbc
import datetime as dt
import pandas as pd
import numpy as np
from itertools import chain
from decimal import *
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.font_manager import FontProperties

LastYearsDate= dt.date.today() - dt.timedelta(days=730)
ThisDate=dt.date.today()

currentTime = dt.datetime.now().time()
timeStamp = currentTime.strftime("%m/%d/%Y %H:%M:%S" )

print ThisDate.strftime("%m/%d/%Y")
print LastYearsDate.strftime("%m/%d/%Y")
myFmt = mdates.DateFormatter('%b %Y')

try:
    cnxn = pyodbc.connect('DSN=WREP; UID=pub; PWD=pub')
except Exception as err:
    print err
anyDailyDBKey= 'OB338'
cursor = cnxn.cursor()
sqlDA = "SELECT d.dbkey,D.DATA_TYPE,D.STATION,D.FREQUENCY,D.AGENCY,D.START_DATE,D.END_DATE FROM DM_KEYREF d " + \
    "WHERE  d.DBKey in ('88715','03576','PT638','OB341','W4254','OB342','OB344','P0061','P0062'," + \
    "'P0063','P0064','US179','US177','TS191','TS189','P1982','P2038','PT502','TO048','TO046','WN400'," + \
    "'WN398','OB340','OB339','OB338','P1959','02869','SC005','VM991','WF835','WF829','09610','P2032'," + \
    "'P2030','P2028','RU208','RU206','P0706','P0704','T0960','SM775','SM773','OB383','OB384','NV837'," + \
    "'NV838','NV846','TO052','P2108','P2111','P2112','W4328','VW827','VW823','WH018','WH012','OB389'," + \
    "'SD017','SM771','TA937','TA939','PS977','UO682','UO680','87537','87535','87527','WF841','SA297'," + \
    "'37315','03550','03578','TO069','SC381','TO065','09601','PT092','TP352','VN450','VN448','VN446'," + \
    "'AL719','03487','03537','03530','03545','TA837','09604','03376','TA835','PD409','TA819','O6378'," + \
    "'O6371','O6374','WF809','09599','SC192','SC190','03396','09600','PS973','PS971','PS969','PS986'," + \
    "'PT115','OG450','OG451','UP246','VN324','VM825','VM823','OG452','P6306','P6310','P6314','OB396'," + \
    "'OB395','US356','PT526','VN339','AI849','AI857','AI861','37759','37194','03341','03343','TB036'," + \
    "'PT642','SP555','OH809','OH808','OH807','P0045','P0047','PT646') and d.frequency = 'DA'"

sqlRI = "SELECT d.dbkey,D.DATA_TYPE,D.STATION,D.FREQUENCY,D.AGENCY,D.START_DATE,D.END_DATE FROM DM_KEYREF d " + \
    "WHERE  d.DBKey in ('88715','03576','PT638','OB341','W4254','OB342','OB344','P0061','P0062'," + \
    "'P0063','P0064','US179','US177','TS191','TS189','P1982','P2038','PT502','TO048','TO046','WN400'," + \
    "'WN398','OB340','OB339','OB338','P1959','02869','SC005','VM991','WF835','WF829','09610','P2032'," + \
    "'P2030','P2028','RU208','RU206','P0706','P0704','T0960','SM775','SM773','OB383','OB384','NV837'," + \
    "'NV838','NV846','TO052','P2108','P2111','P2112','W4328','VW827','VW823','WH018','WH012','OB389'," + \
    "'SD017','SM771','TA937','TA939','PS977','UO682','UO680','87537','87535','87527','WF841','SA297'," + \
    "'37315','03550','03578','TO069','SC381','TO065','09601','PT092','TP352','VN450','VN448','VN446'," + \
    "'AL719','03487','03537','03530','03545','TA837','09604','03376','TA835','PD409','TA819','O6378'," + \
    "'O6371','O6374','WF809','09599','SC192','SC190','03396','09600','PS973','PS971','PS969','PS986'," + \
    "'PT115','OG450','OG451','UP246','VN324','VM825','VM823','OG452','P6306','P6310','P6314','OB396'," + \
    "'OB395','US356','PT526','VN339','AI849','AI857','AI861','37759','37194','03341','03343','TB036'," + \
    "'PT642','SP555','OH809','OH808','OH807','P0045','P0047','PT646') and d.frequency = 'RI'"

for frequency in ('DA','RI'):

    if frequency == 'DA':
        cursor.execute(sqlDA)
    else:
        cursor.execute(sqlRI)
    
    dbhydro_return = cursor.fetchall()
    recKnt= len(dbhydro_return)

    for row in dbhydro_return:
        if frequency == 'DA':
            sql= "select daily_date, value from dmdbase.dm_daily_data where DBKEY='" +\
                row.DBKEY.strip(' ')+\
                "' and daily_date > to_date('" +LastYearsDate.strftime("%m/%d/%Y") + "','MM/DD/YYYY')"
        else:
 #           sql= "select random_date as daily_date, value from dmdbase.dm_random_data where DBKEY='" +\
 #               row.DBKEY.strip(' ')+\
 #               "' and random_date > to_date('" +LastYearsDate.strftime("%m/%d/%Y") + "','MM/DD/YYYY')"
            sql= "SELECT TO_DATE (b.daily_date, 'MM/DD/YYYY') daily_date, a.VALUE FROM" +\
                "(   SELECT to_char(random_date,'MM/DD/YYYY') random_date , avg(value) as value " +\
                "       FROM dmdbase.dm_random_data "+ \
                "       WHERE dmdbase.dm_random_data.DBKEY = '" +\
                row.DBKEY.strip(' ')+\
                "' group by to_char(random_date, 'MM/DD/YYYY')) a right join" +\
                "(   SELECT to_char(daily_date,'MM/DD/YYYY') daily_date " + \
                "      FROM dmdbase.dm_daily_data WHERE " + \
                " dmdbase.dm_daily_data.dbkey = 'OB338'  AND dmdbase.dm_daily_data.daily_date > " + \
                "  to_date('" +LastYearsDate.strftime("%m/%d/%Y") + "','MM/DD/YYYY')) b " + \
                " on b.daily_date = a.random_date order by TO_DATE (b.daily_date, 'MM/DD/YYYY') "


#        print sql
 
        station = row.STATION.strip(' ')
        key= row.DBKEY.strip(' ')
        stationDB = station + ' (dbkey[' + key + '])'
        dataType = row.DATA_TYPE.strip(' ')
        dataSource = row.AGENCY.strip(' ')
        
        DBkeyCursor = cnxn.cursor()   
        DBkeyCursor.execute(sql)

        records=DBkeyCursor.fetchall()
        if len(records):
            dbhydDF = pd.DataFrame(records, columns=['dateread', 'value'])
            arr1 = []
            arr2 = []
            for  record in records:
                daily , val = record
                arr1.append(daily)
                arr2.append(val)
    
            fig = plt.figure()
            ax = "ax" + str(111)
            ax = fig.add_subplot(111)
            
            colorvalue = 0  
          
            if colorvalue == 0:
                usecolor = 'blue'
            elif colorvalue == 1:
                usecolor = 'green'
            else:
                usecolor = 'red'
    
            dmax = dbhydDF.dateread.max()
            dmin = dbhydDF.dateread.min()
            ymax = float(dbhydDF.value.max())
            ymin = float(dbhydDF.value.min())
            yrange = ymax-ymin
            ymin = ymin - round((yrange * .05),2)
            ymax = ymax + round((yrange * .05),2)
            yinterval = round((yrange / 8.0),1)

            xs = pd.date_range(dmin, dmax)
            ys = dbhydDF['value']
#            print len(xs), dmin, dmax
#            print len(ys)
# Create X axis with a full year date range
            ax.set_xlim(LastYearsDate, ThisDate)
# Plot the Data
            if frequency == 'DA':
                ax.plot_date(xs,ys,ydate=False,linestyle='-',linewidth=1,color=usecolor,label=stationDB)
            else:
                ax.plot_date(xs,ys,ydate=False,linestyle='-',marker='o',markersize=5,linewidth=1,color=usecolor,label=stationDB)
    
            fontP = FontProperties()
            fontP.set_size('xx-small')
            
            box = ax.get_position()
            ax.legend( bbox_to_anchor=(1,1), prop = fontP, fancybox=True, shadow=False)

            if dataType == 'UNHD':      
                ax.set_ylabel('Uncorrected Head (ft)')
            else:
                ax.set_ylabel('Head (ft)')
                
            ax.xaxis.set_major_formatter(myFmt)
            minorlocator = plt.MultipleLocator(10)
            major_yloc= plt.MultipleLocator(yinterval)
#            plt.yticks(np.arange(ymin, ymax, yinterval))                
            majorlocator = plt.MultipleLocator(30.4)
            
            ax.xaxis.set_major_locator(majorlocator)
            ax.yaxis.set_major_locator(major_yloc)
            ax.text(0.95, 0.01, 'Plots create: '+ timeStamp + \
                    '\nReviewed by the Hydrogeology Unit of the Water Supply Bureau',
                verticalalignment='bottom', horizontalalignment='right',
                transform=ax.transAxes,
                color='green', fontsize=7)
                      
            ax.set_autoscale_on(True)
            
            for ylabel in ax.yaxis.get_ticklabels():
                ylabel.set_fontsize(7)
          
            for label in ax.xaxis.get_ticklabels():
                label.set_rotation(300)
                label.set_fontsize(7)
                
            if frequency == 'DA':
                title= station +'\n' + dataSource +' Floridan Daily Water Level\n' + \
                      LastYearsDate.strftime("%m/%d/%Y") + ' thru ' + ThisDate.strftime("%m/%d/%Y")
            else:
                title= station +'\n' + dataSource +' Floridan Random Interval Water Level\n' + \
                      LastYearsDate.strftime("%m/%d/%Y") + ' thru ' + ThisDate.strftime("%m/%d/%Y")
            
            plt.title(str(title),fontsize = 8)
            
            fname = str(station) 
            fig =  str('{0}.png'.format(fname))
            figout = fig.strip()
            
#            plt.show()

            out = ".\\hydrographs\\" + str(figout)
            plt.savefig(out)
            plt.close()
            print out + "....saved"
        else:
            print "0 records retrieved. Skipped", station
        
