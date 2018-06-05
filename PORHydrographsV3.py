#   Program name:   POR Hydrographs
#   Version:        V3
#   Date:           7/21/2015
#   Creatd by:      Kevin A. Rodberg
#
#   Purpose:    Generates a series of hydrographs from data
#               queried from DBHydro for Floridan Groundwater Wells

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
from matplotlib.lines import Line2D
from argparse import ArgumentParser
import os
os.environ.update([('NLS_LANG', '.UTF8'),('ORA_NCHAR_LITERAL_REPLACE', 'TRUE'),])


def getOptions():
    parser = ArgumentParser(prog='ReadBinary')
    parser.add_argument("-aquifer", 
                        dest="Aquifer",
                        choices=['Floridan','Biscayne','Hawthorne',
                                 'Limestone','Surficial','Sandstone',
                                 'Tamiami', 'Sand'],
                        default='Floridan',
                        help="Process DBKeys by Aquifer.")
    args = parser.parse_args()
    return args

def setDBKeylist(Aquifer):

    if Aquifer == 'Floridan':
        DBkeyList = ('W4254','P2028','VN450','OG452','P0047','88715','03576','PT638','OB341','OB342','OB343','OB344','P0061','P0062',
                     'P0063','P0064','US179','US177','TS191','TS189','P1982','P2038','PT502','TO048','TO046',
                     'WN400','WN398','OB340','OB339','OB338','P1959','02869','SC005','VM991','WF835','WF829',
                     '09610','P2032','P2030','RU208','RU206','P0706','P0704','T0960','SM775','SM773',
                     'OB383','OB384','NV837','NV838','NV846','TO052','P2108','P2111','P2112','W4328','VW827',
                     'VW823','WH018','WH012','OB389','SD017','SM771','TA937','TA939','PS977','UO682','UO680',
                     '87537','87535','87527','WF841','SA297','37315','03550','03578','TO069','SC381','TO065',
                     '09601','PT092','TP352','VN448','VN446','AL719','AL724','03487','03537','03530',
                     '03545','TA837','09604','03376','TA835','PD409','TA819','O6378','O6371','O6374','WF809',
                     '09599','SC192','SC190','03396','09600','PS973','PS971','PS969','PS986','PT115','OG450',
                     'OG451','UP246','VN324','VM825','VM823','P6306','P6310','P6314','OB396','OB395',
                     'US356','PT526','VN339','AI849','AI857','AI861','37759','37194','03341','03343','TB036',
                     'PT642','SP555','OH809','OH808','OH807','P0045','PT646')
    elif Aquifer == 'Biscayne':
        DBkeyList = ('01754','LP651','01081','01031','00971','LP673','37312','05695','LP646','01461','00994',
                     '00973','08162','NV781','NV782','NV783','37437','NV785','NV787','NV788','NV790','NV791',
                     '37324','NV793','NV794','NV795','NV799','NV802','NV805','NV806','NV807','NV808','NV809',
                     'NV810','NV811','NV812','NV814','NV815','01462')
    elif Aquifer == 'Hawthorne':
        DBkeyList = ('AI846','37311','NV388','02460','02450','02593','37400','LP678','01660','07658','08360',
                     '37318','37454','09235','08830','02757','08862','02643','02415','NV848')
    elif Aquifer == 'Limestone':
        DBkeyList = ('37407','37390','07651','07691','02357','02361','08306','02748','02430','02414','02642',
                     '08339','NV847')
    elif Aquifer == 'Surficial':
        DBkeyList = ('03053','LP684','LP686','LP685','02669','37405','37406','G4564','LP683','LP681','NV865',
                     'LP689','37417','37445','37448','37447','37412','NV850','NV852','NV853','37436','37442',
                     '37441','NV854','NV856','NV857','NV858','NV861','NV862','03115','37451','37449','37452',
                     '37450','LP687','LP690','LP688')
    elif Aquifer == 'Sandstone':
        DBkeyList = ('AI845','37377','LP682','02765','02610','02611','37383','02426','02410','02444','LP680',
                     '37314','LP679','07652','07659','07660','07661','09091','08305','NV816','08855','08298',
                     '07695','02698','02442','02574','02427','02411','07697','02639','02375','37439','02714',
                     'NV836','37459','09046')
    elif Aquifer == 'Tamiami':
        DBkeyList = ('37310','06573','VM674','NV383','88680','88678','02405','37384','LP676','63947','2078',
                     '02075','02046','01859','07679','08355','07681','37456','08814','37317','09071','37429',
                     '37457','37331','37320','37333','37322','08325','08327','37325','37438','NV849')
    elif Aquifer == 'Sand':
        DBkeyList = ('01903','02010','02317','01784','02207','06560','37380','37381','NV387','02699','02612',
                     '37385','02358','02428','37313','LP677','02354','01904','02012','02319','01786','02209',
                     '07667','07680','08300','37316','08333','NV775','NV776','37458','37332','37321','37334',
                     '37323','37327','37326','37328','37329','37335','37336','37338','37337','37453','02700',
                     '08299','08328','08792','02429','02412','08318','NV839','37330')
    else:
        DBkeyList = ''
    return DBkeyList

options = getOptions()
option_dict = vars(options)
print options.Aquifer

DBKeyList = setDBKeylist(options.Aquifer)
#print DBKeyList


ThisDate=dt.date.today()
print ThisDate.strftime("%m/%d/%Y ")

currentTime = dt.datetime.now().time()
timeStr = currentTime.strftime("%H:%M:%S" )
timeStamp = ThisDate.strftime("%m/%d/%Y") + " " + timeStr
print timeStamp
myFmt = mdates.DateFormatter('%b %Y')


try:
    print 'Establishing Database connection'
    cnxn = pyodbc.connect('DSN=WREP; UID=pub; PWD=pub')
except Exception as err:
    print err
    

cursor = cnxn.cursor()

sqlQ="SELECT d.dbkey,D.DATA_TYPE,D.STATION,D.FREQUENCY,D.AGENCY,d.START_DATE as sDATE," + \
        "to_char(D.START_DATE,'MM/DD/YYYY') as START_DATE," + \
        "to_char(D.END_DATE,'MM/DD/YYYY') as END_DATE FROM DM_KEYREF d " + \
    "WHERE  d.DBKey in " + str(DBKeyList) + " and d.frequency = ? order by D.STATION"
#print sqlQ
for frequency in ('DA','RI'):
    print 'Selecting date ranges for ' +frequency+' DBkeys'
    
    cursor.execute(sqlQ, frequency)
    dbhydro_return = cursor.fetchall()
    recKnt= len(dbhydro_return)
        
    steps = recKnt/10
    if steps > 1:
        print 'Progress  interval: ' + str(steps) + ' hydrographs created for each 10%'
        print 'Total hydrographs: ' + str(recKnt)
        i=0
        print 'Building Hydrographs'
        print '                                    1'
        print 'Progress for '+frequency+'   1 2 3 4 5 6 7 8 9 0'
        print '                  0 0 0 0 0 0 0 0 0 0 '
        print '                [',

    else:
 #       print 'Progress  interval: ' + str(steps) + ' hydrographs created for each 10%'
        steps = 1
        print 'Total hydrographs being created for '+frequency+': ' + str(recKnt)
        i=0
        print 'Building Hydrographs'       
        print '                [',
    sys.stdout.flush()
    for row in dbhydro_return:
        i = i + 1
        if i%steps == 0: 
            print 'X', 
            sys.stdout.flush()

            
# Frequency codes for Daily and Random require separate queries as the data is pulled from separate tables in DBHydro.
#   DMDBASE.DAILY_DATA and DMDBASE.RANDOM_DATA

# Daily query:

        if frequency == 'DA':
            sdateDT = row.SDATE
#            print sdateDT
            oneStartDate= row.START_DATE.strip(' ')

            sql= "select daily_date, value from dmdbase.dm_daily_data where DBKEY='" +\
                row.DBKEY.strip(' ')+\
                "' and daily_date > to_date('" +oneStartDate + "','MM/DD/YYYY')"
            
# Hydrographs for Daily data need an additional query for each set of qualifier codes.
#   Currently E for Estimated in one query, all other codes <> M in another.
           
#            sqlwithCodes = "SELECT l.daily_date, r.VALUE, r.code FROM dm_daily_data l " +\
#                           "LEFT JOIN (   SELECT * FROM dm_daily_data c WHERE c.code IS NOT NULL" +\
#                           " and c.code <> '!' and c.code <> 'S' and c.code <> 'E' AND c.DBKEY = '" +\

            sqlwithCodes = "SELECT l.daily_date, r.VALUE, r.code FROM dm_daily_data l " +\
                           "LEFT JOIN (   SELECT * FROM dm_daily_data c WHERE c.code IS NOT NULL" +\
                           " and c.code in('C','N','V','P','<','>','!') AND c.DBKEY = '" +\
                           row.DBKEY.strip(' ')+\
                           "' AND c.daily_date > TO_DATE ('" +oneStartDate + "','MM/DD/YYYY')) r ON l.DAILY_DATE = " +\
                           "r.DAILY_DATE WHERE l.DBKEY = '" +row.DBKEY.strip(' ')+"' AND l.daily_date > " +\
                           "TO_DATE ('" +oneStartDate + "','MM/DD/YYYY') order by l.daily_date"
            
            sqlQualifiers= "SELECT LISTAGG (code, ',') WITHIN GROUP (ORDER BY code) qualifiers " +\
                           "FROM (SELECT DISTINCT code FROM dm_daily_data WHERE code in('C','N','V','P','<','>','!') " +\
                           "AND VALUE IS NOT NULL AND dbkey = '" +\
                           row.DBKEY.strip(' ') + "')"
            
            sqlAnnotations = "SELECT LISTAGG (code, ',') WITHIN GROUP (ORDER BY code) qualifiers " +\
                     "FROM (SELECT distinct A.code FROM DCVP_DBA.DCVP_ANNOTATIONS a, " +\
                           "DMDBASE.KEYWORD_TAB_VIEW b, dm_daily_data c " + \
                           "WHERE a.code IN ('C','V') AND a.station_id = b.station_id   AND b.dbkey = '" +\
                           row.DBKEY.strip(' ')+\
                           "' AND C.DBKEY = b.dbkey AND TO_CHAR (c.daily_date, 'MM/DD/YYYY')=" +\
                           "TO_CHAR(TO_DATE(A.Start_DATE_TIME,'yyyymmdd:hh24mi'),'MM/DD/YYYY'))"
            
#            print sqlAnnotations
            
            
            sqlDCVPcodes = "SELECT l.daily_date, r.VALUE, r.code FROM dm_daily_data l " +\
                           "LEFT JOIN ( SELECT distinct c.daily_date, c.VALUE, A.code FROM DCVP_DBA.DCVP_ANNOTATIONS a, " +\
                           "DMDBASE.KEYWORD_TAB_VIEW b, dm_daily_data c " + \
                           "WHERE a.code IN ('C') AND a.station_id = b.station_id   AND b.dbkey = '" + row.DBKEY.strip(' ')+\
                           "' AND C.DBKEY = b.dbkey AND TO_CHAR (c.daily_date, 'MM/DD/YYYY')=" +\
                           "TO_CHAR(TO_DATE(A.Start_DATE_TIME,'yyyymmdd:hh24mi'),'MM/DD/YYYY')" + \
                           "ORDER BY   A.Start_DATE_TIME) r ON l.DAILY_DATE = " +\
                           "r.daily_date WHERE l.DBKEY = '" +row.DBKEY.strip(' ')+"' AND l.daily_date > " +\
                           "TO_DATE ('" +oneStartDate + "','MM/DD/YYYY') order by l.daily_date"
 #           print sqlDCVPcodes
            sqlDCVPcodesV = "SELECT l.daily_date, r.VALUE, r.code FROM dm_daily_data l " +\
                           "LEFT JOIN ( SELECT distinct c.daily_date, c.VALUE, A.code FROM DCVP_DBA.DCVP_ANNOTATIONS a, " +\
                           "DMDBASE.KEYWORD_TAB_VIEW b, dm_daily_data c " + \
                           "WHERE a.code IN ('V') AND a.station_id = b.station_id   AND b.dbkey = '" + row.DBKEY.strip(' ')+\
                           "' AND C.DBKEY = b.dbkey AND TO_CHAR (c.daily_date, 'MM/DD/YYYY')=" +\
                           "TO_CHAR(TO_DATE(A.Start_DATE_TIME,'yyyymmdd:hh24mi'),'MM/DD/YYYY')" + \
                           "ORDER BY   A.Start_DATE_TIME) r ON l.DAILY_DATE = " +\
                           "r.daily_date WHERE l.DBKEY = '" +row.DBKEY.strip(' ')+"' AND l.daily_date > " +\
                           "TO_DATE ('" +oneStartDate + "','MM/DD/YYYY') order by l.daily_date"            

            sqlwithEs = "SELECT l.daily_date, r.VALUE, r.code FROM dm_daily_data l " +\
                           "LEFT JOIN (   SELECT * FROM dm_daily_data c WHERE c.code = 'E' AND c.DBKEY = '" +\
                           row.DBKEY.strip(' ')+\
                           "' AND c.daily_date > TO_DATE ('" +oneStartDate + "','MM/DD/YYYY')) r ON l.DAILY_DATE = " +\
                           "r.DAILY_DATE WHERE l.DBKEY = '" +row.DBKEY.strip(' ')+"' AND l.daily_date > " +\
                           "TO_DATE ('" +oneStartDate + "','MM/DD/YYYY') order by l.daily_date"
        else:

# Random Interval query:
#   anyDailyDBKey= '01081'    <--- This DBKey was picked to provide daily dates to random data.
            sdateDT = row.SDATE
            oneStartDate = '01/01/1940'
            sql= "SELECT TO_DATE (b.daily_date, 'MM/DD/YYYY') daily_date, a.VALUE FROM" +\
                "(   SELECT to_char(random_date,'MM/DD/YYYY') random_date , avg(value) as value " +\
                "       FROM dmdbase.dm_random_data "+ \
                "       WHERE dmdbase.dm_random_data.DBKEY = '" + row.DBKEY.strip(' ') +\
                "' group by to_char(random_date, 'MM/DD/YYYY')) a right join" +\
                "(   SELECT to_char(daily_date,'MM/DD/YYYY') daily_date " + \
                "      FROM dmdbase.dm_daily_data WHERE " + \
                " dmdbase.dm_daily_data.dbkey = '01081'  AND dmdbase.dm_daily_data.daily_date > " + \
                "  to_date('" +oneStartDate + "','MM/DD/YYYY')) b " + \
                " on b.daily_date = a.random_date order by TO_DATE (b.daily_date, 'MM/DD/YYYY') "
#        print sql
        station = row.STATION.strip(' ')
#        print station
        key= row.DBKEY.strip(' ')
        stationDB = station + ' (dbkey[' + key + '])'
        dataType = row.DATA_TYPE.strip(' ')
        dataSource = row.AGENCY.strip(' ')
        
        DBkeyCursor = cnxn.cursor()
        DBkeyCursor.execute(sql)
        records=DBkeyCursor.fetchall()

# If query returned 0 records skip the chart processing.

        if len(records):
            
# Process Random Interval and Daily data in a similar fashion
#   when retrieving data values and defining the x and y axis

            dbhydDF = pd.DataFrame(records, columns=['dateread','value'])
            dmax = dbhydDF.dateread.max()
            dmin = dbhydDF.dateread.min()

#            print "dmin", dmin, dmax
            ymax = float(dbhydDF.value.max())
            ymin = float(dbhydDF.value.min())
            yrange = ymax-ymin
            ymin = ymin - round((yrange * .05),2)
            ymax = ymax + round((yrange * .05),2)
            yinterval = round((yrange / 8.0),1)


            ys = dbhydDF['value']
#            print len(ys)
            
# Hydrographs for Daily data need additional query processing for each set of qualifier codes.

            if frequency == 'DA':
                dmin = sdateDT                
                xs = pd.date_range(dmin, dmax - dt.timedelta(days=1))                
                AnnoCodesCursor =cnxn.cursor()
                AnnoCodesCursor.execute(sqlDCVPcodes)
                Acodes= AnnoCodesCursor.fetchall()

                AnnoVCursor =cnxn.cursor()
                AnnoVCursor.execute(sqlDCVPcodesV)
                Vcodes= AnnoVCursor.fetchall()
                
                CodesCursor = cnxn.cursor()
                CodesCursor.execute(sqlwithCodes)
                Qcodes=CodesCursor.fetchall()
                codesDF = pd.DataFrame(Qcodes, columns=['dateread','value','code'])
                
                
                qualiferCursor = cnxn.cursor()
                qualiferCursor.execute(sqlQualifiers)
                qualiferRecord=qualiferCursor.fetchall()

                annotationCursor = cnxn.cursor()
                annotationCursor.execute(sqlAnnotations)
                annotationRecord=annotationCursor.fetchall()
                                
                
                cols =[ t[0] for t in qualiferCursor.description ]
                for col, value in zip(cols,qualiferRecord):
                    qualiferStr =str(value).strip('( )')[:-1]
                    
                cols =[ t[0] for t in annotationCursor.description ]
                for col, value in zip(cols,annotationRecord):
                    annotationStr =str(value).strip('( )')[:-1]                    

                ECodesCursor = cnxn.cursor()
                ECodesCursor.execute(sqlwithEs)
                Ecodes=ECodesCursor.fetchall()
                EcodesDF = pd.DataFrame(Ecodes, columns=['dateread','value','code'])
                
                dmax2 = codesDF.dateread.max()
                dmax3 = EcodesDF.dateread.max()

                
                xs2 = pd.date_range(dmin, dmax2 - dt.timedelta(days=1))
                xs3 = pd.date_range(dmin, dmax3 - dt.timedelta(days=1))

                
                ycodes = codesDF['value']
                ecodes = EcodesDF['value']
                if len(Acodes) > 0:
                    annoDF = pd.DataFrame(Acodes, columns=['dateread','value','code'])
                    dmax4 = annoDF.dateread.max()
                    xs4 = pd.date_range(dmin, dmax4 - dt.timedelta(days=1))
                    CannoCodes = annoDF['value']
                if len(Vcodes) > 0:
                    annoVDF = pd.DataFrame(Vcodes, columns=['dateread','value','code'])
                    dmax5 = annoVDF.dateread.max()
                    xs5 = pd.date_range(dmin, dmax5 - dt.timedelta(days=1))
                    VannoCodes = annoVDF['value']
                
            else:
                dmin = dbhydDF.dateread.min()                
                xs = pd.date_range(dmin, dmax )              
                ts2= np.array(ys.values).astype(np.double)
                ts2mask = np.isfinite(ts2)
#                print len(ts2mask), len(ts2), len(xs), len(ys)

# Define general chart characteristics, titles and descriptive text:
                
#            fig = plt.figure()
#            chart = fig.add_subplot(111)
            fig, chart = plt.subplots()

    
            fontP = FontProperties()
            fontP.set_size('xx-small')
                     
            chart.set_autoscale_on(True)
            minorlocator = plt.MultipleLocator(10)
            
            chart.text(0.99, 0.01, 'Plots created: '+ timeStamp + \
                    '\nReviewed by the Hydrogeology Unit of the Water Supply Bureau',
                    verticalalignment='bottom', horizontalalignment='right',
                    transform=chart.transAxes,
                    color='green', fontsize=7)

            if frequency == 'DA':
                title= station +'\n' + dataSource +' ' + options.Aquifer +' Daily Water Level\n' + \
                       sdateDT.strftime("%m/%d/%Y") + ' thru ' + ThisDate.strftime("%m/%d/%Y")
            else:
                title= station +'\n' + dataSource +' '+ options.Aquifer + ' Random Interval Water Level\n' + \
                       sdateDT.strftime("%m/%d/%Y") + ' thru ' + ThisDate.strftime("%m/%d/%Y")
                
            plt.title(str(title),fontsize = 8)
            
# Create X axis with full date range

            chart.set_xlim(sdateDT, ThisDate)
            
# Plot the Data:
            markers =[]
            for m in Line2D.markers:
                try:
                    if len(m) == 1 and m != ' ':
                        markers.append(m)
                except TypeError:
                    pass
            style6 = markers[6]
            style9 = markers[9]
#            print style6
#            print markers
            if frequency == 'DA':
#                print "%s and %s" % (len(xs), len(ys))
                chart.plot_date(xs,ys,ydate=False,linestyle='-',
                                marker='.',markersize=1,linewidth=1,color='blue',label=stationDB)
                
                Qstr='Qualifier List:[' + qualiferStr + ']'
#                Astr='Annotation List:[' + annotationStr + ']'

                chart.plot_date(xs3,ecodes,ydate=False,linestyle='-',
                                marker='.',markersize=1,linewidth=1,color='green',label='Estimated')
                
                chart.plot_date(xs2,ycodes,ydate=False,linestyle='b-',
                                marker='o',markersize=4,linewidth=1,color='red',label=Qstr)                
                if len(VannoCodes) <> len(xs5):
                    print stationDB
                    stop()
                if annotationStr <> 'None':
 #                   print str(len(annotationStr)) + annotationStr
                    if 'C' in annotationStr: 
                        chart.plot_date(xs4,CannoCodes,ydate=False,linestyle='None',
                                        marker=style9,markersize=6,linewidth=1,color='magenta',label='Calibration')
                    if 'V' in annotationStr:
                        chart.plot_date(xs5,VannoCodes,ydate=False,linestyle='None',
                                        marker=style6,markersize=4,linewidth=1,color='yellow',label='Verification')                
            else:
                chart.plot(xs[ts2mask],ys[ts2mask],linestyle='-',
                                marker='o',markersize=2,linewidth=1,color='red',label=stationDB)

# Set xaxis specifics:

            if len(xs) < 1095:
                majorlocator = plt.MultipleLocator(30.4)
            else:
                majorlocator = plt.MultipleLocator(365)
            
            chart.xaxis.set_major_formatter(myFmt)
            chart.xaxis.set_major_locator(majorlocator)

            for label in chart.xaxis.get_ticklabels():
                label.set_rotation(300)
                label.set_fontsize(7)

# Set yaxis specifics:

            major_yloc= plt.MultipleLocator(yinterval)
            chart.yaxis.set_major_locator(major_yloc)

            if dataType == 'UNHD':
                chart.set_ylabel('Uncorrected Head (ft)')
            else:
                chart.set_ylabel('Head (ft)')
            
            for ylabel in chart.yaxis.get_ticklabels():
                ylabel.set_fontsize(7)

            
            chart.legend(loc=3, bbox_to_anchor=(.01,.01), prop = fontP, fancybox=True, shadow=False)
 
# Create figure filenames and output chart .png's to the hydrograph directory.
            stationKey = station + '_' + key 
            fname = str(stationKey) 
            fig =  str('{0}.png'.format(fname))
            figout = fig.strip()
#            plt.show()
            out = "\\\\ad.sfwmd.gov\\dfsroot\\data\\wsd\\Hydrogeology\\Database\\Time-Series_QC\\hydrographs\\" \
                  + options.Aquifer + "\\" + str(figout)
#            out = ".\\hydrographs\\" + str(figout)
            plt.savefig(out)
            plt.close()
#            print out + "....saved"
    
    print ' ]  Done!'
