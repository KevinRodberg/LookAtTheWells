#
#   Program name:   Hydrographs
#   Version:        V4
#   Date:           07/21/2015
#   MultiProcessor  04/25/2017
#   GUI version     06/28/2017
#   2 year range    01/07/2020
#   Creatd by:      Kevin A. Rodberg
#
#   Purpose:    Generates a series of hydrographs from data
#               queried from DBHydro for Groundwater Wells by aquifer

# Import system modules
import sys
import pyodbc
import datetime as dt
import time
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.font_manager import FontProperties
from matplotlib.lines import Line2D
from argparse import ArgumentParser
import multiprocessing
import os
from PyQt5 import QtWidgets as QtWidg
from PyQt5 import uic

os.environ.update([('NLS_LANG', '.UTF8'),('ORA_NCHAR_LITERAL_REPLACE', 'TRUE'),])
basepath = "//ad.sfwmd.gov/dfsroot/data/wsd/Hydrogeology/Database/TimeSeries_QC/hydrographs/TwoYrs/"
print(basepath)
allAquifers = ['Floridan','Biscayne','Hawthorn','Limestone',
               'Surficial','Sandstone','Tamiami', 'Sand']

global debugStr
#  Setup Graphic User Interface
#   Using Qt Designer xml
qtCreatorFile = "//ad.sfwmd.gov/dfsroot/data/wsd/SUP/devel/source/Python/LookAtTheWells/RodbergReportOptions.ui"

Ui_MainWindow, QtBaseClass = uic.loadUiType(qtCreatorFile)

#  Aquifer Choices are available
#   with Radio button determine of "All Aquifers" or "Choose Aquifers"
#     Checkbox options provide choices of Aquifers

class RadioAquiferOptions(QtWidg.QMainWindow,Ui_MainWindow):

#  Initialize Window
#   and define button functionality

    def __init__(self,parent=None):
        global Aquiferbtns
        super(RadioAquiferOptions,self).__init__(parent)
        self.ui=Ui_MainWindow()
        self.ui.setupUi(self)
        Aquiferbtns = [self.ui.cbFloridan,self.ui.cbBiscayne,self.ui.cbHawthorn,
                       self.ui.cbLimestone,self.ui.cbSurficial,self.ui.cbSandstone,
                       self.ui.cbTamiami,self.ui.cbSand]
        self.ui.rbAllAquifers.toggled.connect(lambda:self.btnstate(self.ui.rbAllAquifers))
        self.ui.hzRBs.addWidget(self.ui.rbAllAquifers)
        self.ui.rbOneAquifer.toggled.connect(lambda:self.btnstate(self.ui.rbOneAquifer))
        self.ui.hzRBs.addWidget(self.ui.rbOneAquifer)
        for aquifer in Aquiferbtns:
            self.ui.vtChkBxs.addWidget(aquifer)
        self.ui.BeginExecButton.clicked.connect(lambda:self.startProcessing())
        txt = "Select Aquifers"
        self.ui.PoolResponse.setText(txt)
    def btnstate(self,b):
        if b.text() == "All Aquifers":
            if b.isChecked() == True:
                for aquifer in Aquiferbtns:
                    aquifer.setChecked(True)
            else:
                for aquifer in Aquiferbtns:
                    aquifer.setChecked(False)
        if b.text() == "Choose Aquifers":
            if b.isChecked() == True:
                for aquifer in Aquiferbtns:
                    aquifer.setChecked(False)
            else:
                pass
#
#  Pressing the "Begin Processing" Button
#   starts figure creation
#
    def startProcessing(self):
        start = time.time()
        atLeastOne = 0
        aquiferList = []
        for cBxs in Aquiferbtns:
            print (cBxs.text(), cBxs.isChecked())
            if cBxs.isChecked():
                aquiferList.append(cBxs.text())
                atLeastOne = atLeastOne +1
        if atLeastOne < 1:
            self.ui.cbSand.setChecked(True)
            aquiferList.append(self.ui.cbSand.text())
        for Aquifer2Process in aquiferList:
            DBKeyList = setDBKeylist(Aquifer2Process)
            AllrPool.append(makeChartsByAquifer(Aquifer2Process, DBKeyList))
        for rPool in reversed(AllrPool):
            for rp in rPool:
                rp.wait()
        end = time.time()
        txt = "Figures created for "+str(len(aquiferList))+ \
              " Aquifers [Elapsed time: " +str(end-start)+" seconds]"
        self.ui.PoolResponse.setText(txt)
#
#  Command line options for non GUI operation
#
def getOptions():
    parser = ArgumentParser(prog='RodbergReport')
    parser.add_argument("-aquifer",
                        dest="Aquifer",
                        choices=allAquifers,
                        default='Floridan',
                        help="Process DBKeys for 1 Aquifer.")
    parser.add_argument("-all",
                        action="store_true",
                        dest="bulk", default = None,
                        help="Process All DBKeyLists by Aquifer.")
    args = parser.parse_args()
    return args

#  Define Queries used to get data from DBHydro
#
def setSQLstring(qryName,keys='',rowData=None):
    try:
        if rowData:
            print ("exiting")
    except:
        oneStartDate= rowData['START_DATE']


#  Query DM_KEYREF Info for DAILY DATA DBKEYS
    if qryName == 'DBKeyDatesDA':
        oneStartDate= '01/01/2019'

        SQLstr = "SELECT d.dbkey,D.DATA_TYPE,D.STATION,D.FREQUENCY,D.AGENCY," + \
              " '"+ oneStartDate +"' as sDATE,  '"+ oneStartDate +"' as START_DATE," + \
              " to_char(D.END_DATE,'MM/DD/YYYY') as END_DATE FROM DM_KEYREF d " + \
              "WHERE d.DBKey in " + str(keys) + " AND d.frequency = 'DA' ORDER BY D.STATION"
#        SQLstr = "SELECT d.dbkey,D.DATA_TYPE,D.STATION,D.FREQUENCY,D.AGENCY," + \
#              " d.START_DATE as sDATE,to_char(D.START_DATE,'MM/DD/YYYY') as START_DATE," + \
#              " to_char(D.END_DATE,'MM/DD/YYYY') as END_DATE FROM DM_KEYREF d " + \
#              "WHERE d.DBKey in " + str(keys) + " AND d.frequency = 'DA' ORDER BY D.STATION"

#  Query DM_KEYREF Info for RANDOM INTERVAL DATA DBKEYS
    elif qryName == 'DBKeyDatesRI':
        SQLstr = "SELECT d.dbkey,D.DATA_TYPE,D.STATION,D.FREQUENCY,D.AGENCY," + \
              " d.START_DATE as sDATE,to_char(D.START_DATE,'MM/DD/YYYY') as START_DATE," + \
              " to_char(D.END_DATE,'MM/DD/YYYY') as END_DATE FROM DM_KEYREF d " + \
              "WHERE d.DBKey in " + str(keys) + " AND d.frequency = 'RI' ORDER BY D.STATION"

#  Query DM_DAILY_DATA water levels by DBKEY
    elif qryName =='DailyData':
        oneStartDate= '01/01/2019'

        SQLstr= "SELECT daily_date, value FROM dmdbase.dm_daily_data WHERE DBKEY='" + \
                rowData.DBKEY.strip(' ')+ \
                "' AND daily_date >= TO_DATE('" +oneStartDate + "','MM/DD/YYYY')" + \
                "  ORDER BY daily_date "

    elif qryName == 'DailyCodes':

#  Query DM_DAILY_DATA water levels just for records with "Codes"
        SQLstr= "SELECT l.daily_date, r.VALUE, r.code FROM dm_daily_data l " + \
                "       LEFT JOIN (SELECT * FROM dm_daily_data c WHERE c.code IS NOT NULL" + \
                "                      AND c.code IN ('C','V','P','<','>','!') " + \
                "                      AND c.DBKEY = '" + rowData.DBKEY.strip(' ')+ "'" +\
                "                      AND c.daily_date > TO_DATE ('" +oneStartDate + \
                "                                                  ','MM/DD/YYYY')) r " + \
                "         ON l.DAILY_DATE = r.DAILY_DATE " + \
                "      WHERE l.DBKEY = '" +rowData.DBKEY.strip(' ') + "'" + \
                "        AND l.daily_date >= TO_DATE ('" +oneStartDate + "','MM/DD/YYYY')" +\
                "   ORDER BY l.daily_date"

#  Query DM_DAILY_DATA and produce list of codes found for a DBKEY
    elif qryName == 'DailyQualifiers':
        SQLstr= "SELECT LISTAGG (code, ',') WITHIN GROUP (ORDER BY code) qualifiers " + \
                "  FROM (SELECT DISTINCT code FROM dm_daily_data  " + \
                " WHERE code in('C','V','P','<','>','!') " + \
                "   AND VALUE IS NOT NULL AND dbkey = '" + \
                rowData.DBKEY.strip(' ') + "')"

# Query DCVP_ANNOTATIONS and produce List of Codes found for a DBKEY
    elif qryName == 'DailyAnnotations':
        SQLstr= "SELECT LISTAGG (code, ',') WITHIN GROUP (ORDER BY code) qualifiers " + \
                "  FROM (SELECT distinct a.code FROM DCVP_DBA.DCVP_ANNOTATIONS a, " +\
                "       DMDBASE.KEYWORD_TAB_VIEW b, dm_daily_data c " + \
                " WHERE a.code IN ('C','V') AND a.station_id = b.station_id " + \
                "   AND b.dbkey = '" + rowData.DBKEY.strip(' ')+ "'" +\
                "   AND C.DBKEY = b.dbkey " + \
                "   AND TO_CHAR(c.daily_date, 'MM/DD/YYYY')=" + \
                "       TO_CHAR(TO_DATE(a.Start_DATE_TIME,'yyyymmdd:hh24mi'),'MM/DD/YYYY'))"


# Query DM_DAILY_DATA water levels just for records with DCVP annotations
    elif qryName == 'DCVPcodes':
        SQLstr= "SELECT l.daily_date, r.VALUE, r.code FROM dm_daily_data l " + \
                "    LEFT JOIN (SELECT distinct c.daily_date, c.VALUE, A.code " + \
                "         FROM DCVP_DBA.DCVP_ANNOTATIONS a, " + \
                "              DMDBASE.KEYWORD_TAB_VIEW b, dm_daily_data c " +  \
                "        WHERE a.code IN ('C') AND a.station_id = b.station_id  "+ \
                "          AND b.dbkey = '" + rowData.DBKEY.strip(' ')+ \
                "'         AND C.DBKEY = b.dbkey "+ \
                "          AND TO_CHAR (c.daily_date, 'MM/DD/YYYY')=" + \
                "   TO_CHAR(TO_DATE(A.Start_DATE_TIME,'yyyymmdd:hh24mi'),'MM/DD/YYYY')" + \
                "        ORDER BY   A.Start_DATE_TIME) r "+ \
                "           ON l.DAILY_DATE = r.daily_date "+ \
                "    WHERE l.DBKEY = '" +rowData.DBKEY.strip(' ')+ "'" \
                "      AND l.daily_date >= " + \
                "               TO_DATE ('" +oneStartDate + "','MM/DD/YYYY') " + \
                "    ORDER BY l.daily_date"

#
    elif qryName == 'DCVPcodeV':
        SQLstr= "SELECT l.daily_date, r.VALUE, r.code FROM dm_daily_data l " + \
                "       LEFT JOIN (SELECT distinct c.daily_date, c.VALUE, A.code " +\
                "                    FROM DCVP_DBA.DCVP_ANNOTATIONS a, " +\
                "                         DMDBASE.KEYWORD_TAB_VIEW b, dm_daily_data c " + \
                "                   WHERE a.code IN ('V') AND a.station_id = b.station_id " +\
                "                     AND b.dbkey = '" + rowData.DBKEY.strip(' ')+\
                "'          AND C.DBKEY = b.dbkey AND TO_CHAR(c.daily_date, 'MM/DD/YYYY')=" +\
                "     TO_CHAR(TO_DATE(A.Start_DATE_TIME,'yyyymmdd:hh24mi'),'MM/DD/YYYY')" + \
                "                   ORDER BY A.Start_DATE_TIME) r "+\
                "               ON l.DAILY_DATE = r.daily_date " +\
                "            WHERE l.DBKEY = '" +rowData.DBKEY.strip(' ')+"' " +\
                "              AND l.daily_date >= TO_DATE ('" +oneStartDate + "','MM/DD/YYYY') "+\
                "            ORDER BY l.daily_date"
    elif qryName == 'DailyEcodes':
        SQLstr= "SELECT l.daily_date, r.VALUE, r.code FROM dm_daily_data l " +\
                "       LEFT JOIN (SELECT * FROM dm_daily_data c WHERE c.code = 'E' "+\
                "                     AND c.DBKEY = '" + rowData.DBKEY.strip(' ') + \
                "'       AND c.daily_date > TO_DATE ('" +oneStartDate + "','MM/DD/YYYY')) r "+\
                "              ON l.DAILY_DATE = r.DAILY_DATE "+\
                "           WHERE l.DBKEY = '" +rowData.DBKEY.strip(' ')+"' "+\
                "             AND l.daily_date >= TO_DATE ('" +oneStartDate + "','MM/DD/YYYY') "+\
                "           ORDER BY l.daily_date"
    elif qryName == 'RandomIntervalData':
        oneStartDate = '01/01/1940'
        SQLstr= "SELECT TO_DATE(b.daily_date, 'MM/DD/YYYY') daily_date, a.VALUE " +\
                "   FROM(SELECT to_char(random_date,'MM/DD/YYYY') random_date , " +\
                "          avg(value) as value " +\
                "          FROM dmdbase.dm_random_data "+ \
                "         WHERE dmdbase.dm_random_data.DBKEY = '" + rowData.DBKEY.strip(' ') + \
                "'        GROUP BY to_char(random_date, 'MM/DD/YYYY')) a " +\
                "         RIGHT JOIN(SELECT to_char(daily_date,'MM/DD/YYYY') daily_date " + \
                "                      FROM dmdbase.dm_daily_data " + \
                "                     WHERE dmdbase.dm_daily_data.dbkey = '01081'  " + \
                "                       AND dmdbase.dm_daily_data.daily_date >= " + \
                "                           TO_DATE('" +oneStartDate + "','MM/DD/YYYY')) b " + \
                "   ON b.daily_date = a.random_date " + \
                "   ORDER BY TO_DATE (b.daily_date, 'MM/DD/YYYY') "
    else:
        exit()
    return SQLstr

def setDBKeylist(Aquifer):
    if Aquifer == 'Floridan':
        DBkeyList = ('88715','03576','PT638','OB341','W4254','OB342','OB343','OB344','P0061',
                     'P0062','P0063','P0064','US179','US177','TS191','TS189','P1982','P2038',
                     'PT502','TO048','TO046','WN400','WN398','OB340','OB339','OB338','P1959',
                     '02869','SC005','VM991','WF835','WF829','09610','P2032','P2030','P2028',
                     'RU208','RU206','P0706','P0704','T0960','SM775','SM773','OB383','OB384',
                     'NV837','NV838','NV846','TO052','P2108','P2111','P2112','W4328','VW827',
                     'VW823','WH018','WH012','OB389','SD017','SM771','TA937','TA939','PS977',
                     'UO682','UO680','87537','87535','87527','WF841','SA297','37315','03550',
                     '03578','TO069','SC381','TO065','09601','PT092','TP352','VN450','VN448',
                     'VN446','AL719','AL724','03487','03537','03530','03545','TA837','09604',
                     '03376','TA835','PD409','TA819','O6378','O6371','O6374','WF809','09599',
                     'SC192','SC190','03396','09600','PS973','PS971','PS969','PS986','PT115',
                     'OG450','OG451','UP246','VN324','VM825','VM823','OG452','P6306','P6310',
                     'P6314','OB396','OB395','US356','PT526','VN339','AI849','AI857','AI861',
                     '37759','37194','03341','03343','TB036','PT642','SP555','OH809','OH808',
                     'OH807','P0045','P0047','PT646')
    elif Aquifer == 'Biscayne':
        DBkeyList = ('01754','LP651','01081','01031','00971','LP673','37312','05695','LP646',
                     '01461','00994','00973','08162','NV781','NV782','NV783','37437','NV785',
                     'NV787','NV788','NV790','NV791','37324','NV793','NV794','NV795','NV799',
                     'NV802','NV805','NV806','NV807','NV808','NV809','NV810','NV811','NV812',
                     'NV814','NV815','01462')
    elif Aquifer == 'Hawthorn':
        DBkeyList = ('AI846','37311','NV388','02460','02450','02593','37400','LP678','01660',
                     '07658','08360','37318','37454','09235','08830','02757','08862','02643',
                     '02415','NV848')
    elif Aquifer == 'Limestone':
        DBkeyList = ('37407','37390','07651','07691','02357','02361','08306','02748','02430',
                     '02414','02642','08339','NV847')
    elif Aquifer == 'Surficial':
        DBkeyList = ('03053','LP684','LP686','LP685','02669','37405','37406','G4564','LP683',
                     'LP681','NV865','LP689','37417','37445','37448','37447','37412','NV850',
                     'NV852','NV853','37436','37442','37441','NV854','NV856','NV857','NV858',
                     'NV861','NV862','03115','37451','37449','37452','37450','LP687','LP690',
                     'LP688')
    elif Aquifer == 'Sandstone':
        DBkeyList = ('AI845','37377','LP682','02765','02610','02611','37383','02426','02410',
                     '02444','LP680','37314','LP679','07652','07659','07660','07661','09091',
                     '08305','NV816','08855','08298','07695','02698','02442','02574','02427',
                     '02411','07697','02639','02375','37439','02714','NV836','37459','09046')
    elif Aquifer == 'Tamiami':
        DBkeyList = ('37310','06573','VM674','NV383','88680','88678','02405','37384','LP676',
                     '63947','2078', '02075','02046','01859','07679','08355','07681','37456',
                     '08814','37317','09071','37429','37457','37331','37320','37333','37322',
                     '08325','08327','37325','37438','NV849')
    elif Aquifer == 'Sand':
        DBkeyList = ('01903','02010','02317','01784','02207','06560','37380','37381','NV387',
                     '02699','02612','37385','02358','02428','37313','LP677','02354','01904',
                     '02012','02319','01786','02209','07667','07680','08300','37316','08333',
                     'NV775','NV776','37458','37332','37321','37334','37323','37327','37326',
                     '37328','37329','37335','37336','37338','37337','37453','02700','08299',
                     '08328','08792','02429','02412','08318','NV839','37330')
    else:
        DBkeyList = ''
    return DBkeyList

def fetchSQL(sql, param):
    cnxn = pyodbc.connect('DSN=WREP; UID=pub; PWD=pub')
    try:
        recordSet = pd.read_sql(sql,cnxn)
        return recordSet
    except:
        print ("pandas version = ", pd.__version__)
        print ("This code developed to work with version 0.20.1")
        print ("---  Contact Kevin Rodberg for version compatible with ArcGIS Python Win")
        exit(1)

def makefigures(Aquifer2Process,frequency,row):

# Define chart title and figure name variables from each row
    station = row['STATION']
    key =row['DBKEY']
    dataType = row['DATA_TYPE']
    dataSource = row['AGENCY']
    stationDB = station + ' (dbkey[' + key + '])'

# Frequency codes for Daily and Random require separate queries as the data is pulled
# from separate tables in DBHydro:
#   DMDBASE.DAILY_DATA and DMDBASE.RANDOM_DATA

# Daily query:
    if frequency == 'DA':
        sql = setSQLstring('DailyData',rowData=row)
        sdateDT = row['SDATE']
        sdateDT=dt.datetime(2019,1,1)

#   Hydrographs for Daily data need an additional query for each set of qualifier codes.
#   Currently E for Estimated in one query, all other codes <> M in another.

        sqlwithCodes = setSQLstring('DailyCodes',rowData=row)
        sqlQualifiers= setSQLstring('DailyQualifiers',rowData=row)
        sqlAnnotations = setSQLstring('DailyAnnotations',rowData=row)
        sqlDCVPcodes =  setSQLstring('DCVPcodes',rowData=row)
        sqlDCVPcodesV =  setSQLstring('DCVPcodeV',rowData=row)
        sqlwithEs =  setSQLstring('DailyEcodes',rowData=row)

    else:

# Random Interval query:
        sql = setSQLstring('RandomIntervalData',rowData=row)
        sdateDT = row['SDATE']
#   Fetch records for either Daily or Random Interval with sql String
    dbhydDF=fetchSQL( sql, 'none')
#    print (sql)
# If query returned 0 records skip the chart processing.
    if len(dbhydDF):
# Process Random Interval and Daily data in a similar fashion
#   when retrieving data values and defining the x and y axis
        dbhydDF.columns=['dateread','value']
        dmax = dbhydDF.dateread.max()
        dminRd = dbhydDF.dateread.min()
        ymax = float(dbhydDF.value.max())
        ymin = float(dbhydDF.value.min())
        yrange = ymax-ymin
        ymin = ymin - round((yrange * .05),2)
        ymax = ymax + round((yrange * .05),2)
        yinterval = round((yrange / 8.0),1)
        ys = dbhydDF['value']
# Hydrographs for Daily data need additional query processing for each set of qualifier codes.
        if frequency == 'DA':
            sdateDT=dt.datetime(2019,1,1)
            dmin = sdateDT
            xs = pd.date_range(start=dmin, end=dmax )
            if (len(xs) != len(ys)):
                print(row)
                skipFig = True
            else:
                skipFig = False
                qualiferRecord=fetchSQL( sqlQualifiers, 'none')
                for i, value in qualiferRecord.iterrows():
                    row =qualiferRecord.iloc[i]
                    qualiferStr = str(row['QUALIFIERS'])
    
                annotationRecord=fetchSQL( sqlAnnotations, 'none')
                for i, value in annotationRecord.iterrows():
                    row=annotationRecord.iloc[i]
                    annotationStr =str(row['QUALIFIERS'])
    
    # Create dataframe from Qualifier Code cursor
                codesDF=fetchSQL(sqlwithCodes, 'none')
                codesDF.columns=['dateread','value','code']
                ycodes = codesDF['value']
                dmax2 = codesDF.dateread.max()
                xs2 = pd.date_range(dmin, dmax2 )
    
    # Create dataframe from Estimated Code cursor
                Ecodes=fetchSQL(sqlwithEs, 'none')
                EcodesDF = Ecodes
                EcodesDF.columns=['dateread','value','code']
                ecodes = EcodesDF['value']
                dmax3 = EcodesDF.dateread.max()
                xs3 = pd.date_range(dmin, dmax3 )
    # Create dataframes from Annoatation and Verification cursors
                annoDF=fetchSQL(sqlDCVPcodes, 'none')
                if len(annoDF.index) > 0:
                    annoDF.columns=['dateread','value','code']
                    dmax4 = annoDF.dateread.max()
                    xs4 = pd.date_range(dmin, dmax4 )
                    CannoCodes = annoDF['value']
    
                annoVDF=fetchSQL( sqlDCVPcodesV, 'none')
                if len(annoVDF.index) > 0:
                    annoVDF.columns=['dateread','value','code']
                    dmax5 = annoVDF.dateread.max()
                    xs5 = pd.date_range(dmin, dmax5)
                    VannoCodes = annoVDF['value']

        elif frequency == 'RI':
            skipFig = False
#   Random Interval needs to create a mask for null values with isfinite
            dmin = sdateDT
            xs = pd.date_range( dbhydDF.dateread.min() , dmax )
            ts2= np.array(ys.values).astype(np.double)
            ts2mask = np.isfinite(ts2)

        ThisDate=dt.date.today()
        currentTime = dt.datetime.now().time()
        timeStr = currentTime.strftime("%H:%M:%S" )
        timeStamp = ThisDate.strftime("%m/%d/%Y") + " " + timeStr
        if skipFig == False:
# Define general chart characteristics, titles and descriptive text:
            fig, chart = plt.subplots(figsize=(11,8.5))
            fontP = FontProperties()
            fontP.set_size('xx-small')
            chart.set_autoscale_on(True)
    
            chart.text(0.99, 0.01, 'Plots created: '+ timeStamp + \
                    '\nReviewed by the Hydrogeology Unit of the Water Supply Bureau',
                    verticalalignment='bottom', horizontalalignment='right',
                    transform=chart.transAxes,
                    color='green', fontsize=7)
    
            if frequency == 'DA':
                title= station +'\n' + dataSource +' ' + Aquifer2Process + \
                       ' Daily Water Level\n' + \
                       dmin.strftime("%m/%d/%Y") + ' thru ' + ThisDate.strftime("%m/%d/%Y")
            elif frequency == 'RI':
                title= station +'\n' + dataSource +' '+ Aquifer2Process + \
                       ' Random Interval Water Level\n' + \
                       dmin.strftime("%m/%d/%Y") + ' thru ' + ThisDate.strftime("%m/%d/%Y")
    
            plt.title(str(title),fontsize = 8)
    
    # Create X axis with full date range
            chart.set_xlim(dmin, ThisDate)
    
    # Plot the Data:
            markers =[]
            for m in Line2D.markers:
                try:
                    if len(m) == 1 and m != ' ':
                        markers.append(m)
                except TypeError:
                    pass
            style9 = markers[9]
    
            if frequency == 'DA':
                chart.plot_date(xs,ys,ydate=False,linestyle='-',
                                marker='.',markersize=1,linewidth=1,
                                color='blue',label=stationDB)
                Qstr='Qualifier List:[' + qualiferStr + ']'
                chart.plot_date(xs3,ecodes,ydate=False,linestyle='-',
                                marker='.',markersize=1,linewidth=1,
                                color='green',label='Estimated')
                chart.plot_date(xs2,ycodes,ydate=False,linestyle='-',
                                marker='o',markersize=3,linewidth=1,
                                color='red',label=Qstr)
                if annotationStr != 'None':
                    if 'C' in annotationStr:
                        chart.plot_date(xs4,CannoCodes,ydate=False,linestyle='None',
                                        marker=style9,markersize=4,linewidth=1,
                                        color='magenta',label='Calibration')
                    if 'V' in annotationStr:
                        chart.plot_date(xs5,VannoCodes,ydate=False,linestyle='None',
                                        marker=style9,markersize=4,linewidth=1,
                                        color='orange',label='Verification')
            else:
                chart.plot(xs[ts2mask],ys[ts2mask],linestyle='-',
                                marker='o',markersize=2,linewidth=1,
                                color='red',label=stationDB)
    
    # Set xaxis specifics:
            if len(xs) < 1095:
                majorlocator = plt.MultipleLocator(30.4)
            else:
                majorlocator = plt.MultipleLocator(365)
    
            myFmt = mdates.DateFormatter('%b %Y')
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
    
            chart.legend(loc=3, bbox_to_anchor=(.01,.01), prop=fontP, fancybox=True, shadow=False)
    
    # Create figure filenames and output chart .png's to the hydrograph directory.
            stationKey = station + '_' + key
            fname = str(stationKey)
            fig =  str('{0}.png'.format(fname))
            figout = fig.strip()
            out =  basepath + Aquifer2Process + "\\" + str(figout)
            plt.savefig(out,dpi=200)
            plt.close()
    return

def callback(result):
    result_list.append(result)

def makeChartsByAquifer(Aquifer2Process, DBKeyList):

    # Define SQL for selecting date ranges by DBKEY
    results = []
    pool = multiprocessing.Pool()

    for frequency in ('DA','RI'):
        if frequency == 'DA':
            sqlQ = setSQLstring('DBKeyDatesDA',keys=DBKeyList)
        else:
            sqlQ = setSQLstring('DBKeyDatesRI',keys=DBKeyList)

        print ('Selecting date ranges for ' +frequency+' DBkeys')

        DBKeys2WorkWith=fetchSQL(sqlQ, 'none')
        recKnt= len(DBKeys2WorkWith)

    # Display Execution progress on screen
        print ('Total hydrographs being created for '+frequency+': ' + str(recKnt))
        print ('Building Hydrographs')

        print (Aquifer2Process,frequency)

     # -- Multiprocessing with multiple Processes
        for i, row in  DBKeys2WorkWith.iterrows():
            result = pool.apply_async(makefigures,
                                      (Aquifer2Process,frequency,DBKeys2WorkWith.iloc[i]),
                                      callback=callback)
#            result=makefigures(Aquifer2Process,frequency,DBKeys2WorkWith.iloc[i])
            results.append(result)
    print (len(results), "figures sent to multiprocessing Pool")
    end = time.time()
    pool.close()
    print("Initial processes Elapsed time: ",end - start, " seconds")

    return(results)

#-------------------------------------------------------------------------------
#   Begin Execution
#-------------------------------------------------------------------------------
global Aquifer2Process
global start, end
start = time.time()

if __name__ == '__main__':
    result_list =[]
    AllrPool=[]

#   If command line options are present user ArgumentParser
    if (len(sys.argv) > 1):
        options = getOptions()
        option_dict = vars(options)
#   Command line argument define which aquifers to process
#    options.bulk is for all available Aquifers defined in this program
#     If the -aquifer keyword is provided, a specific aquifer should be provided
        if not options.bulk:
            Aquifer2Process = options.Aquifer
            print (Aquifer2Process)
            DBKeyList = setDBKeylist(Aquifer2Process)
            AllrPool.append(makeChartsByAquifer(Aquifer2Process, DBKeyList))
        else:
            print (allAquifers)
            for Aquifer2Process in allAquifers:
                print (Aquifer2Process)
                DBKeyList = setDBKeylist(Aquifer2Process)
                makeChartsByAquifer(Aquifer2Process, DBKeyList)
                AllrPool.append(makeChartsByAquifer(Aquifer2Process, DBKeyList))
        for rPool in reversed(AllrPool):
            for rp in rPool:
                rp.wait()
                rp.join
        end = time.time()
        print("Figure Creation Multiprocesses Elapsed time: ",end - start, " seconds")
    else:
         app = QtWidg.QApplication(sys.argv)
         window = RadioAquiferOptions()
         window.show()
         sys.exit(app.exec_())


