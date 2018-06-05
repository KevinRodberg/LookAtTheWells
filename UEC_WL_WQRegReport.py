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

basepath = "\\\\ad.sfwmd.gov\\dfsroot\\data\\wsd\\Hydrogeology\\Database\\TimeSeries_QC\\hydrographs\\RegDB\\"
allAquifers = ['Floridan','Biscayne','Hawthorne','Limestone','Surficial','Sandstone','Tamiami', 'Sand']

def getOptions():
    parser = ArgumentParser(prog='RodbergReport')
    parser.add_argument("-aquifer", 
                        dest="Aquifer",
                        choices=allAquifers,
                        help="Process Al1 DBKeys for 1 Aquifer.")
    parser.add_argument("-all",
                        action="store_true",
                        dest="bulk",
                        default='All',
                        help="Process All DBKeyLists by Aquifer.")
    parser.add_argument("-rfgw",
                        action="store_true",
                        dest="rfgw",
                        help="Process Floridan DBKeys with WQ")                        
    parser.add_argument("-uecWS",
                        action="store_true",
                        dest="uecWS",
                        help="Process UEC DBKeys with WQ")
    parser.add_argument("-uecReg",
                        action="store_true",
                        dest="uecReg",
                        help="Process UEC Reg Facilities WL with WQ")
                        
    args = parser.parse_args()
    return args
#
def setDBKeylist(Aquifer):
    if Aquifer == 'Floridan':
        DBkeyList = ('87537','87535','87527','WF841','SA297','37315','03550','03578','TO069','SC381','TO065',
                     '09601','PT092','TP352','VN450','VN448','VN446','AL719','AL724','03487','03537','03530',
                     '03545','TA837','09604','03376','TA835','PD409','TA819','O6378','O6371','O6374','WF809'
                     )     
        DBkeyListWhileTesting = ('88715','03576','PT638','OB341','W4254','OB342','OB343','OB344','P0061','P0062','P0063',
                     'US179','US177','TS191','TS189','P1982','P2038','PT502','TO048','TO046','P0064','OG452',
                     'WN400','WN398','OB340','OB339','OB338','P1959','02869','SC005','VM991','WF835','WF829',
                     '09610','P2032','P2030','P2028','RU208','RU206','P0706','P0704','T0960','SM775','SM773',
                     'OB383','OB384','NV837','NV838','NV846','TO052','P2108','P2111','P2112','W4328','VW827',
                     'VW823','WH018','WH012','OB389','SD017','SM771','TA937','TA939','PS977','UO682','UO680',
                     '87537','87535','87527','WF841','SA297','37315','03550','03578','TO069','SC381','TO065',
                     '09601','PT092','TP352','VN450','VN448','VN446','AL719','AL724','03487','03537','03530',
                     '03545','TA837','09604','03376','TA835','PD409','TA819','O6378','O6371','O6374','WF809',
                     '09599','SC192','SC190','03396','09600','PS973','PS971','PS969','PS986','PT115','OG450',
                     'OG451','UP246','VN324','VM825','VM823','P6306','P6310','P6314','OB396','OB395',
                     'US356','PT526','VN339','AI849','AI857','AI861','37759','37194','03341','03343','TB036',
                     'PT642','SP555','OH809','OH808','OH807','P0045','P0047','PT646')
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

def fetchSQL(cursor, sql, param):
    if param == 'none':
        cursor.execute(sql)
    else:
        cursor.execute(sql)
    recordSet = cursor.fetchall()
    return recordSet

def makeChartsByAquifer(options, DBKeyList):
        ThisDate=dt.date.today()
        currentTime = dt.datetime.now().time()
        timeStr = currentTime.strftime("%H:%M:%S" )
        timeStamp = ThisDate.strftime("%m/%d/%Y") + " " + timeStr
        print timeStamp
        myFmt = mdates.DateFormatter('%b %Y')

        try:
            print 'Establishing Database connection'
            cnxn = pyodbc.connect('DSN=WREP; UID=pub; PWD=pub')
            cnxn2 = pyodbc.connect('DSN=WRED; UID=pub; PWD=pub')
            cnxn3 = pyodbc.connect('DSN=GENP; UID=pub; PWD=pub')

        except Exception as err:
            print err
        cursor = cnxn.cursor()
        cursr2 = cnxn2.cursor()   
        if options.uecReg:
          cursor = cnxn3.cursor()


        # Define SQL for selecting date ranges by DBKEY
        if options.uecWS:
                 sqlMDL = "select Station, Aquifer, Max_Dev_limit from BTURCOTT.MAX_DEV_LIMITS"

        	 sqlQ="SELECT d.dbkey,D.DATA_TYPE,D.STATION,D.FREQUENCY,D.AGENCY,d.START_DATE as sDATE," + \
        	    "      to_char(D.START_DATE,'MM/DD/YYYY') as START_DATE," + \
        	    "      to_char(D.END_DATE,'MM/DD/YYYY') as END_DATE FROM KEYWORD_TAB d " + \
        	    "WHERE d.county IN ('MAR', 'STL') and (d.status is null or d.status ='A') AND d.strata < 210 and d.data_type ='WELL' " + \
        	    "AND d.end_date > to_date('12/31/2006','MM/DD/YYYY') " + \
        	    "AND (d.start_date < to_date ('12/31/2006','MM/DD/YYYY') and d.start_date is not null) AND d.frequency = ? ORDER BY D.STATION"
        	 print sqlQ
        elif options.uecReg:
        	 sqlQ="SELECT ADMIN.PERMIT_NO STATION," + \
   		     "  WU_APP_FACILITY.FACINV_ID DBKEY,WU_FAC_INV.NAME WELL,TL_REQUIREMENTS.NAME DATA_TYPE," + \
   		     "  min(WUC_APP_SIM_SUBMS.APPLIES_TO_DATE) as START_DATE, max(WUC_APP_SIM_SUBMS.APPLIES_TO_DATE) as END_DATE, " + \
   		     "  (max(WUC_APP_SIM_SUBMS.APPLIES_TO_DATE) -min(WUC_APP_SIM_SUBMS.APPLIES_TO_DATE)) POR " + \
   		     "FROM REG.ADMIN ADMIN,REG.APP_COUNTIES APP_COUNTIES,REG.TL_SOURCES TL_SOURCES,REG.APP_LANDUSES APP_LANDUSES," + \
      		     "  REG.APP_LC_DEFS APP_LC_DEFS,REG.TL_REQUIREMENTS TL_REQUIREMENTS,REG.WUC_APP_LC_REQMTS WUC_APP_LC_REQMTS," + \
      		     "  REG.WUC_APP_SIM_SUBMS WUC_APP_SIM_SUBMS,REG.WU_APP_FACILITY WU_APP_FACILITY, REG.WU_FAC_INV WU_FAC_INV " + \
      		     "WHERE ADMIN.APP_NO = APP_LC_DEFS.ADMIN_APP_NO AND WU_FAC_INV.ID = WU_APP_FACILITY.FACINV_ID " + \
      		     "  AND WU_APP_FACILITY.ADMIN_APP_NO = ADMIN.APP_NO" + \
      		     "  AND WU_APP_FACILITY.FACINV_ID = TO_NUMBER(REGEXP_SUBSTR(WUC_APP_LC_REQMTS.REQM_ENT_KEY1,'^[-]?[[:digit:]]*\.?[[:digit:]]*$'))" + \
      		     "  AND WU_APP_FACILITY.SOURCE_ID = TL_SOURCES.ID AND TL_SOURCES.name LIKE '%Surf%'" + \
      		     "  AND SOURCE_ID = TL_SOURCES.ID AND APP_LC_DEFS.ADMIN_APP_NO = WUC_APP_LC_REQMTS.APPLC_ADMIN_APP_NO" + \
      		     "  AND APP_LC_DEFS.ID = WUC_APP_LC_REQMTS.APPLC_ID AND WUC_APP_LC_REQMTS.REQM_ID = WUC_APP_SIM_SUBMS.WALR_REQM_ID " + \
      		     "  AND WUC_APP_LC_REQMTS.TL_LC_REQM_ID = TL_REQUIREMENTS.ID AND WUC_APP_LC_REQMTS.APPLC_ADMIN_APP_NO = ADMIN.APP_no " + \
      		     "  AND ADMIN.APP_NO = APP_COUNTIES.ADMIN_APP_NO AND ADMIN.APP_NO = APP_LANDUSES.ADMIN_APP_NO " + \
      		     "  AND (   TL_REQUIREMENTS.NAME IN ('Chloride') " + \
      		     "          ) " + \
      		     "  AND APP_COUNTIES.PRIORITY = 1 AND APP_LANDUSES.USE_PRIORITY = 1 " + \
      		     "  AND (    WUC_APP_SIM_SUBMS.APPLIES_TO_DATE >= TO_DATE ('2000-01-01', 'YYYY-MM-DD') " + \
      		     "       AND WUC_APP_SIM_SUBMS.APPLIES_TO_DATE < TO_DATE ('2018-01-01', 'YYYY-MM-DD')) " + \
      		     "  AND (APP_LANDUSES.LU_CODE = 'PWS') AND APP_COUNTIES.CNTY_CODE IN (56, 43, 50) " + \
       		     "group by ADMIN.PERMIT_NO ,WU_APP_FACILITY.FACINV_ID ,WU_FAC_INV.NAME, TL_REQUIREMENTS.NAME " + \
 		     "having  (max(WUC_APP_SIM_SUBMS.APPLIES_TO_DATE) -min(WUC_APP_SIM_SUBMS.APPLIES_TO_DATE)) > 365 " 
 		 print sqlQ

        else:
        	 sqlQ="SELECT d.dbkey,D.DATA_TYPE,D.STATION,D.FREQUENCY,D.AGENCY,d.START_DATE as sDATE," + \
        	    "      to_char(D.START_DATE,'MM/DD/YYYY') as START_DATE," + \
        	    "      to_char(D.END_DATE,'MM/DD/YYYY') as END_DATE FROM KEYWORD_TAB d " + \
        	    "WHERE d.DBKey in " + str(DBKeyList) + " AND d.frequency = ? ORDER BY D.STATION"
        WQtest = False

       	    
        for frequency in ('DA','RI'):
            if frequency == 'DA' and options.uecReg:
                 print 'Do dailys for Reg data'
            else:
            	 print 'Selecting date ranges for ' +frequency+' DBkeys'
            	 DBKeys2WorkWith=fetchSQL(cursor, sqlQ, frequency)
            	 recKnt= len(DBKeys2WorkWith)

		# Display Execution progress on screen        
	    	 steps = recKnt/20
	    	 if steps > 1:
			print 'Progress  interval: ' + str(steps) + ' hydrographs created for each 5%'
			print 'Total hydrographs: ' + str(recKnt)
			i=0
			print 'Building Hydrographs'
			print '                                                      1'
			print 'Progress for '+frequency+'   1 1 2 2 3 3 4 4 5 5 6 6 7 7 8 8 9 9 0'
			print '                  0 5 0 5 0 5 0 5 0 5 0 5 0 5 0 5 0 5 0 '
			print '                [',
	    	 else:
			steps = 1
			print 'Total hydrographs being created for '+frequency+': ' + str(recKnt)
			i=0
			print 'Building Hydrographs'       
			print '                [',
	   	 sys.stdout.flush()

           	 for row in DBKeys2WorkWith:
        # Define chart title and figure name variables from each row
               		 station = row.STATION.strip(' ')
             		 dataType = row.DATA_TYPE.strip(' ')
			 if options.uecReg:
			  dataSource = 'REG'
			  key= str(row.WELL)
			  stationDB = station + ',  (well[' + key + '])'
			 # print stationDB, row.DBKEY
			 else:
			  dataSource = row.AGENCY.strip(' ')
			  key= row.DBKEY.strip(' ')
			  stationDB = station + ' (dbkey[' + key + '])'

			 i = i + 1
			 if i%steps == 0: 
			    print 'X', 
			    sys.stdout.flush()

		# Frequency codes for Daily and Random require separate queries as the data is pulled from separate tables in DBHydro.
		#   DMDBASE.DAILY_DATA and DMDBASE.RANDOM_DATA
		# Random INterval Regulatory queries also require separe query from separate database.

		# Daily query:
			 if frequency == 'DA':
			 #   sdateDT = row.SDATE
			 #   oneStartDate= row.START_DATE.strip(' ')
			    oneStartDate = '12/31/2006'                    
			    sdateDT = dt.datetime.strptime(oneStartDate,"%m/%d/%Y").date()

			    sql= "SELECT daily_date, value FROM dmdbase.dm_daily_data WHERE DBKEY='" +\
				row.DBKEY.strip(' ')+\
				"' AND daily_date > TO_DATE('" +oneStartDate + "','MM/DD/YYYY')"
			#    print sql

		#   Hydrographs for Daily data need additional queries for each set of qualifier codes.
		#   Currently E for Estimated in one query, all other codes <> M in another.

			    sqlwithCodes = "SELECT l.daily_date, r.VALUE, r.code FROM dm_daily_data l " +\
					   "       LEFT JOIN (SELECT * FROM dm_daily_data c WHERE c.code IS NOT NULL" +\
					   "                      AND c.code IN ('C','V','P','<','>','!') AND c.DBKEY = '" +\
					   row.DBKEY.strip(' ')+ "'" +\
					   "                      AND c.daily_date > TO_DATE ('" +oneStartDate + "','MM/DD/YYYY')) r " +\
					   "       ON l.DAILY_DATE = r.DAILY_DATE WHERE l.DBKEY = '" +row.DBKEY.strip(' ') +\
					   "' AND l.daily_date > TO_DATE ('" +oneStartDate + "','MM/DD/YYYY') ORDER BY l.daily_date"

			    sqlQualifiers= "SELECT LISTAGG (code, ',') WITHIN GROUP (ORDER BY code) qualifiers " +\
					   "  FROM (SELECT DISTINCT code FROM dm_daily_data WHERE code in('C','V','P','<','>','!') " +\
					   "   AND VALUE IS NOT NULL AND dbkey = '" +\
					   row.DBKEY.strip(' ') + "')"

			    sqlAnnotations = "SELECT LISTAGG (code, ',') WITHIN GROUP (ORDER BY code) qualifiers " +\
					     "  FROM (SELECT distinct a.code FROM DCVP_DBA.DCVP_ANNOTATIONS a, " +\
					     "       DMDBASE.KEYWORD_TAB_VIEW b, dm_daily_data c " + \
					     " WHERE a.code IN ('C','V') AND a.station_id = b.station_id AND b.dbkey = '" +\
					     row.DBKEY.strip(' ')+ "'  AND C.DBKEY = b.dbkey " +\
					     "   AND TO_CHAR(c.daily_date, 'MM/DD/YYYY')=" +\
					     "       TO_CHAR(TO_DATE(a.Start_DATE_TIME,'yyyymmdd:hh24mi'),'MM/DD/YYYY'))"

			    sqlDCVPcodes = "SELECT l.daily_date, r.VALUE, r.code FROM dm_daily_data l " +\
					   "    LEFT JOIN (SELECT distinct c.daily_date, c.VALUE, A.code " +\
					   "         FROM DCVP_DBA.DCVP_ANNOTATIONS a, " +\
					   "              DMDBASE.KEYWORD_TAB_VIEW b, dm_daily_data c " + \
					   "        WHERE a.code IN ('C') AND a.station_id = b.station_id  "+\
					   "          AND b.dbkey = '" + row.DBKEY.strip(' ')+\
					   "'         AND C.DBKEY = b.dbkey "+\
					   "          AND TO_CHAR (c.daily_date, 'MM/DD/YYYY')=" +\
					   "              TO_CHAR(TO_DATE(A.Start_DATE_TIME,'yyyymmdd:hh24mi'),'MM/DD/YYYY')" + \
					   "    ORDER BY   A.Start_DATE_TIME) r "+\
					   "          ON l.DAILY_DATE = r.daily_date "+\
					   "    WHERE l.DBKEY = '" +row.DBKEY.strip(' ')+"' AND l.daily_date > " +\
					   "          TO_DATE ('" +oneStartDate + "','MM/DD/YYYY') ORDER BY l.daily_date"

			    sqlDCVPcodesV = "SELECT l.daily_date, r.VALUE, r.code FROM dm_daily_data l " +\
					    "       LEFT JOIN (SELECT distinct c.daily_date, c.VALUE, A.code " +\
					    "                    FROM DCVP_DBA.DCVP_ANNOTATIONS a, " +\
					    "                         DMDBASE.KEYWORD_TAB_VIEW b, dm_daily_data c " + \
					    "                   WHERE a.code IN ('V') AND a.station_id = b.station_id " +\
					    "                     AND b.dbkey = '" + row.DBKEY.strip(' ')+ "AND C.DBKEY = b.dbkey "+\
					    "'                    AND TO_CHAR(c.daily_date, 'MM/DD/YYYY')=" +\
					    "                         TO_CHAR(TO_DATE(A.Start_DATE_TIME,'yyyymmdd:hh24mi'),'MM/DD/YYYY')" + \
					    "                   ORDER BY A.Start_DATE_TIME) r "+\
					    "               ON l.DAILY_DATE = r.daily_date " +\
					    "            WHERE l.DBKEY = '" +row.DBKEY.strip(' ')+"' "+\
					    "              AND l.daily_date > TO_DATE ('" +oneStartDate + "','MM/DD/YYYY') "+\
					    "            ORDER BY l.daily_date"            

			    sqlwithEs = "SELECT l.daily_date, r.VALUE, r.code FROM dm_daily_data l " +\
					"       LEFT JOIN (SELECT * FROM dm_daily_data c WHERE c.code = 'E' "+\
					"                     AND c.DBKEY = '" + row.DBKEY.strip(' ') + \
					"'                    AND c.daily_date > TO_DATE ('" +oneStartDate + "','MM/DD/YYYY')) r "+\
					"              ON l.DAILY_DATE = r.DAILY_DATE "+\
					"           WHERE l.DBKEY = '" +row.DBKEY.strip(' ')+"' "+\
					"             AND l.daily_date > TO_DATE ('" +oneStartDate + "','MM/DD/YYYY') "+\
					"           ORDER BY l.daily_date"

			 elif frequency =='RI' and not options.uecReg:                    
		# Random Interval query:
		#   anyDailyDBKey= '01081'    <--- This DBKey was picked to provide daily dates to random data.
			   # sdateDT = row.SDATE
			   # print sdateDT
			    oneStartDate = '12/31/2006'    
			    sdateDT = dt.datetime.strptime(oneStartDate,"%m/%d/%Y").date()


			    sql= "SELECT TO_DATE(b.daily_date, 'MM/DD/YYYY') daily_date, a.VALUE " +\
				"   FROM(SELECT to_char(random_date,'MM/DD/YYYY') random_date , avg(value) as value " +\
				"          FROM dmdbase.dm_random_data "+ \
				"         WHERE dmdbase.dm_random_data.DBKEY = '" + row.DBKEY.strip(' ') +\
				"'        GROUP BY to_char(random_date, 'MM/DD/YYYY')) a " +\
				"         RIGHT JOIN(SELECT to_char(daily_date,'MM/DD/YYYY') daily_date " + \
				"                      FROM dmdbase.dm_daily_data " + \
				"                     WHERE dmdbase.dm_daily_data.dbkey = '01081'  " +\
				"                       AND dmdbase.dm_daily_data.daily_date > " + \
				"                           TO_DATE('" +oneStartDate + "','MM/DD/YYYY')) b " + \
				"            ON b.daily_date = a.random_date ORDER BY TO_DATE (b.daily_date, 'MM/DD/YYYY') "
		#If options.rfgw build and prcess query for WQ data.
			 elif options.rfgw or options.uecWS:
				sqlWQ = "SELECT TO_DATE(b.daily_date, 'MM/DD/YYYY') daily_date, " +\
				    "           wq.SP_COND, wq.CHLORIDE, WQ.TDS, WQ.PH FROM " +\
				    "          (SELECT * FROM " +\
				    "               (SELECT S.PROJECT_CODE,S.STATION_ID as station, " +\
				    "                       to_char(S.DATE_COLLECTED,'MM/DD/YYYY') AS WQ_date, " +\
				    "                       S.MATRIX, S.TEST_NUMBER, S.VALUE " +\
				    "                  FROM WQDORA.SAMPLE s, dmdbase.DM_KEYREF k " +\
				    "                 WHERE s.station_id = k.station " +\
				    "                   AND k.dbkey = '" +row.DBKEY.strip(' ')+"'" +\
				    "                   AND s.PROJECT_CODE IN " +\
				    "                       ('RFGW','LFAKB','KBFAS','KFL','LEC','LWC','PAIREDWELL','UEC','UECF','WFPP','GW_CL'))  " +\
				    "                 PIVOT (AVG (VALUE) FOR test_number " +\
				    "                   IN (9 AS SP_COND, 32 AS CHLORIDE, 97 AS TDS, 10 AS PH))) wq " +\
				    "   RIGHT JOIN " +\
				    "    (SELECT to_char(daily_date,'MM/DD/YYYY') daily_date " +\
				    "       FROM dmdbase.dm_daily_data " +\
				    "      WHERE dmdbase.dm_daily_data.DBKEY = '" +row.DBKEY.strip(' ')+"'" +\
				    "        AND dmdbase.dm_daily_data.daily_date > TO_DATE ('" +oneStartDate + "','MM/DD/YYYY')) b" +\
				    "   ON to_date(b.daily_date,'MM/DD/YYYY') = to_date(WQ.wq_date,'MM/DD/YYYY')  " +\
				    "   ORDER BY to_date(b.daily_date,'MM/DD/YYYY')"

	       #   Fetch WQ records if options set to rfgw
				wqrecs=fetchSQL(cursor, sqlWQ, 'none')
				mdl_recs = fetchSQL(cursr2, sqlMDL, 'none')
				WQtest = False
				if len(wqrecs) > 0:
				    wqDF = pd.DataFrame(wqrecs,columns=['wqdate','spcond','chloride','tds','ph'])
				    yWQmaxSC = float(wqDF.spcond.max())
				    yWQminSC = float(wqDF.spcond.min())
				    yWQmaxCL = float(wqDF.chloride.max())
				    yWQminCL = float(wqDF.chloride.min())
				    yWQmaxTDS = float(wqDF.tds.max())
				    yWQminTDS = float(wqDF.tds.min())
				    yWQmaxPH = float(wqDF.ph.max())*100.0
				    yWQminPH = float(wqDF.ph.min())*100.0
				    yWQmax = float(max(yWQmaxSC, yWQmaxCL, yWQmaxTDS, yWQmaxPH))
				    yWQmin = float(min(yWQminSC, yWQminCL, yWQminTDS, yWQminPH))
				    yWQrange = float(yWQmax-yWQmin)
				    yWQmin = float(yWQmin - round((yWQrange * .2),2))
				    if yWQmin < 0.0:
					yWQmin = 0
				    yWQmax = float(yWQmax + round((yWQrange * .2),2))
				    yWQrange = float((yWQmax-yWQmin)*1.1)
				    yWQinterval = float(round((yWQrange / 8.0),1))
	       #   Process just Specific Conductance for now.
				    yWQsc = wqDF['spcond']
				    yWQcl = wqDF['chloride']
				    yWQtds = wqDF['tds']
				    yWQph = wqDF['ph']*100.0
				    lisWQ = yWQph.dropna()
				    dWQmax2 = wqDF.wqdate.max()
				    dmin = wqDF.wqdate.min()
				    WQxs = pd.date_range(dmin, dWQmax2 + dt.timedelta(days=1))
				 #   WQxs = pd.date_range(dmin, dWQmax2)

				    if np.isnan(yWQmax):
					WQtest = False
				    else:
					WQtest = True

			 elif options.uecReg:

				aDateTime = row.START_DATE 
				oneStartDate,aTime = str(aDateTime).split(" ")
			#	print oneStartDate
				sdateDT = dt.datetime.strptime(oneStartDate,"%Y-%m-%d").date()  
				edateDT = ThisDate.strftime("%Y-%m-%d")

				sqlWQ ="SELECT to_date(b.daily_date,'yyyy-mm-dd') as daily_date, avg(value) as value from " + \
				 "    (SELECT to_char(WUC_APP_SIM_SUBMS.APPLIES_TO_DATE, 'MM/DD/YYYY') AS monthly_date, WUC_APP_SIM_SUBMS.SUBM_VALUE as value" + \
				 " FROM REG.ADMIN ADMIN,REG.APP_COUNTIES APP_COUNTIES,REG.TL_SOURCES TL_SOURCES,REG.APP_LANDUSES APP_LANDUSES," + \
				 "  REG.APP_LC_DEFS APP_LC_DEFS,REG.TL_REQUIREMENTS TL_REQUIREMENTS,REG.WUC_APP_LC_REQMTS WUC_APP_LC_REQMTS," + \
				 "  REG.WUC_APP_SIM_SUBMS WUC_APP_SIM_SUBMS,REG.WU_APP_FACILITY WU_APP_FACILITY, REG.WU_FAC_INV WU_FAC_INV " + \
				 "WHERE ADMIN.APP_NO = APP_LC_DEFS.ADMIN_APP_NO AND WU_FAC_INV.ID = WU_APP_FACILITY.FACINV_ID " + \
				 "  AND WU_APP_FACILITY.ADMIN_APP_NO = ADMIN.APP_NO" + \
				 "  AND WU_APP_FACILITY.FACINV_ID = TO_NUMBER(REGEXP_SUBSTR(WUC_APP_LC_REQMTS.REQM_ENT_KEY1,'^[-]?[[:digit:]]*\.?[[:digit:]]*$'))" + \
				 "  AND WU_APP_FACILITY.SOURCE_ID = TL_SOURCES.ID AND TL_SOURCES.name LIKE '%Surf%'" + \
				 "  AND SOURCE_ID = TL_SOURCES.ID AND APP_LC_DEFS.ADMIN_APP_NO = WUC_APP_LC_REQMTS.APPLC_ADMIN_APP_NO" + \
				 "  AND APP_LC_DEFS.ID = WUC_APP_LC_REQMTS.APPLC_ID AND WUC_APP_LC_REQMTS.REQM_ID = WUC_APP_SIM_SUBMS.WALR_REQM_ID " + \
				 "  AND WUC_APP_LC_REQMTS.TL_LC_REQM_ID = TL_REQUIREMENTS.ID AND WUC_APP_LC_REQMTS.APPLC_ADMIN_APP_NO = ADMIN.APP_no " + \
				 "  AND ADMIN.APP_NO = APP_COUNTIES.ADMIN_APP_NO AND ADMIN.APP_NO = APP_LANDUSES.ADMIN_APP_NO " + \
				 "  AND (   TL_REQUIREMENTS.NAME IN ('Chloride') " + \
				 "         ) " + \
				 "  AND APP_COUNTIES.PRIORITY = 1 AND APP_LANDUSES.USE_PRIORITY = 1 " + \
				 "  AND (    WUC_APP_SIM_SUBMS.APPLIES_TO_DATE >= to_date('" + oneStartDate + "','yyyy-mm-dd') " + \
				 "       AND WUC_APP_SIM_SUBMS.APPLIES_TO_DATE < TO_DATE ('2018-01-01', 'YYYY-MM-DD')) " + \
				 "  AND (APP_LANDUSES.LU_CODE = 'PWS') AND APP_COUNTIES.CNTY_CODE IN (56, 43, 50) " + \
				 "  AND WU_APP_FACILITY.FACINV_ID = " + str(row.DBKEY) +") wq " +\
				" RIGHT JOIN (SELECT TO_CHAR (to_date('" + oneStartDate + "','yyyy-mm-dd')-1 + LEVEL, 'yyyy-mm-dd') daily_date FROM DUAL" + \
				"              WHERE (TO_CHAR (to_date('" + oneStartDate + "','yyyy-mm-dd')+1 + LEVEL, 'yyyy-mm-dd') < '2018-01-01') " + \
				"            CONNECT BY LEVEL <= (SELECT (TRUNC (SYSDATE) - to_date('" + oneStartDate + "','yyyy-mm-dd')+1) FROM DUAL)) b " + \
				"         ON to_date(wq.monthly_date,'MM/DD/YYYY') = to_date(b.daily_date,'YYYY-MM-DD') group by b.daily_date order by b.daily_date"

				sql= "SELECT to_date(b.daily_date,'yyyy-mm-dd')  as daily_date, avg(value) as value from " + \
				"    (SELECT to_char(WUC_APP_SIM_SUBMS.APPLIES_TO_DATE, 'MM/DD/YYYY') AS monthly_date, WUC_APP_SIM_SUBMS.SUBM_VALUE as value " + \
				"       FROM REG.ADMIN ADMIN, REG.APP_COUNTIES APP_COUNTIES, REG.TL_SOURCES TL_SOURCES, REG.APP_LANDUSES APP_LANDUSES, " + \
				"            REG.APP_LC_DEFS APP_LC_DEFS,REG.TL_REQUIREMENTS TL_REQUIREMENTS, REG.WUC_APP_LC_REQMTS WUC_APP_LC_REQMTS, " + \
				"            REG.WUC_APP_SIM_SUBMS WUC_APP_SIM_SUBMS, REG.WU_APP_FACILITY WU_APP_FACILITY, REG.WU_FAC_INV WU_FAC_INV " + \
				"      WHERE ADMIN.APP_NO = APP_LC_DEFS.ADMIN_APP_NO AND WU_FAC_INV.ID = WU_APP_FACILITY.FACINV_ID AND " + \
				"            WU_APP_FACILITY.ADMIN_APP_NO = ADMIN.APP_NO AND WU_APP_FACILITY.FACINV_ID = TO_NUMBER" + \
				"(REGEXP_SUBSTR(WUC_APP_LC_REQMTS.REQM_ENT_KEY1,'^[-]?[[:digit:]]*\.?[[:digit:]]*$')) AND " + \
				"            WU_APP_FACILITY.FACINV_ID  = " + str(row.DBKEY) +" " +\
				"        AND WU_APP_FACILITY.SOURCE_ID = TL_SOURCES.ID AND TL_SOURCES.name LIKE '%Surf%' AND SOURCE_ID = TL_SOURCES.ID " + \
				"        AND APP_LC_DEFS.ADMIN_APP_NO = WUC_APP_LC_REQMTS.APPLC_ADMIN_APP_NO AND APP_LC_DEFS.ID = WUC_APP_LC_REQMTS.APPLC_ID " + \
				"        AND WUC_APP_LC_REQMTS.REQM_ID = WUC_APP_SIM_SUBMS.WALR_REQM_ID AND WUC_APP_LC_REQMTS.TL_LC_REQM_ID = TL_REQUIREMENTS.ID " + \
				"        AND WUC_APP_LC_REQMTS.APPLC_ADMIN_APP_NO = ADMIN.APP_no AND ADMIN.APP_NO = APP_COUNTIES.ADMIN_APP_NO " + \
				"        AND ADMIN.APP_NO = APP_LANDUSES.ADMIN_APP_NO AND   WUC_APP_SIM_SUBMS.TMU_CODE like  'FT%' " + \
				"        AND (APP_COUNTIES.PRIORITY LIKE 1) AND (APP_LANDUSES.USE_PRIORITY LIKE 1) AND " + \
				"            (WUC_APP_SIM_SUBMS.APPLIES_TO_DATE >= to_date('" + oneStartDate + "','yyyy-mm-dd') " + \
				"        AND  WUC_APP_SIM_SUBMS.APPLIES_TO_DATE < TO_DATE ('2018-01-01', 'YYYY-MM-DD')) " + \
				"        AND (APP_LANDUSES.LU_CODE = 'PWS') AND APP_COUNTIES.CNTY_CODE IN (56, 43, 50)) wl " + \
				" RIGHT JOIN (SELECT TO_CHAR (to_date('" + oneStartDate + "','yyyy-mm-dd')-1 + LEVEL, 'yyyy-mm-dd') daily_date FROM DUAL" + \
				"              WHERE (TO_CHAR (to_date('" + oneStartDate + "','yyyy-mm-dd')+1 + LEVEL, 'yyyy-mm-dd') < '2018-01-01') " + \
				"            CONNECT BY LEVEL <= (SELECT (TRUNC (SYSDATE) - to_date('" + oneStartDate + "','yyyy-mm-dd')+1) FROM DUAL)) b " + \
				"         ON to_date(wl.monthly_date,'MM/DD/YYYY') = to_date(b.daily_date,'YYYY-MM-DD') group by b.daily_date order by b.daily_date"
				#print "WQ/n"
				#print sqlWQ
				#print "WL/n"
				#print sql


	       #   Fetch WQ records if options set to rfgw
				wqrecs=fetchSQL(cursor, sqlWQ, 'none')
		    #            mdl_recs = fetchSQL(cursr2, sqlMDL, 'none')
				WQtest = False
				if len(wqrecs) > 0:
				    wqDF = pd.DataFrame(wqrecs,columns=['daily_date','chloride'])
				    yWQmaxCL = float(wqDF.chloride.max())
				    yWQminCL = float(wqDF.chloride.min())

				    yWQmax = yWQmaxCL
				    yWQmin = yWQminCL
				    yWQrange = float(yWQmax-yWQmin)
				    if yWQrange < .1:
				      yWQrange = 1.0
				    yWQmin = float(yWQmin - round((yWQrange * .2),2))
				    if yWQmin < 0.0:
					yWQmin = 0
				    yWQmax = float(yWQmax + round((yWQrange * .2),2))
				    yWQrange = float((yWQmax-yWQmin)*1.1)
				    yWQinterval = float(round((yWQrange / 8.0),1))

	       #   Process just Specific Conductance for now.
				    yWQcl = wqDF['chloride']
				    dWQmax2 = wqDF.daily_date.max()

				    dmin = wqDF.daily_date.min()
				    WQxs = pd.date_range(dmin, dWQmax2 + dt.timedelta(days=1))
				 #   WQxs = pd.date_range(dmin, dWQmax2)

				#    print "WQ dates",dmin, dWQmax2

				    yWQcl = wqDF['chloride']
				    yWQsc = None
				    yWQtds = None
				    yWQph = None

				    if np.isnan(yWQmax):
					WQtest = False
				#	print "No WQ records for ", str(row.DBKEY)
				    else:
					WQtest = True
				     #   print sql
				     #   print sqlWQ
      
                # Fetch records for either Daily or Random Interval with sql String
	 		 records=fetchSQL(cursor, sql, 'none')
		# If query returned 0 records skip the chart processing.
			 if len(records):
		# Process Random Interval and Daily data in a similar fashion
		#   when retrieving data values and defining the x and y axis
			    dbhydDF = pd.DataFrame(records, columns=['daily_date','value'])
			    dmax = dbhydDF.daily_date.max()
			    ymax = float(dbhydDF.value.max())
			    ymin = float(dbhydDF.value.min())
			    yavg = float(dbhydDF.value.mean())
			    if np.isnan(ymax):
			       WLtest = False
			 #      print "No WL values for ", str(row.DBKEY)
			    elif ymin == ymax:
			       WLtest = False
			    else:
			       WLtest = True
			   #    print "WL range:",ymin, ymax
			   # print "WL dates", dmin, dmax

			    yrange = float(ymax - ymin)
			    ymin = ymin - round((yrange * .2),2)
			    ymax = ymax + round((yrange * .2),2)
			    yrange = float((ymax - ymin)*1.1)
			    yinterval = round((yrange / 8.0),1)
			    ys = dbhydDF['value']

		# Hydrographs for Daily data need additional query processing for each set of qualifier codes.
			    if frequency == 'DA':
				dmin = sdateDT    
	#                        dmin = oneStartDate
				xs = pd.date_range(dmin, dmax - dt.timedelta(days=1))

				qualiferRecord=fetchSQL(cursor, sqlQualifiers, 'none')
				cols =[ t[0] for t in cursor.description ]
				for col, value in zip(cols,qualiferRecord):
				    qualiferStr =str(value).strip('( )')[:-1]

				annotationRecord=fetchSQL(cursor, sqlAnnotations, 'none')
				cols =[ t[0] for t in cursor.description ]
				for col, value in zip(cols,annotationRecord):
				    annotationStr =str(value).strip('( )')[:-1]                    

		# Create dataframe from Qualifier Code cursor              
				Qcodes=fetchSQL(cursor, sqlwithCodes, 'none')
				codesDF = pd.DataFrame(Qcodes, columns=['dateread','value','code'])
				ycodes = codesDF['value']
				dmax2 = codesDF.dateread.max()
				xs2 = pd.date_range(dmin, dmax2 - dt.timedelta(days=1))

		# Create dataframe from Estimated Code cursor
				Ecodes=fetchSQL(cursor, sqlwithEs, 'none')
				EcodesDF = pd.DataFrame(Ecodes, columns=['dateread','value','code'])
				ecodes = EcodesDF['value']
				dmax3 = EcodesDF.dateread.max()
				xs3 = pd.date_range(dmin, dmax3 - dt.timedelta(days=1))

		# Create dataframes from Annoatation and Verification cursors
				Acodes=fetchSQL(cursor, sqlDCVPcodes, 'none')
				if len(Acodes) > 0:
				    annoDF = pd.DataFrame(Acodes, columns=['dateread','value','code'])
				    dmax4 = annoDF.dateread.max()
				    xs4 = pd.date_range(dmin, dmax4 - dt.timedelta(days=1))
				    CannoCodes = annoDF['value']

				Vcodes=fetchSQL(cursor, sqlDCVPcodesV, 'none')
				if len(Vcodes) > 0:
				    annoVDF = pd.DataFrame(Vcodes, columns=['dateread','value','code'])
				    dmax5 = annoVDF.dateread.max()
				    xs5 = pd.date_range(dmin, dmax5 - dt.timedelta(days=1))
				    VannoCodes = annoVDF['value']
			    elif options.uecReg:
				dmin = sdateDT    
				#dmax = edateDT
				#print dmin, dmax
				dmin = dbhydDF.daily_date.min()				
				xs = pd.date_range(dmin, dmax) 
				if WLtest:
				  ts2= np.array(ys.values).astype(np.double)
				  ts2mask = np.isfinite(ts2)
				else:
				  ts3= np.array(yWQcl.values).astype(np.double)
				  ts3mask = np.isfinite(ts3)
			    else:
				dmin = dbhydDF.dateread.min()                        
				xs = pd.date_range(dmin, dmax )              
				ts2= np.array(ys.values).astype(np.double)
				ts2mask = np.isfinite(ts2)

		# Define general chart characteristics, titles and descriptive text:
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
			      if options.uecWS:
				  title= station +'\n' + dataSource + ' Well [Strata < 200] Daily Water Level\n' + \
							     sdateDT.strftime("%m/%d/%Y") + ' thru ' + ThisDate.strftime("%m/%d/%Y") + '\n' + \
				       ' Mean Water Level = %0.2f' % yavg
			      else:

				  title= station +'\n' + dataSource +' ' + Aquifer2Process +' Daily Water Level\n' + \
				       sdateDT.strftime("%m/%d/%Y") + ' thru ' + ThisDate.strftime("%m/%d/%Y") + '\n' + \
				       ' Mean Water Level = %0.2f' % yavg
			    else:
			      if options.uecWS:
				  title= station +'\n' + dataSource + ' Well [Strata < 200] Random Interval Water Level\n' + \
				       sdateDT.strftime("%m/%d/%Y") + ' thru ' + ThisDate.strftime("%m/%d/%Y")+ '\n' + \
				       ' Mean Water Level = %0.2f' % yavg                      
			      else:
				if WLtest:
				  title= station +'\n' + dataSource +' '+ Aquifer2Process + ' Random Interval Water Level\n' + \
				       sdateDT.strftime("%m/%d/%Y") + ' thru ' + ThisDate.strftime("%m/%d/%Y")+ '\n' + \
				       ' Mean Water Level = %0.2f' % yavg
				else:
				  title= station +'\n' + dataSource +' '+ Aquifer2Process + ' Random Interval Chloride Level\n' + \
				       sdateDT.strftime("%m/%d/%Y") + ' thru ' + ThisDate.strftime("%m/%d/%Y")+ '\n' 
			    plt.title(str(title),fontsize = 8)

		# Create X axis with full date range
			    chart.set_xlim(sdateDT, ThisDate)

			    #chart.set_xlim(dmin, ThisDate)


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

			    if frequency == 'DA':
				if len(xs) <> len(ys):
				  print stationDB, len(xs) , len(ys)
				else:
				    chart.plot_date(xs,ys,ydate=False,linestyle='-',
						marker='.',markersize=1,linewidth=1,color='blue',label=stationDB)
				    Qstr='Qualifier List:[' + qualiferStr + ']'
				    chart.plot_date(xs3,ecodes,ydate=False,linestyle='-',
						marker='.',markersize=1,linewidth=1,color='green',label='Estimated')
				    chart.plot_date(xs2,ycodes,ydate=False,linestyle='b-',
						marker='o',markersize=4,linewidth=1,color='red',label=Qstr)                
				if annotationStr <> 'None':
				    if 'C' in annotationStr: 
					chart.plot_date(xs4,CannoCodes,ydate=False,linestyle='None',
							marker=style9,markersize=6,linewidth=1,color='magenta',label='Calibration')
				    if 'V' in annotationStr:
					chart.plot_date(xs5,VannoCodes,ydate=False,linestyle='None',
							marker=style6,markersize=4,linewidth=1,color='yellow',label='Verification')
			    else:
				if len(xs) <> len(ys):
				    print stationDB, len(xs) , len(ys), dmin, dmax, WLtest, WQtest
				else:         
				    if WLtest:
				      chart.plot(xs[ts2mask],ys[ts2mask],linestyle='-',
					   marker='o',markersize=2,linewidth=1,color='red',label=stationDB)
				    elif WQtest:
				    #  print len(ts3mask), len(yWQcl) , len(WQxs), len(xs)
				      chart.plot(WQxs[ts3mask],yWQcl[ts3mask],linestyle='-',
					   marker='o',markersize=2,linewidth=1,color='red',label=stationDB)                            

		# Set xaxis specifics:
			    if len(xs) < 1095:
				majorlocator = plt.MultipleLocator(30.4)
			    elif len(xs) <7300:
				majorlocator = plt.MultipleLocator(365)
			    else:
				majorlocator = plt.MultipleLocator(730)

			    chart.xaxis.set_major_formatter(myFmt)
			    chart.xaxis.set_major_locator(majorlocator)

			    for label in chart.xaxis.get_ticklabels():
				label.set_rotation(300)
				label.set_fontsize(7)

		# Set yaxis specifics:
			    if WLtest:
			#      print yinterval
			      major_yloc= plt.MultipleLocator(yinterval)
			      chart.yaxis.set_major_locator(major_yloc)
			    elif WQtest:
			 #     print yWQinterval
			      major_yloc= plt.MultipleLocator(yWQinterval)
			      chart.yaxis.set_major_locator(major_yloc)
			    else:
			       print "",

			    if dataType == 'UNHD':
				chart.set_ylabel('Uncorrected Head (ft)')
			    else:
			      if WLtest:
				chart.set_ylabel('Head (ft)')
			      else:
				chart.set_ylabel('Chloride mg/L')


			    for ylabel in chart.yaxis.get_ticklabels():
				ylabel.set_fontsize(7)

		# Add second Y-axis for WQ ranges shared with Xaxis

			    if WLtest and WQtest and len(wqrecs) > 0:
				wqChart = chart.twinx()
				major_y2loc= plt.MultipleLocator(yWQinterval)
				wqChart.yaxis.set_major_locator(major_y2loc)
				wqChart.set_ylabel('Water Quality Range')
				if options.uecReg:
				  wqChart.plot_date(xs,yWQcl,ydate=False,linestyle='None',marker=style9,markersize=6,linewidth=1,color='cyan',label='Chloride (mg/L)')

				if not options.uecReg:
				  wqChart.plot_date(xs,yWQsc,ydate=False,linestyle='None',marker=style9,markersize=6,linewidth=1,color='yellow',label='Specific Conductance (uS/cm)')
				  wqChart.plot_date(xs,yWQtds,ydate=False,linestyle='None',marker=style9,markersize=6,linewidth=1,color='green',label='TDS(mg/L)')
				  wqChart.plot_date(xs,yWQph,ydate=False,linestyle='None',marker=style9,markersize=6,linewidth=1,color='blue',label='pH units *100') 
				  for xm, ym, zm in zip(xs, yWQph, yWQph):
					wqChart.annotate('%s' % str(float(zm/100.0)),  xy = (xm, ym), xytext = (0,10), textcoords='offset points',ha='center', va='top')
				wqChart.legend(loc=1, prop = fontP, fancybox=True, shadow=False)
				for ylabel in wqChart.yaxis.get_ticklabels():
				    ylabel.set_fontsize(7)
				WQtest = False   
			    if WLtest or WQtest:
				    chart.legend(loc=3,  prop = fontP, fancybox=True, shadow=False)

			# Create figure filenames and output chart .png's to the hydrograph directory.
				    stationKey = station + '_' + key 
				    fname = str(stationKey) 
				    fig =  str('{0}.png'.format(fname))
				    figout = fig.strip()
				    if options.uecWS:
				      out =  basepath + "Surficial - UEC" + "\\" + str(figout)
				    else:
				      out =  basepath + Aquifer2Process + "\\" + str(figout)

				    plt.savefig(out)
				    plt.close()

            print ' ]  Done!'

options = getOptions()
option_dict = vars(options)

if options.rfgw or options.uecWS or options.uecReg:
    print "RFGW processing"
    Aquifer2Process = 'Floridan'
    if options.uecWS or options.uecReg:
       Aquifer2Process = 'Surficial'
    print Aquifer2Process
    DBKeyList = setDBKeylist(Aquifer2Process)
    print DBKeyList
    makeChartsByAquifer(options, DBKeyList)
elif not options.bulk:
    print Aquifer2Process
    Aquifer2Process = options.Aquifer
    DBKeyList = setDBKeylist(Aquifer2Process)
    makeChartsByAquifer(options, DBKeyList)
else:
    print allAquifers
    for Aquifer2Process in allAquifers:
        print Aquifer2Process
        DBKeyList = setDBKeylist(Aquifer2Process)
        makeChartsByAquifer(options, DBKeyList)        
