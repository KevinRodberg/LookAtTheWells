import arcpy
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties

import cx_Oracle

import time
import datetime 
from datetime import timedelta
import dateutil.parser

#logfile = r'H:\pyVersion.log'
#with open(logfile, 'w') as f:
#   f.write(str(sys.executable))
print sys.executable
#sys.exit()

deltalen = 7
d1 = datetime.datetime.today()
d2 = d1 - timedelta(deltalen)
currdate = datetime.datetime.strftime(d1, "%Y-%m-%d")
pastdate = datetime.datetime.strftime(d2, "%Y-%m-%d")


def gencsv():
   
   
   #stastr2 = "'S176-H', 'S177-H'"
   #stastr2 = "'S61-H', 'S65-H'"
   stastr2 = "'S97-H', 'S99-H'"
   #stastr2 = "'S176-H', 'S177-H', 'S18CM-H', 'S333-H', 'S49-H', 'S61-H', 'S65-H', 'S68-H', 'S97-H', 'S99-H', 'L001+', 'L005+', 'L006+', 'LZ40+'"
   
   ### Build a DSN (can be subsitited for a TNS name)     
   dsn = cx_Oracle.makedsn("wrepdb.sfwmd.gov", 1521, "WREP")     
   oradb = cx_Oracle.connect("PUB", "pub", dsn)     
   cursor = oradb.cursor()   

   sqlQry = """SELECT bb.station,
                      CAST(TO_CHAR(AVG(aa.VALUE), 'fm9999999.90') AS FLOAT) AS val,
                      TO_DATE(TO_CHAR(aa.date_time, 'YYYY-MM-DD'), 'YYYY-MM-DD') AS daily_date
                FROM DCVP_DBA.TIME_SERIES_VW@dcvp aa
          INNER JOIN
               (SELECT  b.station_id, b.station, b.data_type, b.status from keyword_tab b
                 WHERE b.frequency = 'DA'
                   AND b.station_id IS NOT NULL
                   AND b.station_id IN
                        (""" + stastr2 + """)
                   AND b.status = 'A'     
          order by b.station_id) bb ON aa.station_id=bb.station_id
              WHERE aa.station_id IN ('S176-H', 'S177-H', 'S18CM-H', 'S333-H', 'S49-H', 'S61-H', 'S65-H', 'S68-H', 'S97-H', 'S99-H', 'L001+', 'L005+', 'L006+', 'LZ40+')
                AND aa.date_time >= TO_DATE ('""" + pastdate + """ 12:00:01 AM', 'YYYY-MM-DD HH:MI:SS AM')
                AND aa.date_time <= TO_DATE ('""" + currdate + """ 11:59:59 PM', 'YYYY-MM-DD HH:MI:SS PM')
                GROUP BY aa.station_id, bb.station, bb.data_type, bb.status, TO_DATE(TO_CHAR(aa.date_time, 'YYYY-MM-DD'), 'YYYY-MM-DD')
           ORDER BY TO_DATE(TO_CHAR(aa.date_time, 'YYYY-MM-DD'), 'YYYY-MM-DD')"""
   
   
   cursor.execute(sqlQry)
   datArray = []
   cxRows = cursor.fetchall()
   
   #close the conn to ora
   cursor.close() 
   oradb.close()
   del cursor
   
   dbhydDF = pd.DataFrame(cxRows, columns=['site', 'value', 'dateread'])
   #print dbhydDF.columns
   #print dbhydDF.values
   return dbhydDF
   
   """dmax = dbhydDF.dateread.max()
   dmin = dbhydDF.dateread.min()
      
   dfmin = dbhydDF[dbhydDF['dateread'] == dmin]
   dfmax = dbhydDF[dbhydDF['dateread'] == dmax]
    
   dfcombo = dfmin.merge(dfmax, on="site")
   dfcombo['change'] = dfcombo.value_y - dfcombo.value_x
   dfcombo['DBHYDRO_STATION'] = dfcombo.site
     
   ##drop unwanted fields
   dfcombo.columns = ['staname', 'pastvalue', 'datereadpast', 'currentvalue', 'datereadcur', 'change', 'DBHYDRO_STATION']
   del dfcombo['datereadpast']
   del dfcombo['datereadcur']        
   del dbhydDF, dfmin, dfmax
          
   return dfcombo"""

       
  
def generateGraphs_dbhyd(df):
   
   outfolder = r'H:'
   sta_keys = np.unique(df['site'])
   #print sta_keys

   fig = plt.figure()
   ax = "ax" + str(211)
   ax = fig.add_subplot(211)

   colorvalue = 0  
   for sta_key in sta_keys:
      
     if colorvalue == 0:
        usecolor = 'blue'
     elif colorvalue == 1:
        usecolor = 'green'
     else:
        usecolor = 'red'
        
     df1 = df[df['site'] == sta_key]
     
     print "Attempting to plot " + str(sta_key) 
     #print df1.values
      
     dmax = df1.dateread.max()
     dmin = df1.dateread.min()
        
     xs = pd.date_range(dmin, dmax)
     ys = df1['value']
  
     dfmin = df1[df1['dateread'] == dmin]
     dfmax = df1[df1['dateread'] == dmax]

     #fig = plt.figure()
     #ax = "ax" + str(211)
     #ax = fig.add_subplot(211)
     ax.plot_date(xs, ys, ydate=False, linestyle='-', marker='', linewidth=1, color=usecolor, label=str(sta_key))
     
     colorvalue = colorvalue + 1

   fontP = FontProperties()
   fontP.set_size('xx-small')
 
   #ax.set_ylim([3, 5])
   #ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.05), fancybox=True, shadow=True)
   box = ax.get_position()
   ax.set_position([box.x0, box.y0, box.width * 0.8, box.height])
   ax.legend(loc='center left', bbox_to_anchor=(1, 0.5), prop = fontP, fancybox=True, shadow=False)
   
   ax.set_ylabel('Head (ft)')

   minorlocator = plt.MultipleLocator(1)
   ax.xaxis.set_minor_locator(minorlocator)
   ax.yaxis.set_minor_locator(minorlocator)
   ax.set_autoscale_on(True)
   
   for ylabel in ax.yaxis.get_ticklabels():
        ylabel.set_fontsize(7)
      
   for label in ax.xaxis.get_ticklabels():
        label.set_rotation(330)
        label.set_fontsize(7)

   title = 'STATION: {0}'.format(sta_key)
   fig.text(0.5,0.975,title,horizontalalignment='center',verticalalignment='top')
   fname = str(sta_key) 
   fig =  str('{0}.png'.format(fname))
   figout = fig.strip()
        
   out = str(outfolder) + "\\" + str(figout)

   plt.savefig(out)
   plt.close()
     
   print out + "....saved"
   
   del xs, ys
   del dfmin, dfmax,
   del df1
       
   del sta_keys
   
   #except:
   #arcpy.AddMessage("The following error(s) occured in generateGraphs_dbhyd: " + arcpy.GetMessages(2))
   return



def plot_random():

   outfolder = r'H:'
   figout = 'S01_figure'

   fig = plt.figure()

   vals = []
   vals.append([x, x])
   vals.append([x, 2 * x])
   vals.append([x, 3 * x])

   plt.plot([x, x])
   plt.plot([x, 2 * x])
   plt.plot([x, 3 * x])
   
   plt.legend(['y = x', 'y = 2x', 'y = 3x'])

   fig =  str('{0}.png'.format(figout))
   figout = fig.strip()
        
   #out = str(outfolder) + "\\" + str(figout)
   out = "in_memory\\" + str(figout)

   plt.savefig(out)
   print "saved"
   plt.close()
  




#plot_random()
_dfcombo = gencsv()
generateGraphs_dbhyd(_dfcombo)

