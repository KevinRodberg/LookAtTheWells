net use k: \\ad.sfwmd.gov\dfsroot\data\wsd\SUP\devel\source\Python\LookAtTheWells
net use u: \\ad.sfwmd.gov\dfsroot\data\wsd\Hydrogeology\Database\TimeSeries_QC\hydrographs

::Process LWC for Tamiami, Snadstone, and Mid-Hawthorn Aquifers
k:\WL_WQRegWellReport.py -RegMon -region LWC -path u:\RegDB\

::Process LWC tamiami hydrographs
k:\WL_WQRegWellReport.py -region LWC -aquifer Tamiami -RegMon -path u:\RegDB\

::Process UEC for Surficial Aquifers
k:\WL_WQRegWellReport.py -RegMon -region UEC -path u:\RegDB\
