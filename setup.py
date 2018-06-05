# -*- coding: utf-8 -*-
"""
Created on Mon Jun 19 15:19:15 2017

@author: krodberg
"""

from distutils.core import setup
import py2exe
import numpy
import pandas 
import matplotlib
import sys
from glob import glob
sys.setrecursionlimit(3000)

setup(
      version ="0.1.0",
      description="RodbergReportMP --Multiprocessing  creation of Hydrographs",
      name ="RodbergReportMP",
      data_files = [("Microsoft.VC90.CRT", glob(r'C:\Users\krodberg\AppData\Local\Continuum\Anaconda2\Lib\site-packages\pythonwin\*.*'))],
      console=["RodbergReportMP.py"])