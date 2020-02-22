#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb 21 18:08:44 2020

@author: Pak Shing Ho

This file collects KLD and Compustat from WRDS and merges them using link table
constructed from 'KLD_Compustat_Link.py'.

Before merging, KLD cleaning and correction is required.
"""

import wrds
import pandas as pd
from fuzzywuzzy import fuzz
from cusipCorrection import cusipCorrection # a function to correct wrongly shifted CUSIP

###################
# Load Link table #
###################
KLD_COMP_Link = pd.read_csv('KLD_CRSP_CCM_COMP_link.csv', dtype={'gvkey': str})
print('Link table loaded.')

###################
# Obtain data from WRDS #
###################
print('Obtaining data')
conn = wrds.Connection()

# KLD: Get the list of Tickers, CUSIPs, Company Names and year in KLD
KLD = conn.raw_sql("""
                   select *
                   from kld.history
                   """)
                     
# Compustat: Get Companies with non-missing Asset or Sales Item

# funda = conn.raw_sql("""
#                      select gvkey, datadate, fyear, conm, sale
#                      from
#                      comp.funda
#                      where
#                      (sale > 0 or at > 0)
#                      and consol = 'C'
#                      and indfmt = 'INDL'
#                      and datafmt = 'STD'
#                      and popsrc = 'D'
#                      and curcd = 'USD'
#                      and final = 'Y'
#                      and fic = 'USA'
#                      and datadate >= '1990-01-01'
#                      """)
                     
funda = conn.raw_sql("""
                     select *
                     from
                     comp.funda
                     where
                     (sale > 0 or at > 0)
                     and consol = 'C'
                     and indfmt = 'INDL'
                     and datafmt = 'STD'
                     and popsrc = 'D'
                     and curcd = 'USD'
                     and final = 'Y'
                     and fic = 'USA'
                     and datadate >= '1990-01-01'
                     """)
conn.close()

"""
# Save datasets
KLD.to_csv('KLD.csv', index=False)
funda.to_csv('Compustat.csv', index=False)
"""

###################
# Clean and correct KLD data #
###################
print('Cleaning and correcting KLD data')

# Correct wrongly shifted CUSIP
KLD = cusipCorrection(KLD)

# Set 'NA', '0', '#N/A#' CUSIPs and tickers to missing values
KLD['cusip'].replace({'NA': None, '0': None, '#N/A': None}, inplace=True)
KLD['ticker'].replace({'NA': None, '#N/A': None}, inplace=True)

# Set 'companyname' to uppercase.
# This will allow more backfill and forwardfill observations.
KLD['companyname'] = KLD['companyname'].str.upper()
KLD['ticker'] = KLD['ticker'].str.upper()

# Back fill and forward fill missing CUSIPs. Can also try bfill ticker.
KLD['cusip'] = KLD.groupby(['companyname'])['cusip'].bfill().ffill()
KLD['ticker'] = KLD.groupby(['companyname'])['ticker'].bfill().ffill()

###################
# Merge data #
###################
print('Merging data')

df = KLD_COMP_Link.merge(KLD, left_on=['companyname', 'year', 'cusip_KLD', 'ticker_KLD'],
                         right_on=['companyname', 'year', 'cusip', 'ticker'])

df = df.merge(funda, on=['gvkey', 'fyear'])
# df_dup = df[df.duplicated(['gvkey', 'fyear'], keep=False)] # duplicates due to domicile
# df_dup.drop_duplicates(['gvkey', 'fyear'], keep='first', inplace=True)
df.drop_duplicates(['gvkey', 'fyear'], keep='first', inplace=True)

# Save merged dataset
df.to_csv('KLD_Compustat.csv', index=False)
