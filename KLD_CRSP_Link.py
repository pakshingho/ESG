#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 31 16:19:14 2019

@author: shinggg
"""

import wrds
import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz
from cusipCorrection import cusipCorrection

###################
# Connect to WRDS #
###################
conn=wrds.Connection()

#########################
# Step 1: Link by CUSIP #
#########################

# 1.1 KLD: Get the list of Tickers, CUSIPs, Company Names and year in KLD
_kld1 = conn.raw_sql("""
                     select ticker, cusip, companyname, year from kld.history
                     """)

# Correct wrongly shifted CUSIP
_kld1 = cusipCorrection(_kld1)

# set 'NA', '0', '#N/A#' CUSIPs to missing values
_kld2 = _kld1.copy()
_kld2['cusip'].replace({'NA':None, '0':None, '#N/A':None}, inplace=True)
_kld2['ticker'].replace({'NA':None, '#N/A':None}, inplace=True)
_kld2['companyname'] = _kld2['companyname'].str.upper()

# Back fill and forward fill missing CUSIPs
_kld2['cusip'] = _kld2.groupby(['companyname'])['cusip'].bfill().ffill()

# Construct dates pre-2000, month is Aug; from 2001, monnth is Dec, all days are 31.
_kld2['month'] = '12'
_kld2['day'] = '31'
_kld2.loc[_kld2.year<=2000, 'month'] = '08'
_kld2.year = _kld2.year.astype(int).astype(str)
_kld2['date'] = pd.to_datetime(_kld2[['year', 'month', 'day']]).dt.date
_kld2.drop(columns=['month', 'day'], inplace=True)

_kld2_date = _kld2.groupby(['companyname','cusip']).date.agg(['min', 'max'])\
.reset_index().rename(columns={'min':'fdate', 'max':'ldate'})

# merge fdate ldate back to _kld2 data
_kld2 = pd.merge(_kld2, _kld2_date,how='left', on =['companyname','cusip'])
_kld2 = _kld2.sort_values(by=['companyname','cusip','date'])

# keep only the most recent company name
# determined by having date = ldate
_kld2 = _kld2.loc[_kld2.date == _kld2.ldate].drop(['date'], axis=1)


# 1.2 CRSP: Get all permno-ncusip combinations
_crsp1 = conn.raw_sql("""
                      select permno, ncusip, cusip, comnam, namedt, nameenddt
                      from crsp.stocknames
                      where ncusip != ''
                      """)

# first namedt
_crsp1_fnamedt = _crsp1.groupby(['permno','ncusip']).namedt.min().reset_index()

# last nameenddt
_crsp1_lnameenddt = _crsp1.groupby(['permno','ncusip']).nameenddt.max().reset_index()

# merge both
_crsp1_dtrange = pd.merge(_crsp1_fnamedt, _crsp1_lnameenddt, \
                          on = ['permno','ncusip'], how='inner')

# replace namedt and nameenddt with the version from the dtrange
_crsp1 = _crsp1.drop(['namedt'],axis=1).rename(columns={'nameenddt':'enddt'})
_crsp2 = pd.merge(_crsp1, _crsp1_dtrange, on =['permno','ncusip'], how='inner')

# keep only most recent company name
_crsp2 = _crsp2.loc[_crsp2.enddt ==_crsp2.nameenddt].drop(['enddt'], axis=1)


# 1.3 Create CUSIP Link Table

# Link by full cusip, company names and dates
_link1_1 = pd.merge(_kld2, _crsp2, how='inner', left_on='cusip', right_on='ncusip')\
.sort_values(['companyname','permno','ldate'])

# Keep link with most recent company name
_link1_1_tmp = _link1_1.groupby(['companyname','permno']).ldate.max().reset_index()
_link1_2 = pd.merge(_link1_1, _link1_1_tmp, how='inner', on =['companyname', 'permno', 'ldate'])

# Calculate name matching ratio using FuzzyWuzzy

# Note: fuzz ratio = 100 -> match perfectly
#       fuzz ratio = 0   -> do not match at all

# Comment: token_set_ratio is more flexible in matching the strings:
# fuzz.token_set_ratio('AMAZON.COM INC',  'AMAZON COM INC')
# returns value of 100

# fuzz.ratio('AMAZON.COM INC',  'AMAZON COM INC')
# returns value of 93

_link1_2['name_ratio'] = _link1_2.apply(lambda x: fuzz.token_set_ratio(x.comnam, x.companyname), axis=1)

# Note on parameters:
# The following parameters are chosen to mimic the SAS macro %iclink
# In %iclink, name_dist < 30 is assigned score = 0
# where name_dist=30 is roughly 90% percentile in total distribution
# and higher name_dist means more different names.
# In name_ratio, I mimic this by choosing 10% percentile as cutoff to assign
# score = 0

# 10% percentile of the company name distance
name_ratio_p10 = _link1_2.name_ratio.quantile(0.10)

# Function to assign score for companies matched by:
# full cusip and passing name_ratio
# or meeting date range requirement

def score1(row):
    if (row['fdate']<=row['nameenddt']) & (row['ldate']>=row['namedt']) & (row['name_ratio'] >= name_ratio_p10):
        score = 0
    elif (row['fdate']<=row['nameenddt']) & (row['ldate']>=row['namedt']):
        score = 1
    elif row['name_ratio'] >= name_ratio_p10:
        score = 2
    else:
        score = 3
    return score

# assign size portfolio
_link1_2['score']=_link1_2.apply(score1, axis=1)
_link1_2 = _link1_2[['cusip_x', 'ticker','permno','companyname','comnam','name_ratio','score']].rename(columns={'cusip_x':'cusip'})
_link1_2 = _link1_2.drop_duplicates()