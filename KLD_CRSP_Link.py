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

# Back fill and forward fill missing CUSIPs. Can also try bfill ticker.
_kld2['cusip'] = _kld2.groupby(['companyname'])['cusip'].bfill().ffill()

# Construct dates pre-2000, month is Aug; from 2001, monnth is Dec, all days are 31.
_kld2['month'] = '12'
_kld2['day'] = '31'
_kld2.loc[_kld2.year<=2000, 'month'] = '08' # commented out this has no effect on linking
_kld2.year = _kld2.year.astype(int).astype(str)
_kld2['date'] = pd.to_datetime(_kld2[['year', 'month', 'day']]).dt.date
_kld2.drop(columns=['month', 'day'], inplace=True)

_kld2_date = _kld2.groupby(['companyname','cusip']).date.agg(['min', 'max'])\
.reset_index().rename(columns={'min':'fdate', 'max':'ldate'})

# merge fdate ldate back to _kld2 data
_kld3 = pd.merge(_kld2, _kld2_date,how='left', on =['companyname','cusip'])
_kld3 = _kld3.sort_values(by=['companyname','cusip','date'])

# keep only the most recent company name
# determined by having date = ldate
_kld3 = _kld3.loc[_kld3.date == _kld3.ldate].drop(['date'], axis=1)


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
_link1_1 = pd.merge(_kld3, _crsp2, how='inner', left_on='cusip', right_on='ncusip')\
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


##########################
# Step 2: Link by TICKER #
##########################

# Find links for the remaining unmatched cases using Exchange Ticker 

# Identify remaining unmatched cases 
_nomatch1 = pd.merge(_kld3[['companyname']], _link1_2[['permno','companyname']], on='companyname', how='left')
_nomatch1 = _nomatch1.loc[_nomatch1.permno.isnull()].drop(['permno'], axis=1).drop_duplicates()

# Add KLD identifying information
kldid = _kld2
kldid = kldid.loc[kldid.companyname.notna()]

_nomatch2 = pd.merge(_nomatch1, kldid, how='inner', on=['companyname'])

# Create first and last 'start dates' for Exchange Tickers
# Label date range variables and keep only most recent company name

_nomatch3 = _nomatch2.groupby(['companyname', 'ticker']).date.agg(['min', 'max'])\
.reset_index().rename(columns={'min':'fdate', 'max':'ldate'})

_nomatch3 = pd.merge(_nomatch2, _nomatch3, how='left', on=['companyname','ticker'])

_nomatch3 = _nomatch3.loc[_nomatch3.date == _nomatch3.ldate]

# Get entire list of CRSP stocks with Exchange Ticker information

_crsp_n1 = conn.raw_sql(""" select ticker, comnam, permno, ncusip, namedt, nameenddt
                            from crsp.stocknames """)

_crsp_n1 = _crsp_n1.loc[_crsp_n1.ticker.notna()].sort_values(by=['permno','ticker','namedt'])

# Arrange effective dates for link by Exchange Ticker

_crsp_n1_namedt = _crsp_n1.groupby(['permno','ticker']).namedt.min().reset_index().rename(columns={'min':'namedt'})
_crsp_n1_nameenddt = _crsp_n1.groupby(['permno','ticker']).nameenddt.max().reset_index().rename(columns={'max':'nameenddt'})

_crsp_n1_dt = pd.merge(_crsp_n1_namedt, _crsp_n1_nameenddt, how = 'inner', on=['permno','ticker'])

_crsp_n1 = _crsp_n1.rename(columns={'namedt': 'namedt_ind', 'nameenddt':'nameenddt_ind'})

_crsp_n2 = pd.merge(_crsp_n1, _crsp_n1_dt, how ='left', on = ['permno','ticker'])

_crsp_n2 = _crsp_n2.rename(columns={'ticker':'crsp_ticker'})
_crsp_n2 = _crsp_n2.loc[_crsp_n2.nameenddt_ind == _crsp_n2.nameenddt].drop(['namedt_ind', 'nameenddt_ind'], axis=1)

# Merge remaining unmatched cases using Exchange Ticker 
# Note: Use ticker date ranges as exchange tickers are reused overtime

_link2_1 = pd.merge(_nomatch3, _crsp_n2, how='inner', left_on=['ticker'], right_on=['crsp_ticker'])
_link2_1 = _link2_1.loc[(_link2_1.ldate>=_link2_1.namedt) & (_link2_1.fdate<=_link2_1.nameenddt)]

# Score using company name using 6-digit CUSIP and company name spelling distance
_link2_1['name_ratio'] = _link2_1.apply(lambda x: fuzz.token_set_ratio(x.comnam, x.companyname), axis=1)

_link2_2 = _link2_1
_link2_2['cusip6'] = _link2_2[_link2_2['cusip'].notna()].apply(lambda x: x.cusip[:6], axis=1)
_link2_2['ncusip6'] = _link2_2.apply(lambda x: x.ncusip[:6], axis=1)
_link2_2['ncusip1_7'] = _link2_2.apply(lambda x: x.ncusip[1:7], axis=1)


# Score using company name using 6-digit CUSIP and company name spelling distance

def score2(row):
    if ((row['cusip6']==row['ncusip6']) | (row['cusip6']==row['ncusip1_7'])) & (row['name_ratio'] >= name_ratio_p10):
        score = 0
    elif ((row['cusip6']==row['ncusip6']) | (row['cusip6']==row['ncusip1_7'])):
        score = 4
    elif row['name_ratio'] >= name_ratio_p10:
        score = 5
    else:
        score = 6
    return score

# assign size portfolio
_link2_2['score']=_link2_2.apply(score2, axis=1)

# Some companies may have more than one TICKER-PERMNO link
# so re-sort and keep the case (PERMNO & Company name from CRSP)
# that gives the lowest score for each KLD TICKER 

_link2_2 = _link2_2[['cusip','ticker','permno','companyname','comnam', 'name_ratio', 'score']].sort_values(by=['companyname','ticker','score'])
_link2_2_score = _link2_2.groupby(['companyname', 'ticker']).score.min().reset_index()

_link2_3 = pd.merge(_link2_2, _link2_2_score, how='inner', on=['companyname', 'ticker', 'score'])
_link2_3 = _link2_3[['cusip','ticker','permno','companyname','comnam','name_ratio','score']].drop_duplicates()

#####################################
# Step 3: Finalize LInks and Scores #
#####################################

iclink = _link1_2.append(_link2_3)