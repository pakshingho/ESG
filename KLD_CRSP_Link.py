#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 31 16:19:14 2019

@author: Pak Shing Ho

In KLD dataset, 'companyname'-'year' is the unique identifier as some 'cusip's and 
'ticker's are missing.

# to create a linking table between CRSP and KLD
# Output is a score reflecting the quality of the link
# Score = 0 (best link) to Score = 6 (worst link)
#
# More explanation on score system:
# - 0: BEST match: using (cusip, cusip dates and company names)
#          or (exchange ticker, company names and 6-digit cusip)
# - 1: Cusips and cusip dates match but company names do not match
# - 2: Cusips and company names match but cusip dates do not match
# - 3: Cusips match but cusip dates and company names do not match
# - 4: tickers and 6-digit cusips match but company names do not match
# - 5: tickers and company names match but 6-digit cusips do not match
# - 6: tickers match but company names and 6-digit cusips do not match

"""

import wrds
import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz
from cusipCorrection import cusipCorrection

###################
# Connect to WRDS #
###################
conn = wrds.Connection()

#########################
# Step 1: Link by CUSIP #
#########################

# 1.1 KLD: Get the list of Tickers, CUSIPs, Company Names and year in KLD
_kld1 = conn.raw_sql("""
                     select ticker, cusip, companyname, year from kld.history
                     """)

# Correct wrongly shifted CUSIP
_kld1 = cusipCorrection(_kld1)

# set 'NA', '0', '#N/A#' CUSIPs and tickers to missing values
_kld2 = _kld1.copy()
_kld2['cusip'].replace({'NA':None, '0':None, '#N/A':None}, inplace=True)
_kld2['ticker'].replace({'NA':None, '#N/A':None}, inplace=True)

# set 'companyname' to uppercase. This will allow more backfill and forwardfill observations
_kld2['companyname'] = _kld2['companyname'].str.upper()

# Back fill and forward fill missing CUSIPs. Can also try bfill ticker.
_kld2['cusip'] = _kld2.groupby(['companyname'])['cusip'].bfill().ffill()
_kld2['ticker'] = _kld2.groupby(['companyname'])['ticker'].bfill().ffill()


# Construct dates pre-2000, month is Aug; from 2001, monnth is Dec, all days are 31.
_kld2['month'] = '12'
_kld2['day'] = '31'
_kld2.loc[_kld2.year<=2000, 'month'] = '08' # commented out this has no effect on linking KLD-CRSP but affects linking with CCM-Link
_kld2.year = _kld2.year.astype(int).astype(str)
_kld2['date'] = pd.to_datetime(_kld2[['year', 'month', 'day']]).dt.date
_kld2.drop(columns=['month', 'day'], inplace=True)

_kld2_date = _kld2.groupby(['companyname', 'cusip']).date.agg(['min', 'max'])\
.reset_index().rename(columns={'min':'fdate', 'max':'ldate'})

# merge fdate ldate back to _kld2 data
_kld3 = pd.merge(_kld2, _kld2_date, how='left', on =['companyname','cusip'])
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
# Step 3: Finalize Links and Scores #
#####################################

# Caution: This link is based uppercase of 'companyname'. One need to convert 
# 'companyname' in KLD data set before using this link to merge data sets.
KLD_CRSP_link = _link1_2.append(_link2_3)


################################################
# Using Link Tables to Merge KLD and CRSP Data #
################################################

"""
36397 merge by date method
49606 merge by monthly period method
49606 merge by business day method 49559 if not consider pre-2000 Aug
"""

KLD = _kld2.copy()
KLD['monthly'] = pd.to_datetime(KLD.date).dt.to_period('M') # month method

KLD['date'] = pd.to_datetime(KLD.date) + pd.offsets.BusinessMonthBegin(0) - pd.offsets.BusinessDay(1) # business day method
KLD['date'] = KLD['date'].apply(lambda x: x.date()) # business day method

# CRSP Monthly Stock files
crsp_msf = conn.raw_sql("""
                        select distinct permno, date,
                                       cusip
                        from crsp.msf
                        """)
crsp_msf['monthly'] = pd.to_datetime(crsp_msf.date).dt.to_period('M') # month method

# Merge KLD with the link table
KLD_linked = KLD.merge(KLD_CRSP_link, on=['companyname'],
                       suffixes=('_KLD', '_LINK'))
# KLD_linked.drop(columns=[''], inplace=True)

# Merge CRSP with the link table
crsp_linked = crsp_msf.merge(KLD_CRSP_link, on='permno',
                             suffixes=('_crsp', '_LINK'))

# 4 different merging methods give the same result:
linked1 = KLD_linked.merge(crsp_linked, left_on=['companyname', 'date', 'permno', 'ticker_LINK'], 
                           right_on=['companyname', 'date', 'permno', 'ticker'],
                           suffixes=('_KLD_LINK', '_crsp_LINK'))
linked1.drop(columns=['monthly_crsp_LINK', 'cusip_LINK_crsp_LINK', 'ticker',
                      'comnam_crsp_LINK', 'name_ratio_crsp_LINK', 'score_crsp_LINK'], inplace=True)
linked1.rename(columns={'monthly_KLD_LINK':'monthly',
                        'cusip_LINK_KLD_LINK':'cusip_LINK',
                        'comnam_KLD_LINK':'comnam',
                        'name_ratio_KLD_LINK':'name_ratio',
                        'score_KLD_LINK':'score'}, inplace=True)

linked2 = crsp_linked.merge(KLD_linked, left_on=['companyname', 'date', 'permno', 'ticker'], 
                            right_on=['companyname', 'date', 'permno', 'ticker_LINK'],
                            suffixes=('_crsp_LINK', '_KLD_LINK'))
linked2.drop(columns=['monthly_crsp_LINK', 'cusip_LINK_crsp_LINK', 'ticker',
                      'comnam_crsp_LINK', 'name_ratio_crsp_LINK', 'score_crsp_LINK'], inplace=True)
linked2.rename(columns={'monthly_KLD_LINK':'monthly',
                        'cusip_LINK_KLD_LINK':'cusip_LINK',
                        'comnam_KLD_LINK':'comnam',
                        'name_ratio_KLD_LINK':'name_ratio',
                        'score_KLD_LINK':'score'}, inplace=True)

linked3 = KLD_linked.merge(crsp_msf, on=['permno', 'date'],
                           suffixes=('_KLD_LINK', '_crsp'))
linked3.drop(columns=['monthly_crsp'], inplace=True)
linked3.rename(columns={'cusip':'cusip_crsp', 'monthly_KLD_LINK':'monthly'}, inplace=True)

linked4 = KLD.merge(crsp_linked, on=['companyname', 'monthly'],
                    suffixes=('_KLD', '_crsp_LINK'))
linked4.drop(columns=['date_crsp_LINK'], inplace=True)
linked4.rename(columns={'date_KLD':'date', 'cusip':'cusip_KLD', 'ticker_crsp_LINK':'ticker_LINK'}, inplace=True)

# check if linked1, linked2, linked3, and linked4 identical (expect TRUE)
linked2[sorted(linked2)].sort_values(list(linked2[sorted(linked2)].columns)).reset_index(drop=True).equals(linked1[sorted(linked1)].sort_values(list(linked1[sorted(linked1)].columns)).reset_index(drop=True))
linked3[sorted(linked3)].sort_values(list(linked3[sorted(linked3)].columns)).reset_index(drop=True).equals(linked2[sorted(linked2)].sort_values(list(linked2[sorted(linked2)].columns)).reset_index(drop=True))
linked4[sorted(linked4)].sort_values(list(linked4[sorted(linked4)].columns)).reset_index(drop=True).equals(linked3[sorted(linked3)].sort_values(list(linked3[sorted(linked3)].columns)).reset_index(drop=True))

# How about lowercase tickers?