#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 11 20:28:08 2019

@author: shinggg
"""

import wrds
import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz

###################
# Connect to WRDS #
###################
conn=wrds.Connection()

conn.describe_table(library="crsp", table="msf")
conn.describe_table(library="crsp", table="msenames")
conn.describe_table(library="comp", table="security")

###########################
# Method 1: Link by CUSIP #
###########################

# 1.1 CRSP MSF: Get the list of PERMNO, DATE in CRSP Monthly Stock File
crsp_msf = conn.raw_sql("""
                        select distinct permno, date, 
                                        cusip
                        from crsp.msf
                        """)

# 1.2 CRSP MSENAMES: Get the list of PERMNO, NCUSIP, NAMEDT, NAMEENDT in CRSP Monthly Stock Event - Name History
crsp_msenames = conn.raw_sql("""
                             select distinct permno, ncusip, namedt, nameendt,
                                             cusip, ticker, comnam
                             from crsp.msenames
                             where ncusip != ''
                             and shrcd in (10,11)
                             """)

# 1.3 COMPUSTAT SECURITY: Get the list of GVKEY, IID in COMPUSTAT SECURITY
comp = conn.raw_sql("""
                    select distinct gvkey, iid, cusip,
                                    tic
                    from comp.security
                    where cusip != ''
                    and excntry='USA'
                    """)

# 1.4 Merge CRSP MSF and MSENAMES and keep relevant dates
crsp_m = crsp_msf.merge(crsp_msenames, on='permno')
crsp_m = crsp_m[(crsp_m.namedt <= crsp_m.date) & (crsp_m.date <= crsp_m.nameendt)]

# 1.5 Merge CRSP and COMPUSTAT by 8-digit cusip
# Compustat also provides historical CUSIP and TICKER in its Snapshot product,
# although historical identifiers are not available in their standard 
# fundamental data feed.
comp['cusip8'] = comp.cusip.str[0:8]
link = comp.merge(crsp_m, left_on='cusip8', right_on='ncusip')
link = link[['permno','gvkey', 'date']]
link = link.sort_values(['permno','gvkey','date'])
link = link.reset_index(drop=True)

# 1.1 - 1.5 all-in-one from SQL
cusip_link = conn.raw_sql("""
                          select distinct a.permno, gvkey, date
                          from
                          crsp.msf as a,
                          crsp.msenames as b,
                          comp.security as c
                          where
                          b.ncusip != '' and shrcd in (10,11)
                          and c.cusip != '' and excntry='USA'
                          and a.permno = b.permno
                          and NAMEDT <= date and date <= NAMEENDT
                          and b.ncusip = substr(c.cusip,1,8)
                          """)
cusip_link = cusip_link.sort_values(['permno','gvkey','date'])
cusip_link = cusip_link.reset_index(drop=True)

# Check if the merge method and sql all in one method equivalent:
cusip_link.equals(link) # Expect True


####################################
# Method 2: Link by CCM Link Table #
####################################

# 2.1 all-in-one from SQL
ccm_link = conn.raw_sql("""
                        select distinct a.permno, gvkey, date
                        from
                        crsp.msf as a,
                        crsp.msenames as b,
                        crsp.Ccmxpf_linktable as c
                        where
                        shrcd in (10,11)
                        and linktype in ('LU','LC')
                        and LINKPRIM in ('P','C')
                        and USEDFLAG=1
                        and a.permno = b.permno and b.permno = lpermno
                        and NAMEDT <= date and date <= NAMEENDT
                        and linkdt <= date and date <= coalesce(linkenddt, current_date)
                        """)

##################################
# Compare Method 1 and 2 #
##################################

# Get Companies with non-missing Asset or Sales Item
funda = conn.raw_sql("""
                     select distinct gvkey, datadate, 
                     sale, at
                     from
                     comp.funda
                     where
                     sale > 0 or at > 0
                     """)

funda = funda.sort_values(['gvkey', 'datadate']).reset_index(drop=True)

# Create 'year' variables from dates
funda['year'] = pd.to_datetime(funda.datadate).dt.year
ccm_link['year'] = pd.to_datetime(ccm_link.date).dt.year
cusip_link['year'] = pd.to_datetime(cusip_link.date).dt.year

# Use CCM LINK to Match on Calendar Year Base
ccm_l1 = pd.merge(ccm_link, funda, on=['gvkey', 'year'])
ccm_l2 = pd.merge(ccm_link, funda, on=['gvkey', 'year']).groupby('year')['gvkey'].nunique()

# Use CUSIP LINK to Match on Calendar Year Base
cusip_l1 = pd.merge(cusip_link, funda, on=['gvkey', 'year'])
cusip_l2 = pd.merge(cusip_link, funda, on=['gvkey', 'year']).groupby('year')['gvkey'].nunique()

merge_l = pd.merge(ccm_l2, cusip_l2, on=['year'], how='left', suffixes=('_ccm', '_cusip'))

merge_l.plot(kind='area', grid=True, stacked=False)
