#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 23 15:15:37 2020

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


markup: sale, cogs, ppegt, xsg&a, xlr, emp

PIRIC from FRED
"""

###################
# Connect to WRDS #
###################
conn = wrds.Connection()

# Get Companies with non-missing Asset or Sales Item
funda = conn.raw_sql("""
                     select gvkey, datadate, fyear, conm,
                     sale, at
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

funda = funda.sort_values(['gvkey', 'datadate']).reset_index(drop=True)

# Create 'year' variables from dates
#funda['year'] = pd.to_datetime(funda.datadate).dt.year # duplicates exist due to calendar and fiscal year mix
funda[funda.gvkey=='001000']
funda.duplicated(['gvkey', 'datadate']).sum()
funda.duplicated(['gvkey', 'fyear']).sum()
#funda.duplicated(['gvkey', 'year']).sum()

#funda_dup = funda[funda.duplicated(['gvkey', 'year'], keep=False)]

temp.duplicated(['gvkey', 'year']).sum()
temp_dup = temp[temp.duplicated(['gvkey', 'year'], keep=False)]

temp_dup.sort_values(['gvkey', 'year', 'score', 'name_ratio'],
                     ascending=[True, True, True, False],
                     inplace=True)

temp_drop_dup = temp_dup.drop_duplicates(subset=['gvkey', 'year'], keep='first')

set(temp_dup.gvkey) - set(temp_dup[temp_dup.score.isin([0,1,2,3,4,5,6])].gvkey)


temp2 = pd.merge(temp, funda, left_on=['gvkey', 'year'], right_on=['gvkey', 'fyear'])
temp2_dup = temp2[temp2.duplicated(['gvkey', 'fyear'], keep=False)]

"""
funda.query("(sale > 0 or at > 0) \
            and consol == 'C' \
            and indfmt == 'INDL' \
            and datafmt == 'STD' \
            and popsrc == 'D' \
            and curcd == 'USD' \
            and final == 'Y' \
            and fic == 'USA'")
"""

temp3 = temp2[temp2.duplicated(['gvkey', 'datadate'], keep=False)]

