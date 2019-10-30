#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 29 20:12:17 2019

@author: Pak Shing Ho

Input: a DataFrame
Output: return a corrected DataFrame

Some CUSIP digits are wrongly recorded by omitting one, two or three zeros in 
front and shifting all the remaining digits to the left.

To correct the wrongly shifted CUSIPs, I first shift the CUSIP digits to the
right and add "0"'s in front. Then take the first 8 digits and compare them to
the original 8-digit-CUSIP. If there're sets of intersection, this means there
are wrongly shifted CUSIPs. I collect this set of wrongly shifted CUSIPs and 
create a dictionary to map them back to the correct CUSIPs correspondingly.
"""

def cusipCorrection(DataFrame):
    data = DataFrame.copy()
    data['0cusip7'] = '0' + data['cusip'].str[0:7]
    data['00cusip6'] = '00' + data['cusip'].str[0:6]
    data['000cusip5'] = '000' + data['cusip'].str[0:5]

    # Create a column to store original CUSIP column:
    data['cusip_orig'] = data['cusip']
    
    # The list of CUSIPs that requires adding '0' in front:
    lst1=data['cusip'].unique()
    lst2=data['0cusip7'].unique()
    lst8 = set(lst1) & set(lst2)
    
    # The list of CUSIPs that requires adding '00' in front:
    lst11=data['cusip'].unique()
    lst22=data['00cusip6'].unique()
    lst88 = set(lst11) & set(lst22)
    
    # The list of CUSIPs that requires adding '000' in front:
    lst111=data['cusip'].unique()
    lst222=data['000cusip5'].unique()
    lst888 = set(lst111) & set(lst222)
    
    # Create dictionary mapping wrong CUSIPs to their corresponding correct CUSIPs:
    # one zero added in front:
    rcdict0 = {}
    for c in lst8:
        if len(set(data[data['0cusip7']==c]['cusip']))==1:
            #print(len(set(data[data['0cusip7']==c]['cusip']))==1) # check if wrongly shifted cusip-8-digit is unique
            _c = list(data[data['0cusip7']==c]['cusip'])[0]
        rcdict0[_c] = c
    
    # two zero added in front:
    lst88.remove('00030710')
    rcdict00 = {}
    for c in lst88:
        if len(set(data[data['00cusip6']==c]['cusip']))==1:
            #print(len(set(data[data['00cusip6']==c]['cusip']))==1) # check if wrongly shifted cusip-8-digit is unique
            _c = list(data[data['00cusip6']==c]['cusip'])[0]
        rcdict00[_c] = c

    # three zero added in front:
    lst888.remove('00030710')
    rcdict000 = {}
    for c in lst888:
        if len(set(data[data['000cusip5']==c]['cusip']))==1:
            #print(len(set(data[data['00cusip6']==c]['cusip']))==1) # check if wrongly shifted cusip-8-digit is unique
            _c = list(data[data['000cusip5']==c]['cusip'])[0]
        rcdict000[_c] = c
        
    # Some hand maps
    cusip6Dict = {"18772103":"01877210",
                  "01877230":"01877210",
                  "886309":"00088630"}
    
    data.drop(columns=['0cusip7', '00cusip6', '000cusip5'], inplace=True)
    
    # Merging all dictionaries above:
    cusipDict = {**rcdict0, **rcdict00, **rcdict000, **cusip6Dict}
    
    # Count total observations with wrong CUSIPs:
    counter = 0
    for d in cusipDict.keys():
        count = len(data[data.cusip==d])
        counter += count
    print('\nTotal number of distinct wrong CUSIP: ' + str(len(cusipDict)))
    print('\nToral wrong CUSIP observations: ' + str(counter))
    
    # Correction by assigning according to the dictionary 'cusipDict' constructed above:
    for d in cusipDict.keys():
        data.loc[data.cusip==d, 'cusip'] = cusipDict[d]
    
    # Check any wrong Cusip observation left again after assignment:
    print('Correction Done! Number of wrong CUSIP remain unassigned: ' 
          +  str(len(data[data.cusip.isin(cusipDict.keys())])))
    
    return data