# -*- coding: utf-8 -*-
"""
Created on Sun Jul 28 17:10:36 2019

@author: Loic VERLINGUE
"""
import re
import pandas as pd
import numpy as np

def ScreenCons(df):
     '''
     Select the screening consultation reports from a list of patients' report given a date
     
    Parameters
    ----------
    df : data frame with at least the following columns names:
        value : reports
        nip : patient id
        DATE_SIGN_OK : date patient has signed
    
    Returns
    -------
    Only screening consultation reports
    
     '''
     
    #############
    #get only the consultation report at inclusion
    ############# LV: check
        
    # option 1
    # search for 'inclusion' term and others relevant ones in values
        
     IncL=[]
     for line in df.index:
         if re.search("inclusion|screening|consentement",df.loc[line,'value']): 
             IncL.append(line)
     len(IncL)
     #df.loc[IncL[10],'value']
        
     NOIncL=[]
     for line in df.index:
        if re.search("failure|screenfail|C1J1|C1 J1|J1C1",df.loc[line,'value']): 
            NOIncL.append(line)
     len(NOIncL)
    
     # create dfinc
     dfinc=df.loc[~np.isin(IncL,NOIncL),:] 
     #   len(dfinc['nip'].unique()) # 157
     # dfinc.tail() 
     mask=dfinc['date'] <= dfinc['DATE_SIGN_OK']
     dfinc=dfinc[mask]
             
     # aggregate reports for single patients  
         
     group_dict = {'date': 'first', 'DATE_SIGN_OK': 'last',
                       'value': lambda g: ' '.join(g)}
         
     # for dfinc
     dfinc = dfinc.groupby('nip', as_index=False).agg(group_dict)
     #   (dfinc['date']==dfinc['DATE_SIGN_OK']).value_counts()
         
     #   dfinc['value'][dfinc.shape[0]-6]
        
     # for df with date sign
     mask = (df['date'] == df['DATE_SIGN_OK'])
     df1 = df[mask]
         
     #    df1.shape
     IncL=[]
     for line in df1.index:
         if re.search("inclu|screening|consent",df1.loc[line,'value']): 
            IncL.append(line)
     len(IncL)
             
     df1=df1.loc[IncL,:] 
     #    df1.shape
     #   len(df1.nip.unique())
        
     df1 = df1.groupby('nip', as_index=False).agg(group_dict)
     df1['value'][df1.shape[0]-10]
     #len(df1['nip'].unique()) # 708
             
     # identify 1st reports not retained in dfinc
         
     NotInBoth=~dfinc.nip.isin(df1.nip)
     NotInBoth.value_counts()
        
     # dfinc[NotInBoth].tail()
        
     # concatenate both df1 and dfinc
     dfall = pd.concat([dfinc[NotInBoth], df1], axis=0, ignore_index=True)
     
     # to add:
     # discard too short reports
     # check too long reports
     return dfall
