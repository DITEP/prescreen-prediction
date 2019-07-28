# -*- coding: utf-8 -*-
"""
Created on Sun Jul 28 08:01:06 2019

@author: Valentin Charvet, Loic Verlingue

from /prescreen/evaluation/fetch_reports.py
strange: source file ditep_inclus.xlsx DATE_DEBUT is 1 months less than expected!! changed in xlsx file
"""

"""
fetches consultation reports from validation cohort for DITEP Phase 1/2 trials
"""

import pandas as pd
from prescreen.simbad.ScreenCons import ScreenCons

def get_frames(path):
    
    ## DITEP OK 
    ditep_inclus = pd.read_excel(path + '/ditep_inclus.xlsx')
    ditep_inclus.rename(columns={'NOIGR':'nip', 'DATE CR': 'date', 'CR': 'value'}, inplace=True)
       
    ditep_inclus['date'] = ditep_inclus.loc[:, 'date'].dt.date
    ditep_inclus['DATE_SIGN_OK'] = ditep_inclus.loc[:, 'DATE_SIGN_OK'].dt.date
    #len(ditep_inclus['nip'].unique())
    ###
    df_inclus=ScreenCons(ditep_inclus)
    df_inclus=df_inclus.assign(screenfail=0) # check name screenfail ?
    #df_inclus['value'][4] # check 4 -> comment in ScreenCons
    #df_inclus.shape
    
    '''
    mask_consult = (ditep_inclus['date'] == ditep_inclus['DATE_SIGN_OK'])
    mask_consult.value_counts()
    ditep_inclus = ditep_inclus[mask_consult]
    ditep_inclus.drop(['DATE CR', 'DATE SIGN_OK'], axis=1, inplace=True)

    ditep_inclus = ditep_inclus.groupby('nip', as_index=False)\
        .agg(lambda s: ' '.join(s))

    '''
    
    ## DITEP screenfail
  
    ditep_sf =pd.read_excel( path + '/ditep_sf.xlsx')
    ditep_sf.rename(columns={'NOIGR':'nip', 'DATE CR': 'date', 'CR': 'value'}, inplace=True)
       
    ditep_sf['date'] = ditep_sf.loc[:, 'date'].dt.date
    ditep_sf['DATE_SIGN_OK'] = ditep_sf.loc[:, 'DATE_SIGN_OK'].dt.date
    
    df_sf=ScreenCons(ditep_sf)
    df_sf=df_sf.assign(screenfail=1) # check ok name screenfail ?
     
    '''
    mask_consult = (ditep_sf['DATE CR'] == ditep_sf['DATE SIGN_OK'])

    ditep_sf = ditep_sf[mask_consult]
    ditep_sf.drop(['DATE CR', 'DATE SIGN_OK'], axis=1, inplace=True)

    ditep_sf = ditep_sf.groupby('nip', as_index=False) \
        .agg(lambda s: ' '.join(s))
    '''
    
    dfall_p2 = pd.concat([df_inclus, df_sf], axis=0, ignore_index=True)
    
    '''
    ## poumons inclus
    path = base_path + '/poumons_inclusion.csv'

    poumons = pd.read_csv(path, sep=';', encoding='utf-8')\
        .drop('Unnamed: 0', axis=1)

    mask_consult = (poumons['DATE CR'] == poumons['DATE_SIGN_OK'])

    poumons = poumons[mask_consult]
    poumons.drop(['DATE CR', 'DATE_SIGN_OK'], axis=1, inplace=True)

    poumons = poumons.groupby('nip', as_index=False) \
        .agg(lambda s: ' '.join(s))

    # poumons SF
    path = base_path + '/poumons_sf.csv'

    poumons_sf = pd.read_csv(path, sep=';', encoding='utf-8')\
        .drop('Unnamed: 0', axis=1)

    mask_consult = (poumons_sf['DATE CR'] == poumons_sf['DATE_SIGN_OK'])

    poumons_sf = poumons_sf[mask_consult]
    poumons_sf.drop(['DATE CR', 'DATE_SIGN_OK'], axis=1, inplace=True)

    poumons_sf = poumons_sf.groupby('nip', as_index=False) \
        .agg(lambda s: ' '.join(s))
    '''
    
#    return ditep_inclus, ditep_sf, poumons, poumons_sf
    return dfall_p2
