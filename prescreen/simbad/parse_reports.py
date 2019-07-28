# -*- coding: utf-8 -*-
"""
Created on Mon Jul 22 22:16:33 2019

@author: Valentin Charvet, Loic Verlingue
"""

"""
script to parse reports from cr_sfditep-2012.xslsx
parses CC and RH reports
"""
import pandas as pd
import re

from clintk.text_parser.parser import ReportsParser
from clintk.utils.connection import get_engine, sql2df

import argparse

# n_reports : no use
def fetch_and_fold(path, engine, targets, n_reports):
    """ function to fetch reports from simbad data
    Parameters
    ----------
    For definition of parameters, see arguments in `main_fetch_and_fold`
    """

    # fetching targets
    df_targets = sql2df(engine, targets)

    # fetching reports
    df = pd.read_excel(path)
#    df.shape
    
    # normalize nip
    df['nip'] = df['N° Dossier patient IGR'].astype(str) + df['LC']
    df['nip'] = df.loc[:, 'nip'] \
        .apply(lambda s: s[:4] + '-' + s[4:-2] + ' ' + s[-2:])

    len(df['nip'].unique()) # only 714?
  #  df.nip.value_counts().head()
  #  df.nip.value_counts().describe()
    
    df.drop(['N° Dossier patient IGR', 'LC', 'NOCET', 'SIGLE_ETUDE',
             'LIBELLE_TYPE_ETUDE', 'NUM CR', 'CR RESP'], axis=1, inplace=True)

    df.rename(columns={'CR DATE': 'date', 'text CR': 'value'}, inplace=True)

    # keep only date in 'date columns'
    df['date'] = df.loc[:, 'date'].dt.date
    df['DATE_SIGN_OK'] = df.loc[:, 'DATE_SIGN_OK'].dt.date
#    len(df['DATE_SIGN_OK'].unique())

    # taking only consultation reports
    df = df[df['CR NAT'] == 'CC']
    
    # removing NAs and dups N°1
    df.dropna(inplace=True)
    df.drop_duplicates(subset=['value'], inplace=True)
   
    # mask to get only the first one
    ############# LV: check
#    mask = (df['date'] == df['DATE_SIGN_OK'])
#    df = df[mask]
    # search for 'inclusion' term and others relevant ones in values
    
    IncL=[]
    for line in df.index:
       if re.search("inclu|screening|protocole|consent",df.loc[line,'value']): 
            IncL.append(line)
    len(IncL)
    #df.loc[IncL[10],'value']
    
    # create dfinc
    dfinc=df.loc[IncL,] 
#   len(dfinc['nip'].unique()) # 705
   
    # taking only the first reports for each patient for df1 and dfinc
        # LV: ? does it take only first reports (from date) or concatenate duplicated reports of patients?
    
    group_dict = {'date': 'first', 'DATE_SIGN_OK': 'last',
                  'value': lambda g: ' '.join(g)}
    
    # for df with date sign
    mask = (df['date'] == df['DATE_SIGN_OK'])
    df1 = df[mask]
    df1 = df1.groupby('nip', as_index=False).agg(group_dict)
    len(df1['nip']) # 713   
    
    # for dfinc
   # dfinc = dfinc.groupby('nip', as_index=False).agg(group_dict)
    #dfinc.loc[3,'value']
    dinc = dfinc.groupby('nip', as_index=False).agg(group_dict)
   
    
    # identify 1st reports not retained in dfinc
    #
    not df1.nip.isin(dfinc.nip)
    ISIN=~df1.nip.isin(dfinc.nip)
    ISIN.value_counts()
    df1[ISIN]
    # concatenate both df1 and dfinc
    dfall = pd.concat([dfinc, df1[ISIN]], axis=0, ignore_index=True)
    #dfall.shape
#    dfall.nip.value_counts().describe()
   
    '''
    NIPlacks=[]
    for NIPindex in df1.index:
        if df1.nip[NIPindex] not in dfinc.nip:
            NIPlacks.append(NIPindex)
    len(NIPlacks) # why all = 713?
    df1[NIPlacks]
    #df1.loc[NIPlacks[len(NIPlacks)-1],"value"]
    
    # concatenate both df1 and dfinc
    dfall = pd.concat([dfinc, df1], axis=0, ignore_index=True)
   # dfall.head()
    
   pd.concat(g for _, g in df.groupby("nip") if len(g) > 1)
    # why still some dupllicates???
    '''
   
    # # filter uninformative reports and taking the first
    # df_rh = df_rh[~(df_rh['value'].str.match('Examen du', na=False))]
    # df_rh.dropna(inplace=True)
    # df_rh.drop_duplicates(subset=['value'], inplace=True)
    #
    # # taking only the first reports
    # df_rh = df_rh.groupby('nip', as_index=False).agg(group_dict)
    #
    # df = pd.concat([df_cc, df_rh], ignore_index=True)


    # removing useless tags (blocks parsing)
    dfall['value'] = dfall.loc[:, 'value'].apply(lambda s: \
        str(s).replace('<u>', '').replace('</u>', ''))

    # filter date
    # df = df[df['date'] <= (df['DATE_SIGN_OK'] + datetime.timedelta(weeks=8))]

    dfall = dfall.merge(df_targets, on='nip')
    
    # ReportParser function is in clintk/text_parser/parser_utils.py
    parser = ReportsParser(headers='b', is_html=False, norm=False,
                           n_jobs=-1, col_name='value')

    dfall['value'] = parser.transform(dfall)

    dfall['feature'] = ['report']*dfall.shape[0]

    dfall = dfall.loc[:, ['nip', 'id', 'feature', 'value', 'date']]
    
    # I would put those 2 functions in line 54 : removing NAs and dups N°1
  #  dfall = dfall[dfall['value'] != ''] # are tere some?
  #  dfall.drop_duplicates(inplace=True) # usefull? are there some?

    return dfall


def main_fetch_and_fold():
    description = 'Folding cc text reports from Simbad'
    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('-p', '--path',
                        help='path to file that contains the reports')
    parser.add_argument('--id', '-I',
                        help='id to connect to sql server')
    parser.add_argument('--ip', '-a',
                        help='ip address of the sql server')
    parser.add_argument('--db', '-d',
                        help='name of the database on the sql server')
    parser.add_argument('--targets', '-t',
                        help='name of the table containing targets on the db')
    parser.add_argument('--output', '-o',
                        help='output path to write the folded result')
    parser.add_argument('-n', '--nb',
                        help='number of reports to fetch', type=int)

    args = parser.parse_args()

    engine = get_engine(args.id, args.ip, args.db)

    # getting variables from args
    df = fetch_and_fold(args.path, engine, args.targets, args.nb)

    output = args.output
    df.to_csv(output, sep=';', encoding='utf-8')

    print('done')

    return 0



if __name__ == "__main__":
    main_fetch_and_fold()
