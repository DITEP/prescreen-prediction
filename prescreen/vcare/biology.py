"""
fetching biology measurements
"""
import pandas as pd

from clintk.utils.connection import get_engine, sql2df
from datetime import timedelta
from bs4 import BeautifulSoup
from io import StringIO

import requests
import argparse


def fetch(url, header_path, id, ip, dbase, targets_table):
    """
    il suffit de concatener toutes les tables extraites pour ensuite les fold

    url : str
        url to the location of biology files

    header_path : str
        path to csv file containing header

    id : str
        login to the sql database

    ip : str
        ip adress to the sql server

    dbase : str
        name of the database on the given server

    targets_table : str @TODO remplacer par df_targets
        name of the table containing targets information

    Returns
    -------
    """
    # url = 'http://esimbad/testGSAV7/reslabo?FENID=resLaboPatDitep&NIP={}' \
    #       '&STARTDATE={}&ENDDATE={}'

    # header_path = '~/workspace/data/biology/header.csv'
    # constant names specific to our database
    KEY1 = 'id'
    KEY2 = 'NIP'
    C1J1 = 'C1J1'

    header = pd.read_csv(header_path, sep=';', encoding='latin1').columns


    engine = get_engine(id, ip, dbase)

    df_ids = sql2df(engine, targets_table)[[KEY1, 'nip', C1J1]]
    df_ids.rename({'nip': KEY2}, inplace=True, axis=1)
    df_ids['patient_id'] = df_ids[KEY1]

    cols = [KEY2, 'Analyse', 'Resultat', 'Date prelvt']
    df_res = pd.DataFrame(data=None, columns=cols)

    for index, row in df_ids.iterrows():
        nip = row[KEY2].replace(' ', '')
        patient_id = row['patient_id']
        c1j1_date = row[C1J1].date()
        start_date = c1j1_date - timedelta(weeks=8)

        c1j1 = str(c1j1_date).replace('-', '')
        start = str(start_date).replace('-', '')

        req = requests.get(url.format(nip, start, c1j1))
        values = BeautifulSoup(req.content, 'html.parser').body.text

        new_df = pd.read_csv(StringIO(values), sep=';', header=None,
                             index_col=False, names=header)
        new_df = new_df.loc[:, cols + ['LC']] # remove LC

        # normalize nip
        new_df[KEY2] = row[KEY2]
        # new_df[KEY2] = new_df[KEY2].map(str)
        # new_df[KEY2] = [nip[:4] + '-' + nip[4:] for nip in new_df[KEY2]]

        new_df.drop('LC', axis=1, inplace=True)

        df_res = pd.concat([df_res, new_df], axis=0,
                           sort=False, ignore_index=True)

    return df_res


def fetch_and_fold(url, header, id, ip, db, targets):
    key1, key2, date = 'patient_id', 'nip', 'date'
    # engine for sql connection
    engine = get_engine(id, ip, db)

    # fetching targets table
    df_targets = sql2df(engine, targets).loc[:, ['nip', 'id', 'C1J1']]
    df_targets.loc[:, 'C1J1'] = pd.to_datetime(df_targets['C1J1'],
                                               format='%Y-%m-%d',
                                               unit='D')

    # url = 'http://esimbad/testGSAV7/reslabo?FENID=resLaboPatDitep&NIP={}' \
    #       '&STARTDATE={}&ENDDATE={}'
    url = url
    header_path = header


    df_bio = fetch(url, header_path,id, ip, db, targets)
    # parse_dates
    df_bio['Date prelvt'] = pd.to_datetime(df_bio['Date prelvt'],
                                           format='%Y%m%d').dt.date

    df_bio.rename({'Date prelvt': date, 'Analyse': 'feature',
                   'Resultat': 'value'}, inplace=True, axis=1)

    # joining with targets
    # df_bio = df_bio.merge(df_targets, on=None, left_on='NIP',
    #                       right_on='nip').drop('NIP', axis=1)
    df_bio = df_targets.merge(df_bio, left_on='nip', right_on='NIP')
    df_bio.drop('NIP', axis=1, inplace=True)

    df_bio['C1J1'] = pd.to_datetime(df_bio['C1J1'],
                                           format='%Y%m%d').dt.date
    df_bio = df_bio[df_bio[date] <= df_bio['C1J1']] # ?

    df_bio.rename({'id': 'patient_id'}, axis=1, inplace=True)
    df_bio.drop('C1J1', axis=1, inplace=True)

    #removing non float values
    df_bio['value'] = pd.to_numeric(df_bio.loc[:, 'value'], errors='coerce',
                                    downcast='float')

    # df_bio already folded

    print('done')

    return df_bio



def main_fetch_and_fold():
    description = 'Folding biology measures from Ventura Care'
    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('--url', '-u',
                        help='url to where measures are stored')
    parser.add_argument('--header', '-H',
                        help='path to the header file to read csv')
    parser.add_argument('--id', '-I',
                        help='id to connect to sql server')
    parser.add_argument('--ip', '-a',
                        help='ip adress of the sql server')
    parser.add_argument('--db', '-d',
                        help='name of the database on the sql server')
    parser.add_argument('--targets', '-t',
                        help='name of the table containing targets on the db')
    parser.add_argument('--output', '-o',
                        help='output path to write the folded result')

    args = parser.parse_args()

    # column names for merged dataframe

    df_bio = fetch_and_fold(args.url, args.header, args.id, args.ip,
                            args.db, args.targets)

    # df_bio already folded
    output = args.output
    df_bio.to_csv(output, encoding='utf-8', sep=';')

    print('done')

    return df_bio


if __name__ == "__main__":
    main_fetch_and_fold()



def read_biology(url, header):
    """ reads biology measures for one patient

    Parameters
    ----------
    url : str
        url containing file

    header : list
        column names
    Returns
    -------
    dict
        keys are name of the biologic parameter, value are the measures
    """
    req = requests.get(url)
    values = BeautifulSoup(req.content, 'html.parser').body.text

    df = pd.read_csv(StringIO(values), sep=';', header=None,
                     index_col=False, names=header)
    res_dic = {}
    for i, row in df.iterrows():
        key = row['Analyse'].replace(' ', '_') # ou analyse?
        value = row['Resultat']

        res_dic[key] = value

    return res_dic


