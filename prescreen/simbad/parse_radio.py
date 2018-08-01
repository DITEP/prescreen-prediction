"""
script to parse radiology reports from  cr_sfditep-2012.xslsx

They are denoted as CR


"""
import pandas as pd

from preprocessing.html_parser.parser import ReportsParser
from preprocessing.utils.connection import get_engine, sql2df

import datetime
import argparse


def parse_cr():
    description = 'Folding radiology reports from Simbad'
    parser = argparse.ArgumentParser(description=description)

    parser.add_argument(['--version', '-V'],
                        help='version number to keep track of files')
    parser.add_argument(['-p', '--path'],
                        help='path to file that contains the reports')
    parser.add_argument(['--id', '-I'],
                        help='id to connect to sql server')
    parser.add_argument(['--ip', '-a'],
                        help='ip adress of the sql server')
    parser.add_argument(['--db', '-d'],
                        help='name of the database on the sql server')
    parser.add_argument(['--targets', '-t'],
                        help='name of the table containing targets on the db')
    parser.add_argument(['--output', '-o'],
                        help='output path to write the folded result')

    args = parser.parse_args()

    # getting variables from args
    VERSION = args.V
    PATH = args.p

    engine = get_engine(args.I, args.a, args.d)
    # fetching targets
    df_targets = sql2df(engine, args.t)

    # fetching reports
    # PATH = 'data/cr_sfditep_2012.xlsx'
    df = pd.read_excel(PATH)

    df = df[df['CR NAT'] == 'CR']
    df.rename(columns={'CR DATE': 'date', 'text CR': 'value'}, inplace=True)

    # keep only date in 'date columns'
    df['date'] = df.loc[:, 'date'].dt.date
    df['DATE_SIGN_OK'] = df.loc[:, 'DATE_SIGN_OK'].dt.date

    # remove useless reports
    df = df[~(df['value'].str.match('Examen du', na=False))]

    # filter by date
    df = df[df['date'] <= (df['DATE_SIGN_OK'] + datetime.timedelta(weeks=6))]

    # normalize nip
    df['nip'] = df['N° Dossier patient IGR'].astype(str) + df['LC']
    df['nip'] = df.loc[:, 'nip'] \
        .apply(lambda s: s[:4] + '-' + s[4:-2] + ' ' + s[-2:])

    df.drop(['N° Dossier patient IGR', 'LC', 'NOCET', 'SIGLE_ETUDE',
             'LIBELLE_TYPE_ETUDE', 'NUM CR', 'CR RESP'], axis=1, inplace=True)

    df = df.merge(df_targets, on='nip')

    # removing useless tags (blocks parsing)
    df['value'] = df.loc[:, 'value'].apply(lambda s: \
        str(s).replace('<u>', '').replace('</u>', ''))

    sections = ['critere d evaluation', 'nom du protocole']
    parser = ReportsParser(headers='b', is_html=False, col_name='value',
                           remove_sections=sections)

    df['value'] = parser.transform(df)

    # dropping empty rows
    df = df[df['value'] != '']

    df['feature'] = ['rad']*df.shape[0]

    df = df.loc[:, ['nip', 'id', 'feature', 'value', 'date']]

    # output = '/home/v_charvet/workspace/data/features/simbad/radiology_v{}.csv'
    output = args.o
    df.to_csv(output.format(VERSION), sep=';', encoding='utf-8')

    print('done')

    return df


if __name__ == "__main__":
    parse_cr()
