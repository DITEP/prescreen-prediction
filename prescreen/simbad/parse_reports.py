"""
script to parse reports from cr_sfditep-2012.xslsx

parses CC and RH reports

after parsing them, they need to be concatenated to the ones from vcare to
train a tfidf model

"""
import pandas as pd

from preprocessing.html_parser.parser import ReportsParser
from preprocessing.utils.connection import get_engine, sql2df

import datetime
import argparse


def parse_cc():
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
    df = pd.read_excel(PATH)

    df = df[df['CR NAT'] == 'CC']
    df.rename(columns={'CR DATE': 'date', 'text CR': 'value'}, inplace=True)

    # keep only date in 'date columns'
    df['date'] = df.loc[:, 'date'].dt.date
    df['DATE_SIGN_OK'] = df.loc[:, 'DATE_SIGN_OK'].dt.date

    # removing useless tags (blocks parsing)
    df['value'] = df.loc[:, 'value'].apply(lambda s: \
        str(s).replace('<u>', '').replace('</u>', ''))

    # filter date
    df = df[df['date'] <= (df['DATE_SIGN_OK'] + datetime.timedelta(weeks=6)) ]
    # normalize nip
    df['nip'] = df['N° Dossier patient IGR'].astype(str) + df['LC']
    df['nip'] = df.loc[:, 'nip'] \
        .apply(lambda s: s[:4] + '-' + s[4:-2] + ' ' + s[-2:])

    df.drop(['N° Dossier patient IGR', 'LC', 'NOCET', 'SIGLE_ETUDE',
             'LIBELLE_TYPE_ETUDE', 'NUM CR', 'CR RESP'], axis=1, inplace=True)

    df = df.merge(df_targets, on='nip')

    parser = ReportsParser(headers='b', is_html=False,
                           n_jobs=1, col_name='value')

    df['value'] = parser.transform(df)

    df['feature'] = ['report']*df.shape[0]

    df = df.loc[:, ['nip', 'id', 'feature', 'value', 'date']]
    df = df[df['value'] != '']
    df.drop_duplicates(inplace=True)

    output = args.o
    df.to_csv(output.format(VERSION), sep=';', encoding='utf-8')

    print('done')

    return df


def parse_rh():
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
    df = pd.read_excel(PATH)

    df = df[df['CR NAT'] == 'RH']
    df.rename(columns={'CR DATE': 'date', 'text CR': 'value'}, inplace=True)

    # keep only date in 'date columns'
    df['date'] = df.loc[:, 'date'].dt.date
    df['DATE_SIGN_OK'] = df.loc[:, 'DATE_SIGN_OK'].dt.date

    # filter uninformative reports
    df = df[~(df['value'].str.match('Examen du', na=False))]
    # filter by date
    df = df[df['date'] <= (df['DATE_SIGN_OK'] + datetime.timedelta(weeks=6))]

    # removing useless tags (blocks parsing)
    df['value'] = df.loc[:, 'value'].apply(lambda s: \
        str(s).replace('<u>', '').replace('</u>', ''))

    # normalize nip
    df['nip'] = df['N° Dossier patient IGR'].astype(str) + df['LC']
    df['nip'] = df.loc[:, 'nip'] \
        .apply(lambda s: s[:4] + '-' + s[4:-2] + ' ' + s[-2:])

    df.drop(['N° Dossier patient IGR', 'LC', 'NOCET', 'SIGLE_ETUDE',
             'LIBELLE_TYPE_ETUDE', 'NUM CR', 'CR RESP'], axis=1, inplace=True)

    df = df.merge(df_targets, on='nip')

    sections = ['liste rendez vous prevus', 'bilan biologique',
               'medecin referent', 'dicte le', 'dicte par']
    parser = ReportsParser(headers='b', remove_sections=sections,
                           is_html=False,
                           col_name='value')

    df['value'] = parser.transform(df)

    df['feature'] = ['report']*df.shape[0]

    df = df.loc[:, ['nip', 'id', 'feature', 'value', 'date']]
    df = df[df['value'] != '']
    df.drop_duplicates(inplace=True)

    output = args.o
    df.to_csv(output.format(VERSION), sep=';', encoding='utf-8')

    print('done')

    return df


if __name__ == "__main__":
    parse_cc()
