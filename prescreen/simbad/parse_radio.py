"""
script to parse radiology reports from  cr_sfditep-2012.xslsx

They are denoted as CR


"""
import pandas as pd

from clintk.text_parser.parser import ReportsParser
from clintk.utils.connection import get_engine, sql2df

import argparse


def parse_cr(path, id, ip, db, targets, n_reports):
    PATH = path

    engine = get_engine(id, ip, db)
    # fetching targets
    df_targets = sql2df(engine, targets)

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
    # df = df[df['date'] <= (df['DATE_SIGN_OK'] + datetime.timedelta(weeks=8))]

    # normalize nip
    df['nip'] = df['N° Dossier patient IGR'].astype(str) + df['LC']
    df['nip'] = df.loc[:, 'nip'] \
        .apply(lambda s: s[:4] + '-' + s[4:-2] + ' ' + s[-2:])

    df.drop(['N° Dossier patient IGR', 'LC', 'NOCET', 'SIGLE_ETUDE',
             'LIBELLE_TYPE_ETUDE', 'NUM CR', 'CR RESP'], axis=1, inplace=True)

    # taking only the first report
    df = df.groupby('nip', as_index=False).agg('first')

    df = df.merge(df_targets, on='nip')

    # removing useless tags (blocks parsing)
    df['value'] = df.loc[:, 'value'].apply(lambda s: \
        str(s).replace('<u>', '').replace('</u>', ''))

    sections = ['resultats', 'resultat', 'evaluation des cibles', 'conclusion']
    parser = ReportsParser(headers='b', is_html=False, col_name='value',
                           sections=sections)

    df['value'] = parser.transform(df)

    # dropping empty rows
    df = df[df['value'] != '']

    df['feature'] = ['rad'] * df.shape[0]

    df = df.loc[:, ['nip', 'id', 'feature', 'value', 'date']]

    return df


def main_parse_cr():
    description = 'Folding radiology reports from Simbad'
    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('-p', '--path',
                        help='path to file that contains the reports')
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
    parser.add_argument('-n', '--nb',
                        help='number of reports to fetch')

    args = parser.parse_args()

    # getting variables from args
    df = parse_cr(args.path, args.id, args.ip, args.db, args.targets, args.nb)

    # output = '/home/v_charvet/workspace/data/features/simbad/radiology_v{}.csv'
    output = args.output
    df.to_csv(output, sep=';', encoding='utf-8')

    print('done')

    return 0


if __name__ == "__main__":
    main_parse_cr()
