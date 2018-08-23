"""
script to parse reports from cr_sfditep-2012.xslsx

parses CC and RH reports

after parsing them, they need to be concatenated to the ones from vcare to
train a tfidf model

"""
import pandas as pd

from clintk.text_parser.parser import ReportsParser
from clintk.utils.connection import get_engine, sql2df

import argparse


def fetch_and_fold(path, ID, ip, db, targets, n_reports):
    """ function to fetch reports from simbad data

    Parameters
    ----------
    For definition of parameters, see arguments in `main_fetch_and_fold`
    """

    engine = get_engine(ID, ip, db)
    # fetching targets
    df_targets = sql2df(engine, targets)

    # fetching reports
    df = pd.read_excel(path)

    # normalize nip
    df['nip'] = df['N° Dossier patient IGR'].astype(str) + df['LC']
    df['nip'] = df.loc[:, 'nip'] \
        .apply(lambda s: s[:4] + '-' + s[4:-2] + ' ' + s[-2:])

    df.drop(['N° Dossier patient IGR', 'LC', 'NOCET', 'SIGLE_ETUDE',
             'LIBELLE_TYPE_ETUDE', 'NUM CR', 'CR RESP'], axis=1, inplace=True)

    df.rename(columns={'CR DATE': 'date', 'text CR': 'value'}, inplace=True)

    df_cc = df[df['CR NAT'] == 'CC']
    df_rh = df[df['CR NAT'] == 'RH']

    # taking only the first for each patient
    df_cc.dropna(inplace=True)
    df_cc.drop_duplicates(subset=['value'], inplace=True)

    # taking only the first reports
    group_dict = {'date': 'first', 'DATE_SIGN_OK': 'last',
                  'value': lambda g: ' '.join(g[:n_reports])}
    df_cc = df_cc.groupby('nip', as_index=False).agg(group_dict)

    # filter uninformative reports and taking the first
    df_rh = df_rh[~(df_rh['value'].str.match('Examen du', na=False))]
    df_rh.dropna(inplace=True)
    df_rh.drop_duplicates(subset=['value'], inplace=True)

    # taking only the first reports
    df_rh = df_rh.groupby('nip', as_index=False).agg(group_dict)

    df = pd.concat([df_cc, df_rh], ignore_index=True)

    # keep only date in 'date columns'
    df['date'] = df.loc[:, 'date'].dt.date
    df['DATE_SIGN_OK'] = df.loc[:, 'DATE_SIGN_OK'].dt.date

    # removing useless tags (blocks parsing)
    df['value'] = df.loc[:, 'value'].apply(lambda s: \
        str(s).replace('<u>', '').replace('</u>', ''))

    # filter date
    # df = df[df['date'] <= (df['DATE_SIGN_OK'] + datetime.timedelta(weeks=8))]

    df = df.merge(df_targets, on='nip')

    parser = ReportsParser(headers='b', is_html=False, norm=False,
                           n_jobs=-1, col_name='value')

    df['value'] = parser.transform(df)

    df['feature'] = ['report']*df.shape[0]

    df = df.loc[:, ['nip', 'id', 'feature', 'value', 'date']]
    df = df[df['value'] != '']
    df.drop_duplicates(inplace=True)

    return df


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

    # getting variables from args
    df = fetch_and_fold(args.path, args.id, args.ip, args.db, args.targets,
                        args.nb)

    output = args.output
    df.to_csv(output, sep=';', encoding='utf-8')

    print('done')

    return 0



if __name__ == "__main__":
    main_fetch_and_fold()
