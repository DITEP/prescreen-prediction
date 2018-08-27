"""
script to load SF patients that are not in VCare into the database

"""
import pandas as pd

from clintk.utils.connection import get_engine

import argparse


def load():
    # patient_id to start iterate
    description = 'Load targets from file into the sql server'
    parser = argparse.ArgumentParser(description=description)

    parser.add_argument(['-p', '--path'],
                        help='path to file that contains the patients info')
    parser.add_argument(['--id', '-I'],
                        help='id to connect to sql server')
    parser.add_argument(['--ip', '-a'],
                        help='ip adress of the sql server')
    parser.add_argument(['--db', '-d'],
                        help='name of the database on the sql server')
    parser.add_argument(['--output', '-o'],
                        help='name of the new table on the server')
    # PATH = "/home/v_charvet/workspace/data/cr/cr_rad_tronc.xlsx"

    args = parser.parse_args()
    LAST_ID = 3300

    engine = get_engine(args.I, args.a, args.d)

    path = args.p
    # input = 'data/cr_sfditep_2012.xlsx'
    df = pd.read_excel(path).loc[:, ['N° Dossier patient IGR', 'LC',
                                     'DATE_SIGN_OK']]

    df.drop_duplicates(inplace=True)

    # normalize nip
    df['nip'] = df['N° Dossier patient IGR'].astype(str) + df['LC']
    df['nip'] = df.loc[:, 'nip']\
        .apply(lambda s: s[:4] + '-' + s[4:-2] + ' ' + s[-2:])

    new_ids = [LAST_ID + i for i in range(df.shape[0])]
    screenfail = [1] * df.shape[0]

    target = {'nip': df.loc[:, 'nip'],
              'id': new_ids,
              'prescreen': df['DATE_SIGN_OK'],
              'screenfail': screenfail}

    df_target = pd.DataFrame(target)

    df_target.to_sql(args.o, engine, if_exists='replace')

    print('done')

    return df_target


if __name__ == "__main__":
    load()
