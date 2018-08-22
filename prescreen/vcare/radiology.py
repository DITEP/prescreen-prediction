"""
fetching and processing radiology reports

Two data inputs needed: xls file that contains the contents of the reports and
the table containing additionnal patients information from SQL server
"""
import pandas as pd

from clintk.utils.connection import get_engine, sql2df
from clintk.utils.unfold import transform_and_label
from clintk.utils.fold import Folder
from clintk.text_parser.parser import ReportsParser
from sklearn.pipeline import Pipeline
from sklearn.feature_extraction.text import TfidfVectorizer

import argparse


def fetch_and_fold(path, id, ip, db, targets, n_reports):
    """ function to fetch radiology reports from vcare database

       Parameters
       ----------
       For definition of parameters, see arguments in `main_fetch_and_fold`
    """
    engine = get_engine(id, ip, db)

    key1, key2, date = 'patient_id', 'nip', 'date'

    # fetching targets table
    df_targets = sql2df(engine, targets).loc[:, [key2, 'id', 'C1J1']]
    df_targets.loc[:, 'C1J1'] = pd.to_datetime(df_targets['C1J1'],
                                               format='%Y-%m-%d',
                                               unit='D')

    df_rad = pd.read_excel(path, sep=';', usecols=7, parse_dates=[1, 2, 5])

    # filter SESSION and keep prescreen values
    mask = df_rad['SESSION'].isin(['NUM', 'PDF'])

    # mask_prescreen = df_rad['OBSERVATION DATE'] == 'Before Date'
    df_rad = df_rad[~mask]  # [mask_prescreen]

    df_rad['CR_DATE'] = pd.to_datetime(df_rad['CR_DATE'], format='%Y%m%d')

    # remove useless rows
    df_rad = df_rad[~(df_rad['CONTENU_CR'].str.match('Examen du', na=False))]

    df_rad.rename({'CR_DATE': date}, axis=1, inplace=True)

    df_rad = df_rad.merge(df_targets, on=None, left_on='Nip',
                          right_on='nip').drop('Nip', axis=1)

    df_rad['patient_id'] = df_rad['id']
    df_rad.drop('id', axis=1, inplace=True)

    folder = Folder(key1, key2, ['CONTENU_CR'], date, n_jobs=-1)

    rad_folded = folder.fold(df_rad)

    # keeping only first report
    rad_folded = rad_folded.groupby(key1, as_index=False).agg('first')

    # removing useless tags (blocks parsing)
    rad_folded['value'] = rad_folded.loc[:, 'value'].apply(lambda s: \
                                                               str(s).replace(
                                                                   '<u>',
                                                                   '').replace(
                                                                   '</u>', ''))

    sections = ['resultats', 'resultat', 'evaluation des cibles', 'conclusion']
    parser = ReportsParser(headers='b', is_html=False, col_name='value',
                           sections=sections)

    rad_folded['value'] = parser.transform(rad_folded)

    rad_folded = rad_folded[rad_folded['value'] != '']
    rad_folded.drop_duplicates(inplace=True)
    rad_folded['feature'] = ['rad'] * rad_folded.shape[0]


    return rad_folded


def main_fetch_and_fold():
    description = 'Folding radiology reports from Ventura Care'
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
    # PATH = "/home/v_charvet/workspace/data/cr/cr_rad_tronc.xlsx"

    args = parser.parse_args()

    rad_folded = fetch_and_fold(args.path, args.id, args.db, args.targets,
                                args.nb)

    output = args.output
    rad_folded.to_csv(output, encoding='utf-8', sep=';')

    print('done')

    return rad_folded




if __name__ == "__main__":
    main_fetch_and_fold()



