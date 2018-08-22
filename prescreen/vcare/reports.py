""""
fetching and processing electronic medical reports
"""
import pandas as pd

from clintk.utils.connection import get_engine, sql2df
from clintk.utils.fold import Folder
from clintk.text_parser.parser import ReportsParser

import argparse


def fetch_and_fold(table, ID, ip, db, targets, n_reports):
    """ function to fetch reports from vcare database

    Parameters
    ----------
    For definition of parameters, see arguments in `main_fetch_and_fold`
    """
    engine = get_engine(ID, ip, db)

    key1, key2, date = 'patient_id', 'nip', 'date'

    # data used to train the model
    df_targets = sql2df(engine, targets).loc[:, ['nip', 'id', 'C1J1']]
    df_targets.loc[:, 'C1J1'] = pd.to_datetime(df_targets['C1J1'],
                                               format='%Y-%m-%d',
                                               unit='D')

    df_reports = sql2df(engine, table)\
        .loc[:, ['original_date', 'patient_id', 'report']]

    mask = [report is not None for report in df_reports['report']]

    df_reports.rename(columns={'original_date': 'date'}, inplace=True)
    df_reports = df_reports.loc[mask]

    # joining features df with complete patient informations
    df_reports = df_reports.merge(df_targets, on=None, left_on='patient_id',
                                  right_on='id').drop('id', axis=1)
    # df_reports = df_reports[df_reports[date] <= df_reports['C1J1']]

    # folding frames so that they have the same columns
    folder = Folder(key1, key2, ['report'], date, n_jobs=-1)
    reports_folded = folder.fold(df_reports)

    # taking only first report
    reports_folded = reports_folded.groupby(key1, as_index=False).agg('first')

    # parsing and vectorising text reports
    sections = ['examens complementaire', 'hopital de jour',
                'examen du patient']

    parser = ReportsParser(sections=None, n_jobs=-1, norm=False,
                           col_name='value')

    reports_folded['value'] = parser.transform(reports_folded)

    return reports_folded


def main_fetch_and_fold():
    description = 'Folding text reports from Ventura Care'
    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('--reports', '-r',
                        help='name of the table contains the reports')
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
                        help='number of reports to fetch')
    args = parser.parse_args()

    # getting variables from args

    reports_folded = fetch_and_fold(args.reports, args.id, args.ip, args.db,
                                    args.targets, args.nb)

    output = args.output
    reports_folded.to_csv(output, encoding='utf-8', sep=';')
    print('done')

    return reports_folded



if __name__ == "__main__":
    main_fetch_and_fold()
