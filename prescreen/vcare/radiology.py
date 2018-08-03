"""
fetching and processing radiology reports

Two data inputs needed: xls file that contains the contents of the reports and
the table containing additionnal patients information from SQL server
"""
import pandas as pd

from preprocessing.utils.connection import get_engine, sql2df
from preprocessing.utils.unfold import transform_and_label
from preprocessing.utils.fold import Folder
from preprocessing.html_parser.parser import ReportsParser
from sklearn.pipeline import Pipeline
from sklearn.feature_extraction.text import TfidfVectorizer

import argparse


def fetch_and_fold():
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
    # PATH = "/home/v_charvet/workspace/data/cr/cr_rad_tronc.xlsx"

    args = parser.parse_args()

    # getting variables from args
    PATH = args.path

    engine = get_engine(args.id, args.ip, args.db)

    key1, key2, date = 'patient_id', 'nip', 'date'

    # fetching targets table
    df_targets = sql2df(engine, args.targets).loc[:, [key2, 'id', 'C1J1']]
    df_targets.loc[:, 'C1J1'] = pd.to_datetime(df_targets['C1J1'],
                                               format='%Y-%m-%d',
                                               unit='D')

    df_rad = pd.read_excel(PATH, sep=';', usecols=7, parse_dates=[1, 2, 5])

    # filter SESSION and keep prescreen values
    mask = df_rad['SESSION'].isin(['NUM', 'PDF'])
    mask_prescreen = df_rad['OBSERVATION DATE'] == 'Before Date'

    df_rad = df_rad[~mask][mask_prescreen]
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

    # removing useless tags (blocks parsing)
    rad_folded['value'] = rad_folded.loc[:, 'value'].apply(lambda s: \
        str(s).replace('<u>', '').replace('</u>', ''))

    sections = ['critere d evaluation', 'nom du protocole']
    parser = ReportsParser(headers='b', is_html=False, col_name='value',
                           remove_sections=sections)

    rad_folded['value'] = parser.transform(rad_folded)

    rad_folded = rad_folded[rad_folded['value'] != '']
    rad_folded.drop_duplicates(inplace=True)
    rad_folded['feature'] = ['rad'] * rad_folded.shape[0]

    rad_folded.sort_values(by=[key1, date], inplace=True)

    output = args.output
    rad_folded.to_csv(output, encoding='utf-8', sep=';')

    print('done')

    return rad_folded


def fetch_and_transorm():
    description = 'Transforms and unfolds radiology reports from Ventura Care'
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
    # PATH = "/home/v_charvet/workspace/data/cr/cr_rad_tronc.xlsx"

    args = parser.parse_args()

    # getting variables from args
    PATH = args.path

    engine = get_engine(args.id, args.ip, args.db)

    key1, key2, date = 'patient_id', 'nip', 'date'

    # fetching targets table
    df_targets = sql2df(engine, args.targets).loc[:, ['nip', 'id', 'C1J1']]
    df_targets.loc[:, 'C1J1'] = pd.to_datetime(df_targets['C1J1'],
                                               format='%Y-%m-%d',
                                               unit='D')

    df_rad = pd.read_excel(PATH, sep=';', usecols=7, parse_dates=[1, 2, 5])

    # filter SESSION and keep prescreen values
    mask = df_rad['SESSION'].isin(['NUM', 'PDF'])
    mask_prescreen = df_rad['OBSERVATION DATE'] == 'Before Date'

    df_rad = df_rad[~mask][mask_prescreen]
    df_rad['CR_DATE'] = pd.to_datetime(df_rad['CR_DATE'], format='%Y%m%d')

    df_rad.rename({'CR_DATE': date}, axis=1, inplace=True)

    df_rad = df_rad.merge(df_targets, on=None, left_on='Nip',
                          right_on='nip').drop('Nip', axis=1)

    df_rad['patient_id'] = df_rad['id']
    df_rad.drop('id', axis=1, inplace=True)

    folder = Folder(key1, key2, ['CONTENU_CR'], date, n_jobs=-1)

    rad_folded = folder.fold(df_rad)

    steps = [('parser', ReportsParser(headers=None, n_jobs=-1)),
             ('tfidf', TfidfVectorizer(max_df=0.65,
                                       min_df=0,
                                       ngram_range=(1, 3),
                                       norm=None))]
    rad_folded = transform_and_label(rad_folded, key1, key2, date,
                                     'feature', 'value', Pipeline, steps=steps)


    rad_folded.sort_values(by=[key1, date], inplace=True)
    output = args.output
    rad_folded.to_csv(output, encoding='utf-8', sep=';')

    print('done')

    return rad_folded


if __name__ == "__main__":
    fetch_and_transorm()




def fetch2():
    """ deprecated script """

    path = "data/cr_rad_tronc.xlsx"

    df = pd.read_excel(path, sep=';', usecols=7, parse_dates=[1, 2, 5])
    df = df.sample(n=10000)

    mask = df['SESSION'].isin(['NUM', 'PDF'])
    mask_prescreen = df['OBSERVATION DATE'] == 'Before Date'

    df = df[~mask]
    nips = df['Nip'][mask_prescreen]

    parser = ReportsParser(strategy='strings',
                        remove_sections=[],
                        remove_tags=[],
                        col_name='CONTENU_CR',
                        headers=None)

    X = parser.transform(df.CONTENU_CR.fillna('empty'))
    X_prescreen = X[mask_prescreen]

    model = TfidfVectorizer(ngram_range=(1, 5), lowercase=False,
                            max_df=0.9, min_df=0.1)
    model.fit(X)

    vectors = model.transform(X_prescreen)

    dico = {'nip': nips}
    for i in range(vectors.shape[1]):
        dico['radiology_vec_{}'.format(i)] = vectors[:, i]

    df_res = pd.DataFrame(dico)
    return df_res.groupby('nip', as_index=False).mean()
