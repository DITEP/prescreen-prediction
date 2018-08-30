"""
fetches biology for validation cohort
"""
import pandas as pd

from clintk.utils import Unfolder
from datetime import timedelta
from bs4 import BeautifulSoup
from io import StringIO

import requests



def fetch(url, header_path, df_ids):
    """

    Parameters
    ----------
    url : str
        url to the location of biology files

    header_path : str
        path to csv file containing header


    df_ids : pd.DataFrame
        df containing target info
        columns should be [nip, date_sign_ok]

    Returns
    -------

    """
    header = pd.read_csv(header_path, sep=';', encoding='latin1').columns

    cols = ['nip', 'Analyse', 'Resultat', 'Date prelvt']
    df_res = pd.DataFrame(data=None, columns=cols)

    for index, row in df_ids.iterrows():
        nip = row['nip']

        start_date = row['DATE SIGN_OK']
        end_date = start_date + timedelta(weeks=4)

        start = str(start_date).replace('-', '')
        stop = str(end_date).replace('-', '')

        req = requests.get(url.format(nip.replace(' ', ''), start, stop))
        values = BeautifulSoup(req.content, 'html.parser').body.text

        new_df = pd.read_csv(StringIO(values), sep=';', header=None,
                             index_col=False, names=header)
        new_df = new_df.loc[:, cols]

        new_df['nip'] = nip

        df_res = pd.concat([df_res, new_df], axis=0,
                           sort=False, ignore_index=True)

    return df_res


def fetch_and_fold(url, header_path, targets):

    df_bio = fetch(url, header_path, targets)

    df_bio['Date prelvt'] = pd.to_datetime(df_bio['Date prelvt'],
                                           errors='coerce',
                                           format='%Y%m%d').dt.date
    df_bio.dropna(inplace=True)

    df_bio.rename({'Date prelvt': 'date', 'Analyse': 'feature',
                   'Resultat': 'value'}, inplace=True, axis=1)

    df_bio['value'] = pd.to_numeric(df_bio.loc[:, 'value'], errors='coerce',
                                    downcast='float')

    return df_bio


def main_fetch():
    base_path = 'data/cohorte_validation'


    ## ditep
    path = base_path + '/ditep_inclus.csv'
    ditep_ok = pd.read_csv(path, sep=';',
                           parse_dates=[-2]).loc[:, ['nip','DATE SIGN_OK']]

    path = base_path + '/ditep_sf.csv'
    ditep_sf = pd.read_csv(path, sep=';',
                           parse_dates=[-2]).loc[:, ['nip', 'DATE SIGN_OK']]

    ditep = pd.concat([ditep_ok, ditep_sf], ignore_index=True)

    ditep['DATE SIGN_OK'] = ditep['DATE SIGN_OK'].dt.date

    url = 'http://esimbad/testGSAV7/reslabo?FENID=resLaboPatDitep&NIP={}' \
          '&STARTDATE={}&ENDDATE={}'
    header_path = '/home/v_charvet/workspace/data/biology/header.csv'

    bio_ditep = fetch_and_fold(url, header_path, ditep)


    # poumon
    path = base_path + '/poumons_inclusion.csv'
    poumon_ok = pd.read_csv(path, sep=';',
                            parse_dates=[-2]).loc[:, ['nip', 'DATE_SIGN_OK']]

    path = base_path + '/poumons_sf.csv'
    poumon_sf = pd.read_csv(path, sep=';',
                            parse_dates=[-2]).loc[:, ['nip', 'DATE_SIGN_OK']]

    poumon = pd.concat([poumon_ok, poumon_sf], ignore_index=True)
    poumon.rename({'DATE_SIGN_OK': 'DATE SIGN_OK'}, axis=1, inplace=True)

    poumon['DATE SIGN_OK'] = poumon['DATE SIGN_OK'].dt.date

    bio_poumon = fetch_and_fold(url, header_path, poumon)


    #unfolding features
    bio_ditep['null_id'] = [1] * bio_ditep.shape[0]
    bio_poumon['null_id'] = [1] * bio_poumon.shape[0]

    unfolder = Unfolder('nip', 'null_id', 'feature', 'value', 'date', False, -1)

    ditep_unfold = unfolder.fit(bio_ditep).unfold()

    poumon_unfold = unfolder.fit(bio_poumon).unfold()

    ditep_unfold.to_csv('data/ditep_bio_unfold.csv', sep=';')
    poumon_unfold.to_csv('data/poumons_bio_unfold.csv', sep=';')


    return bio_ditep, bio_poumon

if __name__ == "__main__":
    main_fetch()












