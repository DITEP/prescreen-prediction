"""
fetches consultation reports from validation cohort
"""
import pandas as pd


def get_frames():
    base_path = 'data/cohorte_validation'


    ## DITEP OK
    path = base_path + '/ditep_inclus.csv'

    ditep_inclus = pd.read_csv(path, sep=';', encoding='utf-8')\
        .drop('Unnamed: 0', axis=1)

    mask_consult = (ditep_inclus['DATE CR'] == ditep_inclus['DATE SIGN_OK'])

    ditep_inclus = ditep_inclus[mask_consult]
    ditep_inclus.drop(['DATE CR', 'DATE SIGN_OK'], axis=1, inplace=True)

    ditep_inclus = ditep_inclus.groupby('nip', as_index=False)\
        .agg(lambda s: ' '.join(s))



    ## DITEP screenfail
    path = base_path + '/ditep_sf.csv'
    ditep_sf = pd.read_csv(path, sep=';', encoding='utf-8')\
        .drop('Unnamed: 0', axis=1)

    mask_consult = (ditep_sf['DATE CR'] == ditep_sf['DATE SIGN_OK'])

    ditep_sf = ditep_sf[mask_consult]
    ditep_sf.drop(['DATE CR', 'DATE SIGN_OK'], axis=1, inplace=True)

    ditep_sf = ditep_sf.groupby('nip', as_index=False) \
        .agg(lambda s: ' '.join(s))


    ## poumons inclus
    path = base_path + '/poumons_inclusion.csv'

    poumons = pd.read_csv(path, sep=';', encoding='utf-8')\
        .drop('Unnamed: 0', axis=1)

    mask_consult = (poumons['DATE CR'] == poumons['DATE_SIGN_OK'])

    poumons = poumons[mask_consult]
    poumons.drop(['DATE CR', 'DATE_SIGN_OK'], axis=1, inplace=True)

    poumons = poumons.groupby('nip', as_index=False) \
        .agg(lambda s: ' '.join(s))

    # poumons SF
    path = base_path + '/poumons_sf.csv'

    poumons_sf = pd.read_csv(path, sep=';', encoding='utf-8')\
        .drop('Unnamed: 0', axis=1)

    mask_consult = (poumons_sf['DATE CR'] == poumons_sf['DATE_SIGN_OK'])

    poumons_sf = poumons_sf[mask_consult]
    poumons_sf.drop(['DATE CR', 'DATE_SIGN_OK'], axis=1, inplace=True)

    poumons_sf = poumons_sf.groupby('nip', as_index=False) \
        .agg(lambda s: ' '.join(s))


    return ditep_inclus, ditep_sf, poumons, poumons_sf
