"""
fetches radiology reports from validation cohort

"""
import pandas as pd


def get_frames():
    base_path = 'data/cohorte_validation'

    # regular expression to find radiology reports
    pattern = "recist"


    ## DITEP OK
    path = base_path + '/ditep_inclus.csv'

    ditep_inclus = pd.read_csv(path, sep=';', encoding='utf-8')\
        .drop('Unnamed: 0', axis=1)

    mask_scanner = ditep_inclus['CR'].str.contains(pattern, case=False)

    ditep_inclus = ditep_inclus[mask_scanner]
    ditep_inclus = ditep_inclus.groupby('nip', as_index=False).agg('first')



    ## DITEP screenfail
    path = base_path + '/ditep_sf.csv'
    ditep_sf = pd.read_csv(path, sep=';', encoding='utf-8')\
        .drop('Unnamed: 0', axis=1)

    mask_scanner = ditep_sf['CR'].str.contains(pattern, case=False)

    ditep_sf = ditep_sf[mask_scanner]
    ditep_sf = ditep_sf.groupby('nip', as_index=False).agg('first')


    ## poumons inclus
    path = base_path + '/poumons_inclusion.csv'

    poumons = pd.read_csv(path, sep=';', encoding='utf-8')\
        .drop('Unnamed: 0', axis=1)

    mask_scanner = poumons['CR'].str.contains(pattern, case=False)

    poumons = poumons[mask_scanner]
    poumons = poumons.groupby('nip', as_index=False).agg('first')

    # poumons SF
    path = base_path + '/poumons_sf.csv'

    poumons_sf = pd.read_csv(path, sep=';', encoding='utf-8')\
        .drop('Unnamed: 0', axis=1)

    mask_scanner = poumons_sf['CR'].str.contains(pattern, case=False)

    poumons_sf = poumons_sf[mask_scanner]
    poumons_sf = poumons_sf.groupby('nip', as_index=False).agg('first')



    return ditep_inclus, ditep_sf, poumons, poumons_sf








