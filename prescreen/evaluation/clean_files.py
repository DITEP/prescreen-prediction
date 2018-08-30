"""
loads the validation files, removes useless rows and columns
"""
import pandas as pd



def main():
    base_path = 'data/cohorte_validation'

    # tables to get NIP
    path = base_path + '/POUMON_Etudes_II_III_Patients.xlsx'
    poumon_ref = pd.read_excel(path, sheet_name='POUMON_Etudes_Patients')\
        .loc[:, ['NOIGR', 'LC']]

    path = base_path + '/DITEP_Etudes_II_III_Patients.xlsx'
    ditep_ref = pd.read_excel(path, sheet_name='DITEP_Etudes_Patients')\
        .loc[:, ['NOIGR', 'LC']]



    # DITEP screenfail
    path = base_path + '/ditep_sf.xlsx'

    df = pd.read_excel(path).loc[:, ['NOIGR', 'CR', 'DATE CR', 'DATE_DEBUT']]

    mask_pdf = df['CR'].str.contains('\\NAS', regex=False)
    mask_empty = df['CR'].str.contains('Examen du', regex=False)

    df = df[~mask_pdf][~mask_empty]

    df['DATE_DEBUT'] = df.loc[:, 'DATE_DEBUT'].dt.date
    df['DATE CR'] = df.loc[:, 'DATE CR'].dt.date
    df['DATE SIGN_OK'] = df['DATE_DEBUT'] + pd.Timedelta(1, 'M')

    df.drop('DATE_DEBUT', axis=1, inplace=True)

    df = df.merge(ditep_ref, on='NOIGR')

    df['nip'] = df['NOIGR'].astype(str) + df['LC']
    df['nip'] = df.loc[:, 'nip'] \
        .apply(lambda s: s[:4] + '-' + s[4:-2] + ' ' + s[-2:])

    df.drop(['NOIGR', 'LC'], axis=1, inplace=True)

    df.to_csv(base_path + '/ditep_sf.csv', sep=';', encoding='utf-8')


    ## DITEP inclus
    path = base_path + '/ditep_inclus.xlsx'

    df = pd.read_excel(path).loc[:, ['NOIGR', 'CR', 'DATE CR', 'DATE_DEBUT']]

    mask_pdf = df['CR'].str.contains('\\NAS', regex=False)
    mask_empty = df['CR'].str.contains('Examen du', regex=False)

    df = df[~mask_pdf][~mask_empty]

    df['DATE_DEBUT'] = df.loc[:, 'DATE_DEBUT'].dt.date
    df['DATE CR'] = pd.to_datetime(df.loc[:, 'DATE CR'],
                                   errors='coerce').dropna().dt.date
    df['DATE SIGN_OK'] = df['DATE_DEBUT'] + pd.Timedelta(1, 'M')

    df.drop('DATE_DEBUT', axis=1, inplace=True)

    df = df.merge(ditep_ref, on='NOIGR').dropna()

    df['nip'] = df['NOIGR'].astype(str) + df['LC']
    df['nip'] = df.loc[:, 'nip'] \
        .apply(lambda s: s[:4] + '-' + s[4:-2] + ' ' + s[-2:])

    df.drop(['NOIGR', 'LC'], axis=1, inplace=True)
    df.to_csv(base_path + '/ditep_inclus.csv', sep=';', encoding='utf-8')


    ## poumons 1
    path = base_path + '/poumons_inclusion.xlsx'

    df = pd.read_excel(path).loc[:, ['NOIGR', 'CR', 'DATE CR',
                                     'DATE_SIGN_OK']]

    df.dropna(axis=0, inplace=True)

    mask_pdf = df['CR'].str.contains('\\NAS', regex=False)
    mask_empty = df['CR'].str.contains('Examen du', regex=False)

    df = df[~mask_pdf][~mask_empty]
    df['DATE_SIGN_OK'] = df.loc[:, 'DATE_SIGN_OK'].dt.date
    df['DATE CR'] = pd.to_datetime(df.loc[:, 'DATE CR'],
                                   errors='coerce').dropna().dt.date

    df = df.merge(poumon_ref, on='NOIGR').dropna()

    df['nip'] = df['NOIGR'].astype(str) + df['LC']
    df['nip'] = df.loc[:, 'nip'] \
        .apply(lambda s: s[:4] + '-' + s[4:-2] + ' ' + s[-2:])

    df.drop(['NOIGR', 'LC'], axis=1, inplace=True)

    df.to_csv(base_path + '/poumons_inclusion.csv', sep=';', encoding='utf-8')

    ## poumons2
    path = base_path + '/poumons_inclusion2.xlsx'

    df = pd.read_excel(path).loc[:, ['NOIGR', 'CR', 'DATE CR',
                                     'DATE_SIGN_OK']]

    df.dropna(axis=0, inplace=True)

    mask_pdf = df['CR'].str.contains('\\NAS', regex=False)
    mask_empty = df['CR'].str.contains('Examen du', regex=False)

    df = df[~mask_pdf][~mask_empty]
    df['DATE_SIGN_OK'] = pd.to_datetime(df['DATE_SIGN_OK'],
                                       errors='coerce').dropna().dt.date
    df['DATE CR'] = pd.to_datetime(df['DATE CR'],
                                       errors='coerce').dropna().dt.date

    df = df.merge(poumon_ref, on='NOIGR').dropna()

    df['nip'] = df['NOIGR'].astype(str) + df['LC']
    df['nip'] = df.loc[:, 'nip'] \
        .apply(lambda s: s[:4] + '-' + s[4:-2] + ' ' + s[-2:])

    df.drop(['NOIGR', 'LC'], axis=1, inplace=True)

    df.to_csv(base_path + '/poumons_inclusion2.csv', sep=';', encoding='utf-8')


    ## poumons SF
    path = base_path + '/poumons_sf.xlsx'

    colnames =['NOIGR', 'IDENTIFIANT', 'CR', 'DATE CR', 'DATE_SIGN_OK']
    # first col is scanned report
    df = pd.read_excel(path, header=1, names=colnames)\
        .loc[:, ['NOIGR', 'CR', 'DATE CR', 'DATE_SIGN_OK']]

    df.dropna(axis=0, inplace=True)

    mask_pdf = df['CR'].str.contains('\\NAS', regex=False)
    mask_empty = df['CR'].str.contains('Examen du', regex=False)

    df = df[~mask_pdf][~mask_empty]
    df['DATE_SIGN_OK'] = df.loc[:, 'DATE_SIGN_OK'].dt.date
    df['DATE CR'] = pd.to_datetime(df['DATE CR'],
                                   errors='coerce').dropna().dt.date

    df = df.merge(poumon_ref, on='NOIGR').dropna()

    df['nip'] = df['NOIGR'].astype(str) + df['LC']
    df['nip'] = df.loc[:, 'nip'] \
        .apply(lambda s: s[:4] + '-' + s[4:-2] + ' ' + s[-2:])

    df.drop(['NOIGR', 'LC'], axis=1, inplace=True)

    df.to_csv(base_path + '/poumons_sf.csv', sep=';', encoding='utf-8')

    return 0


if __name__ == "__main__":
    main()
