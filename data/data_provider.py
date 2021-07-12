import covsirphy as cs
import pandas as pd
import os
import requests
from zipfile import ZipFile

class DataProvider:
    def __init__(self):
        pass

    def __from_covsirphy(self):
        # Source
        # https://lisphilar.github.io/covid19-sir/
        data_loader = cs.DataLoader("../input")
        jhu_data = data_loader.jhu()  # Cases (JHU)
        oxcgrt_data = data_loader.oxcgrt()  # Government Response (OxCGRT)
        pcr_data = data_loader.pcr()  # Tests
        vaccine_data = data_loader.vaccine()
        aut_cov = jhu_data.subset("Austria")
        aut_gov = oxcgrt_data.subset("Austria")
        aut_pcr = pcr_data.subset("Austria")
        aut_vac = vaccine_data.subset("Austria")

    def __from_autstat(self):
        # Source:
        # https://www.statistik.at/web_de/statistiken/menschen_und_gesellschaft/bevoelkerung/bevoelkerungsstruktur/bevoelkerung_nach_alter_geschlecht/index.html
        dfs = pd.read_excel("./data/bevoelkerung_am_1.1.2020_nach_alter_und_bundesland_-_insgesamt.xlsx",
                            sheet_name=None)
        dfs = dfs['Insgesamt'][['Bevölkerung am 1.1.2020 nach Alter und Bundesland - Insgesamt', 'Unnamed: 1']].loc[
              4:104]
        dfs = dfs.reset_index(drop=True)

        pop_groups = []
        for i in range(0, 104, 10):
            print('age', i, 'to', i + 9)
            pop_groups.append(dfs['Unnamed: 1'].loc[i:i + 9].sum())

        pp80 = sum(pop_groups[-3:])
        pop_groups = pop_groups[:-3]
        pop_groups.append(pp80)
        assert (sum(pop_groups) == dfs['Unnamed: 1'].sum())
        print(sum(pop_groups))

    def __from_ages(self):
        url = 'https://covid19-dashboard.ages.at/data/data.zip'
        save_path = './data/ages/data.zip'

        def download_url(url, save_path, chunk_size=128):
            r = requests.get(url, stream=True)
            with open(save_path, 'wb') as fd:
                for chunk in r.iter_content(chunk_size=chunk_size):
                    fd.write(chunk)

        download_url(url, save_path)
        with ZipFile('./data/ages/data.zip', 'r') as zipObj:
            zipObj.extractall(path='./data/ages/')

        # Source:
        # https://www.imperial.ac.uk/media/imperial-college/medicine/mrc-gida/2020-03-16-COVID19-Report-9.pdf
        pop_groups_ifr = [0.00002, 0.00006, 0.0003, 0.0008, 0.0015, 0.006, 0.022, 0.051, 0.093]
        pop_groups_hosp = [0.001, 0.003, 0.012, 0.032, 0.049, 0.102, 0.166, 0.243, 0.273]
        pop_groups_hosp_icu = [0.05, 0.05, 0.05, 0.05, 0.063, 0.122, 0.274, 0.432, 0.703]

        urls = ['https://www.ages.at/fileadmin/AGES2015/Wissen-Aktuell/COVID19/growth.csv',
                'https://www.ages.at/fileadmin/AGES2015/Wissen-Aktuell/COVID19/R_eff.csv',
                'https://www.ages.at/fileadmin/AGES2015/Wissen-Aktuell/COVID19/R_eff_bundesland.csv',
                'https://www.ages.at/fileadmin/AGES2015/Wissen-Aktuell/COVID19/meta_data.csv'
                ]

        for url in urls:
            with open('./data/ages/' + os.path.split(url)[1], 'wb') as f, \
                    requests.get(url, stream=True) as r:
                for line in r.iter_lines():
                    f.write(line + '\n'.encode())

        aut_reff = pd.read_csv("./data/ages/R_eff.csv", sep=';', index_col=0, header=0,
                               dtype={'Datum': str, 'R_eff': str, 'R_eff_lwr': str, 'R_eff_upr': str})
        aut_reff[0:] = aut_reff[0:].replace({',': '.'}, regex=True).astype(float)

