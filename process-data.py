import subprocess
import shutil
import glob
import os
import config
import datetime
import pandas as pd
import numpy as np

import time
import ssl
ssl._create_default_https_context = ssl._create_unverified_context


def format_date_for_row(d):
    row_date_temp = d.strftime('%x')
    if row_date_temp[0] == '0':
        row_date = row_date_temp[1:]
    else:
        row_date = row_date_temp

    return row_date


def download_files():
    print('New Johns Hopkins COVID-19 Files Download')
    # first you have to run
    #  $ git clone https://github.com/CSSEGISandData/COVID-19.git
    # then add the home path to your repos to the config.py file

    # update local repo
    subprocess.call('cd ' + config.config['HOME_DIRECTORY'] + '; git pull origin master', shell=True)

    # Call for Clone of JHU Data, later deleted before committing to Repo
    print('Cloning JHU Data')
    subprocess.call('git clone https://github.com/CSSEGISandData/COVID-19.git', shell=True)
    global dir_path
    dir_path = config.config['HOME_DIRECTORY'] + '/COVID-19'

    # copy file to my repo for processing
    print('Copying Files')

    #  get all daily files
    list_of_files = glob.glob(config.config['HOME_DIRECTORY'] + '/COVID-19/csse_covid_19_data/csse_covid_19_daily_reports/*.csv')
    #'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/*.csv')


    # copy to my repo
    for the_file in list_of_files:
        shutil.copy(the_file, config.config['HOME_DIRECTORY'] + '/data/')

    return datetime.datetime.now()

    ####
    #('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/*.csv')



def process_states_data():
    print('processing state data')
    # configuration
    data_folder = './data/'
    start_date = datetime.datetime(2020, 3,
                                   22)  # this is the date JH daily files were in the correct format for processing
    today_date = datetime.datetime.now()
    cases_datafile = data_folder + 'COVID-19-Cases-USA-By-State.csv'
    cases_starter_datefile = data_folder + 'COVID-19-Cases-USA-By-State-Starter.csv'
    death_datafile = data_folder + 'COVID-19-Deaths-USA-By-State.csv'
    death_starter_datefile = data_folder + 'COVID-19-Deaths-USA-By-State-Starter.csv'

    try:
        # load cases starter file
        df_cases = pd.read_csv(cases_starter_datefile, encoding='utf-8', index_col='State')
        # load deaths starter file
        df_deaths = pd.read_csv(death_starter_datefile, encoding='utf-8', index_col='State')
    except:
        df_cases = pd.DataFrame()
        df_deaths = pd.DataFrame()

    while start_date <= today_date:
        file_date = start_date.strftime('%m-%d-%Y')
        daily_date = format_date_for_row(start_date)
        daily_datafile = data_folder + file_date + '.csv'
        print(start_date)
        # load JH state files
        try:
            df = pd.read_csv(daily_datafile, encoding='utf-8', index_col=False)
        except:
            start_date += datetime.timedelta(days=1)
            continue

        # filter only US rows
        df = df[df['Country_Region'] == 'US']

        # group by state
        df_daily_sum = df.groupby('Province_State').agg({'Confirmed': 'sum', 'Deaths': 'sum', 'Recovered': 'sum'})

        # drop unneeded rows
        if 'Wuhan Evacuee' in df.index:
            df_daily_sum = df_daily_sum.drop(['Wuhan Evacuee'])
        if 'Recovered' in df.index:
            df_daily_sum = df_daily_sum.drop(['Recovered'])

        # get cases
        df_daily_cases = df_daily_sum.iloc[:, [0]]

        # get deaths
        df_daily_deaths = df_daily_sum.iloc[:, [1]]

        # insert empty column for current date into summary file
        dft = pd.DataFrame({daily_date: np.array([0] * df_cases.shape[0], dtype='int32'), })
        df_cases.insert(df_cases.shape[1], daily_date, dft.values)

        # insert cases into summary df
        for index, row in df_daily_cases.iterrows():
            if index in df_cases.index:
                df_cases.at[index, daily_date] = row['Confirmed']

                # insert empty column for current date into summary file
        dft = pd.DataFrame({daily_date: np.array([0] * df_deaths.shape[0], dtype='int32'), })
        df_deaths.insert(df_deaths.shape[1], daily_date, dft.values)

        # insert deaths into summary df
        for index, row in df_daily_deaths.iterrows():
            if index in df_deaths.index:
                df_deaths.at[index, daily_date] = row['Deaths']

        start_date += datetime.timedelta(days=1)

    # save files
    df_deaths.to_csv(death_datafile, encoding='utf-8')
    df_cases.to_csv(cases_datafile, encoding='utf-8')

    """
    START GETTING PEAKS
    """
    # input_path = 'https://raw.githubusercontent.com/jeffcore/covid-19-usa-by-state/master/COVID-19-Deaths-USA-By-State.csv'
    # deaths = pd.read_csv(input_path, index_col='State', error_bad_lines=False)
    deaths = df_deaths
    deathDF = deaths.transpose()
    deathDF.head()

    cases = df_cases
    caseDF = cases.transpose()
    caseDF.head()

    DeathCols = list(deathDF.columns)
    CaseCols = list(caseDF.columns)

    KeepList = ['State', 'Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado',
                'Connecticut', 'Delaware', 'District of Columbia', 'Florida', 'Georgia',
                'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky',
                'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota',
                'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire',
                'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio',
                'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota',
                'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia', 'Wisconsin',
                'Wyoming']

    DeathDropList = np.setdiff1d(DeathCols, KeepList)
    CaseDropList = np.setdiff1d(CaseCols,KeepList)
    C19Deaths = deathDF.drop(DeathDropList, axis=1)
    C19Cases = caseDF.drop(CaseDropList,axis=1)

    States = list(C19Deaths.columns)

    DailyDeaths = C19Deaths.diff()
    DailyCases = C19Cases.diff()
    DailyDeaths.to_csv('COVID-DailyDeaths.csv', encoding='utf-8', index=False)
    DailyCases.to_csv('COVID-DailyCases.csv', encoding='utf-8', index=False)

    # Deaths
    ThreeDayAvgD = pd.DataFrame()
    SevenDayAvgD = pd.DataFrame()
    NineDayAvgD = pd.DataFrame()

    for col in States:
        ThreeDayAvgD[col] = DailyDeaths.loc[:, col].rolling(window=3).mean()
        SevenDayAvgD[col] = DailyDeaths.loc[:, col].rolling(window=7).mean()
        NineDayAvgD[col] = DailyDeaths.loc[:, col].rolling(window=9).mean()

    # Cases
    ThreeDayAvgC = pd.DataFrame()
    SevenDayAvgC = pd.DataFrame()
    NineDayAvgC = pd.DataFrame()

    for col in States:
        ThreeDayAvgC[col] = DailyCases.loc[:, col].rolling(window=3).mean()
        SevenDayAvgC[col] = DailyCases.loc[:, col].rolling(window=7).mean()
        NineDayAvgC[col] = DailyCases.loc[:, col].rolling(window=9).mean()

    # Deaths
    OneDayFrameD = pd.DataFrame()
    ThreeDayFrameD = pd.DataFrame()
    SevenDayFrameD = pd.DataFrame()
    NineDayFrameD = pd.DataFrame()

    for col in States:
        max1D = DailyDeaths[col].max()
        indexMax1D = DailyDeaths[col].idxmax()
        max3D = ThreeDayAvgD[col].max()
        indexMax3D = ThreeDayAvgD[col].idxmax()
        max7D = SevenDayAvgD[col].max()
        indexMax7D = SevenDayAvgD[col].idxmax()
        max9D = NineDayAvgD[col].max()
        indexMax9D = NineDayAvgD[col].idxmax()
        OneDayFrameD[col] = [max1D, indexMax1D]
        ThreeDayFrameD[col] = [max3D, indexMax3D]
        SevenDayFrameD[col] = [max7D, indexMax7D]
        NineDayFrameD[col] = [max9D, indexMax9D]

    # Cases
    OneDayFrameC = pd.DataFrame()
    ThreeDayFrameC = pd.DataFrame()
    SevenDayFrameC = pd.DataFrame()
    NineDayFrameC = pd.DataFrame()

    for col in States:
        max1C = DailyCases[col].max()
        indexMax1C = DailyCases[col].idxmax()
        max3C = ThreeDayAvgC[col].max()
        indexMax3C = ThreeDayAvgC[col].idxmax()
        max7C = SevenDayAvgC[col].max()
        indexMax7C = SevenDayAvgC[col].idxmax()
        max9C = NineDayAvgC[col].max()
        indexMax9C = NineDayAvgC[col].idxmax()
        OneDayFrameC[col] = [max1C, indexMax1C]
        ThreeDayFrameC[col] = [max3C, indexMax3C]
        SevenDayFrameC[col] = [max7C, indexMax7C]
        NineDayFrameC[col] = [max9C, indexMax9C]

    # Deaths
    OneDayMaxFrameD = pd.DataFrame()
    OneDayMaxFrameD = OneDayFrameD.transpose()
    OneDayMaxFrameD = OneDayMaxFrameD.reset_index()
    OneDayMaxFrameD = OneDayMaxFrameD.rename(columns={'index': 'State', 0: 'Peak1DayDeaths', 1: 'Peak1DayDeathDate'})

    ThreeDayAvgFrameD = pd.DataFrame()
    ThreeDayAvgFrameD = ThreeDayFrameD.transpose()
    ThreeDayAvgFrameD = ThreeDayAvgFrameD.reset_index()
    ThreeDayAvgFrameD = ThreeDayAvgFrameD.rename(columns={'index': 'State', 0: 'Peak3DayAvgDeaths', 1: 'Peak3DayAvgDeathDate'})

    SevenDayAvgFrameD = pd.DataFrame()
    SevenDayAvgFrameD = SevenDayFrameD.transpose()
    SevenDayAvgFrameD = SevenDayAvgFrameD.reset_index()
    SevenDayAvgFrameD = SevenDayAvgFrameD.rename(columns={'index': 'State', 0: 'Peak7DayAvgDeaths', 1: 'Peak7DayAvgDeathDate'})

    NineDayAvgFrameD = pd.DataFrame()
    NineDayAvgFrameD = NineDayFrameD.transpose()
    NineDayAvgFrameD = NineDayAvgFrameD.reset_index()
    NineDayAvgFrameD = NineDayAvgFrameD.rename(columns={'index': 'State', 0: 'Peak9DayAvgDeaths', 1: 'Peak9DayAvgDeathDate'})

    # Cases

    OneDayMaxFrameC = pd.DataFrame()
    OneDayMaxFrameC = OneDayFrameC.transpose()
    OneDayMaxFrameC = OneDayMaxFrameC.reset_index()
    OneDayMaxFrameC = OneDayMaxFrameC.rename(columns={'index': 'State', 0: 'Peak1DayCases', 1: 'Peak1DayCaseDate'})

    ThreeDayAvgFrameC = pd.DataFrame()
    ThreeDayAvgFrameC = ThreeDayFrameC.transpose()
    ThreeDayAvgFrameC = ThreeDayAvgFrameC.reset_index()
    ThreeDayAvgFrameC = ThreeDayAvgFrameC.rename(columns={'index': 'State', 0: 'Peak3DayAvgCases', 1: 'Peak3DayAvgCaseDate'})

    SevenDayAvgFrameC = pd.DataFrame()
    SevenDayAvgFrameC = SevenDayFrameC.transpose()
    SevenDayAvgFrameC = SevenDayAvgFrameC.reset_index()
    SevenDayAvgFrameC = SevenDayAvgFrameC.rename(columns={'index': 'State', 0: 'Peak7DayAvgCases', 1: 'Peak7DayAvgCaseDate'})

    NineDayAvgFrameC = pd.DataFrame()
    NineDayAvgFrameC = NineDayFrameC.transpose()
    NineDayAvgFrameC = NineDayAvgFrameC.reset_index()
    NineDayAvgFrameC = NineDayAvgFrameC.rename(columns={'index': 'State', 0: 'Peak9DayAvgCases', 1: 'Peak9DayAvgCaseDate'})


    TotalDF = pd.DataFrame()
    TotalDF['State'] = States
    # Cases
    TotalDF['Peak1DayCases'] = OneDayMaxFrameC['Peak1DayCases']
    TotalDF['Peak1DayCasesDate'] = OneDayMaxFrameC['Peak1DayCaseDate']
    TotalDF['Peak3DayAvgCases'] = ThreeDayAvgFrameC['Peak3DayAvgCases']
    TotalDF['Peak3DayAvgCasesDate'] = ThreeDayAvgFrameC['Peak3DayAvgCaseDate']
    TotalDF['Peak7DayAvgCases'] = SevenDayAvgFrameC['Peak7DayAvgCases']
    TotalDF['Peak7DayAvgCasesDate'] = SevenDayAvgFrameC['Peak7DayAvgCaseDate']
    TotalDF['Peak9DayAvgCases'] = NineDayAvgFrameC['Peak9DayAvgCases']
    TotalDF['Peak9DayAvgCasesDate'] = NineDayAvgFrameC['Peak9DayAvgCaseDate']
    #Deaths
    TotalDF['Peak1DayDeaths'] = OneDayMaxFrameD['Peak1DayDeaths']
    TotalDF['Peak1DayDeathsDate'] = OneDayMaxFrameD['Peak1DayDeathDate']
    TotalDF['Peak3DayAvgDeaths'] = ThreeDayAvgFrameD['Peak3DayAvgDeaths']
    TotalDF['Peak3DayAvgDeathsDate'] = ThreeDayAvgFrameD['Peak3DayAvgDeathDate']
    TotalDF['Peak7DayAvgDeaths'] = SevenDayAvgFrameD['Peak7DayAvgDeaths']
    TotalDF['Peak7DayAvgDeathsDate'] = SevenDayAvgFrameD['Peak7DayAvgDeathDate']
    TotalDF['Peak9DayAvgDeaths'] = NineDayAvgFrameD['Peak9DayAvgDeaths']
    TotalDF['Peak9DayAvgDeathsDate'] = NineDayAvgFrameD['Peak9DayAvgDeathDate']

    #timestr = time.strftime("%Y%m%d")

    TotalDF.to_csv('COVID-Peaks.csv', encoding='utf-8', index=False)


def commit_to_repo():
    print('committing to repo')
    today_date = datetime.datetime.now().strftime('%m-%d-%Y')
    subprocess.call(f'git commit -a -m "{today_date} data update"; git push origin master', shell=True)


def command_verification(command):
    print('please review following commands')
    print(command)
    result = input('Press ENTER to start: (type no to stop) ')
    if result == 'no':
        return False
    else:
        return True


def main():
    download_files()
    if command_verification("Process the files?"):
        process_states_data()
        if command_verification("Delete Local Data?"):
            try:
                shutil.rmtree(dir_path)
            except OSError as e:
                print("Error: %s : %s" % (dir_path, e.strerror))
            if command_verification("Commit to Repo?"):
                commit_to_repo()
    print('finished')


if __name__ == "__main__":
    main()