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
    subprocess.call('cd ' + config.config['HOME_DIRECTORY'] + '/COVID-19; git pull origin master', shell=True)

    # copy file to my repo for processing
    print('Copying Files')

    #  get all daily files
    list_of_files = glob.glob(
        config.config['HOME_DIRECTORY'] + '/COVID-19/csse_covid_19_data/csse_covid_19_daily_reports/*.csv')

    # copy to my repo
    for the_file in list_of_files:
        shutil.copy(the_file, config.config['HOME_DIRECTORY'] + '/Rolling-Peak-COVID-Deaths-by-State/data/')

    return datetime.datetime.now()



def process_states_data():
    print('processing state data')
    # configuration
    data_folder = './data/'
    start_date = datetime.datetime(2020, 3,
                                   22)  # this is the date JH daily files were in the correct format for processing
    today_date = datetime.datetime.now()
    cases_datafile = 'COVID-19-Cases-USA-By-State.csv'
    cases_starter_datefile = data_folder + 'COVID-19-Cases-USA-By-State-Starter.csv'
    death_datafile = 'COVID-19-Deaths-USA-By-State.csv'
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

    Cols = list(deathDF.columns)

    KeepList = ['State', 'Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado',
                'Connecticut', 'Delaware', 'District of Columbia', 'Florida', 'Georgia',
                'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky',
                'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota',
                'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire',
                'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio',
                'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota',
                'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia', 'Wisconsin',
                'Wyoming']

    DropList = np.setdiff1d(Cols, KeepList)
    C19Deaths = deathDF.drop(DropList, axis=1)

    States = list(C19Deaths.columns)

    DailyDeaths = C19Deaths.diff()

    ThreeDayAvg = pd.DataFrame()
    SevenDayAvg = pd.DataFrame()
    NineDayAvg = pd.DataFrame()

    for col in States:
        ThreeDayAvg[col] = DailyDeaths.loc[:, col].rolling(window=3).mean()
        SevenDayAvg[col] = DailyDeaths.loc[:, col].rolling(window=7).mean()
        NineDayAvg[col] = DailyDeaths.loc[:, col].rolling(window=9).mean()

    OneDayFrame = pd.DataFrame()
    ThreeDayFrame = pd.DataFrame()
    SevenDayFrame = pd.DataFrame()
    NineDayFrame = pd.DataFrame()

    for col in States:
        max1 = DailyDeaths[col].max()
        indexMax1 = DailyDeaths[col].idxmax()
        max3 = ThreeDayAvg[col].max()
        indexMax3 = ThreeDayAvg[col].idxmax()
        max7 = SevenDayAvg[col].max()
        indexMax7 = SevenDayAvg[col].idxmax()
        max9 = NineDayAvg[col].max()
        indexMax9 = NineDayAvg[col].idxmax()
        OneDayFrame[col] = [max1, indexMax1]
        ThreeDayFrame[col] = [max3, indexMax3]
        SevenDayFrame[col] = [max7, indexMax7]
        NineDayFrame[col] = [max9, indexMax9]

    OneDayMaxFrame = pd.DataFrame()
    OneDayMaxFrame = OneDayFrame.transpose()
    OneDayMaxFrame = OneDayMaxFrame.reset_index()
    OneDayMaxFrame = OneDayMaxFrame.rename(columns={'index': 'State', 0: 'Peak1DayDeaths', 1: 'Peak1DayDate'})

    ThreeDayAvgFrame = pd.DataFrame()
    ThreeDayAvgFrame = ThreeDayFrame.transpose()
    ThreeDayAvgFrame = ThreeDayAvgFrame.reset_index()
    ThreeDayAvgFrame = ThreeDayAvgFrame.rename(columns={'index': 'State', 0: 'Peak3DayAvgDeaths', 1: 'Peak3DayAvgDate'})

    SevenDayAvgFrame = pd.DataFrame()
    SevenDayAvgFrame = SevenDayFrame.transpose()
    SevenDayAvgFrame = SevenDayAvgFrame.reset_index()
    SevenDayAvgFrame = SevenDayAvgFrame.rename(columns={'index': 'State', 0: 'Peak7DayAvgDeaths', 1: 'Peak7DayAvgDate'})

    NineDayAvgFrame = pd.DataFrame()
    NineDayAvgFrame = NineDayFrame.transpose()
    NineDayAvgFrame = NineDayAvgFrame.reset_index()
    NineDayAvgFrame = NineDayAvgFrame.rename(columns={'index': 'State', 0: 'Peak9DayAvgDeaths', 1: 'Peak9DayAvgDate'})

    TotalDF = pd.DataFrame()
    TotalDF['State'] = States
    TotalDF['Peak1DayDeaths'] = OneDayMaxFrame['Peak1DayDeaths']
    TotalDF['Peak1DayDate'] = OneDayMaxFrame['Peak1DayDate']
    TotalDF['Peak3DayAvgDeaths'] = ThreeDayAvgFrame['Peak3DayAvgDeaths']
    TotalDF['Peak3DayAvgDate'] = ThreeDayAvgFrame['Peak3DayAvgDate']
    TotalDF['Peak7DayAvgDeaths'] = SevenDayAvgFrame['Peak7DayAvgDeaths']
    TotalDF['Peak7DayAvgDate'] = SevenDayAvgFrame['Peak7DayAvgDate']
    TotalDF['Peak9DayAvgDeaths'] = NineDayAvgFrame['Peak9DayAvgDeaths']
    TotalDF['Peak9DayAvgDate'] = NineDayAvgFrame['Peak9DayAvgDate']

    timestr = time.strftime("%Y%m%d")

    TotalDF.to_csv('COVID-Deaths-Peaks_%s.csv' % timestr, encoding='utf-8')


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
        if command_verification("Commit to Repo?"):
            commit_to_repo()
    print('finished')


if __name__ == "__main__":
    main()