import pandas as pd
import numpy as np
import csv
import os

#
# Process Mobility Data, return dataframe in CSV with each column for states and each row for dates
#

def process_Mobility():
    dataFile = 'IHMEStats.csv'
    AllData = pd.read_csv(dataFile)

    KeepList = ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado',
                'Connecticut', 'Delaware', 'District of Columbia', 'Florida', 'Georgia',
                'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky',
                'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota',
                'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire',
                'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio',
                'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota',
                'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia', 'Wisconsin',
                'Wyoming']
    locNumberList = list(range(523,574))

    # Need to first get just the US locations by state, but then need to drop out rows for Georgia (country) based on LocationID
    DataUSTemp = AllData[AllData['location_name'].isin(KeepList)]
    DataUS = DataUSTemp[DataUSTemp['location_id'].isin(locNumberList)]

    MobilityData = DataUS[['location_name', 'date', 'mobility_composite', 'mobility_data_type']]
    MobilityData = MobilityData.dropna()
    MobilityObs = MobilityData[MobilityData['mobility_data_type'] == 'observed']

    Dates = MobilityObs['date'].unique()
    cols = ['State']
    cols.extend(Dates)
    MobsStates = pd.DataFrame(columns=[cols])
    MobsStates['State'] = KeepList

    #MobsStates['Date'] = Dates

    for state in KeepList:
        StateData = MobilityObs[MobilityObs['location_name'] == state]

        StateDates = StateData['date']
        StateMobsVals = StateData['mobility_composite']

        StateZipList = list(zip(StateDates,StateMobsVals))

        for row in StateZipList:
            stateInd = np.where(MobsStates['State'] == state)[0]
            MobsStates.loc[stateInd, row[0]] = row[1]

    States1 = MobsStates.reset_index()
    States1 = States1.transpose()
    States1 = States1.reset_index()
    States1 = States1[1:]
    States1.to_csv('StateMobilityTest.csv',encoding='utf-8',index=False)

    with open("StateMobilityTest.csv", 'r') as f:
        with open("StateMobility.csv", 'w') as f1:
            next(f)  # skip header line
            for line in f:
                f1.write(line)

    if os.path.exists("StateMobilityTest.csv"):
        os.remove("StateMobilityTest.csv")
    else:
        print("The file does not exist")

    #MobsStates.to_csv('StateMobility.csv', encoding='utf-8', index=False)

"""
Run Commands
"""
def command_verification(command):
    print('please review following commands')
    print(command)
    result = input('Press ENTER to start: (type no to stop) ')
    if result == 'no':
        return False
    else:
        return True

def main():
    if command_verification("Process Mobility Data files?"):
        process_Mobility()
    print('finished')


if __name__ == "__main__":
    main()

