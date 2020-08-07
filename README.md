# Rolling-Peak-COVID-Deaths-by-State
Rolling peak COVID-19 Death Data by State

This code uses data from the Johns Hopkins COVID Data Repository, https://github.com/CSSEGISandData/COVID-19.  It calculates the peak single day deaths & cases by state and the peak 3-, 5- and 9-day average peak death & case rate by state.  It returns a CSV with each of these values and the date on which they occurred.  Rows are listed by state.

# Social Distancing Mobility Measure
IHME_Mobility.py takes input data from the Institute for Health Metrics and Evaluation (IHME) weekly release of COVID-19 related data, including social distancing mobility measure (https://covid19.healthdata.org/united-states-of-america).  Download the available dataset from (http://www.healthdata.org/covid/data-downloads) and renaming the file "Hospitalization_all_locs.csv" to "IHMEStats.csv" and place it in your working directory.  Run the processing code IHME_Mobility.py will return a csv with one row for each state and a column for the social distancing mobility metric for dates after 2/8/2020 through the last available observed mobility date in the dataset.  This will remove all projected mobility measures and only report the observed values.  Each column can be plotted to show the social mobility over time (column 1).

# Acknowledgements: 
This work was informed by code from jeffcore to compile these data daily - https://github.com/jeffcore/covid-19-usa-by-state.  Thanks to the Johns Hopkins CSSE and University of Washington IHME.  
