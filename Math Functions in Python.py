pip install pandas
pip install Haver --extra-index-url http://www.haver.com/Python --trusted-host www.haver.com

import Haver
## Please note python is case sensitive 
import pandas
import numpy
## We will now set the path. Clients can use 'auto' if they are unsure where their directory lies
## Haver.direct('on') should be used if you are using DLX Direct
Haver.path('D:\DLX\DATA')


# Math functions (FX conversion, magnitude conversion and stacking of multiple functions are currently not supported)

# applying seasonal adjustment and % period function to the same series
hd = Haver.data(['sa(gdp@usecon)', 'yryr%(gdp@usecon)'])
hd2 = Haver.data(['gdp@usecon', 'c@usecon'])


# Retrieving all metadata from Emerging Asia database
asia = Haver.metadata(database='emergepr')
# Filtering for a list of codes for Korea based on geocode 542
korea2 = asia[asia['geography1'] == "542"]


# Wildcards
# To retrieve cross country data within EMERGEPR for example
# We can now create another variable which will contain a metadataframe with only the codes that match our regular expression. 
# The ^ indicates the beginning of the string.
# The $ sign indicates the end of the string.
# The . is the equivalent of ?
asia2 = ['^s...pc$' ,'^h...pc$']
pattern = '|'.join(asia2)
asia3 = asia[asia['code'].str.contains(pattern, regex=True)]
asia3_data = Haver.data(asia3)
print(asia3_data)


# Retrieving China GDP data from Emerging Asia database
china = Haver.metadata(database='emergepr')
china2 = china[china['geography1'] == "924"]
china_gdp = china2[china2['group'].isin(["B60", "B62", "B63", "B74", "S54"])]
codes_list = china_gdp['code'].tolist()
data = Haver.data(codes_list, database='emergepr')
print(data)


# Calculating Contributions to China GDP
gdp_contri = Haver.data(['yryr%(H924G1@EMERGEPR)', 'H924G1@EMERGEPR', 'H924NGDP@EMERGEPR', 'yryr%(H924G2@EMERGEPR)', 'H924G2@EMERGEPR', 'yryr%(H924G3@EMERGEPR)', 'H924G3@EMERGEPR'])

gdp_contri['primary'] = (
    gdp_contri['yryr%(h924g1)'] *
    gdp_contri['h924g1'] /
    gdp_contri['h924ngdp'])

gdp_contri['secondary'] = (
    gdp_contri['yryr%(h924g2)'] *
    gdp_contri['h924g2'] /
    gdp_contri['h924ngdp'])

gdp_contri['tertiary'] = (
    gdp_contri['yryr%(h924g3)'] *
    gdp_contri['h924g3'] /
    gdp_contri['h924ngdp'])


# How to get list of monetary policy dates, where +1 = meeting
dates = Haver.data(['R111MTG@INTDAILY', 
                    'R158MTG@INTDAILY', 
                    'R023MTG@INTDAILY', 
                    'R193MTG@INTDAILY'])
dates = dates.rename(columns={
    'r111mtg': 'US',
    'r158mtg': 'Japan',
    'r023mtg': 'Euro Area',
    'r193mtg': 'Australia'
})
result = {
    col: dates.index[dates[col] == 1]
    for col in dates.columns
}