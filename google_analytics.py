#!/usr/bin/env python
# coding: utf-8

# # Extract Google Analytics tables into mysql db
# 
# Summary: Use python to pull data from Google Analytics using GA API.
# * Use python and GA API to extract data
# * Store data in dataframe.
# * Load into mysql tables.
# * Access data using SQL queries
# 
# References:
# 
# https://developers.google.com/analytics/devguides/reporting/core/v4/quickstart/service-py
# 
# https://medium.com/henry-jia/google-analytics-dashboard-project-a718112edf3a
# 
# https://canonicalized.com/google-analytics-python-pandas-plolty/
# 
# https://www.themarketingtechnologist.co/getting-started-with-the-google-analytics-reporting-api-in-python/
# 
# ### Update 3-2-2020: Fix Sampling Limitations
# Ref: https://github.com/adlerhs/ga-python-utils/blob/master/exportGAPageViews.py
# 
# https://www.ryanpraski.com/python-google-analytics-api-unsampled-data-multiple-profiles/

# # Install Dependencies

# In[ ]:


from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import numpy as np
import connect
from datetime import datetime
import time
from sqlalchemy import create_engine
from sqlalchemy.types import VARCHAR
import re
import calendar as cl
import datetime as dt

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = '<REPLACE_WITH_JSON_FILE>'
VIEW_ID = '<REPLACE_WITH_VIEW_ID>'


# # Prepare Utility Methods

# In[ ]:


def log(message):
    print('[%s] %s' % (datetime.now(), message))


# In[ ]:


def initialize_analyticsreporting():
  """Initializes an Analytics Reporting API V4 service object.

  Returns:
    An authorized Analytics Reporting API V4 service object.
  """
  credentials = ServiceAccountCredentials.from_json_keyfile_name(
      KEY_FILE_LOCATION, SCOPES)

  # Build the service object.
  analytics = build('analyticsreporting', 'v4', credentials=credentials)

  return analytics


# In[ ]:


# define global parameters
aDAllRows = []
aDDAllRows = []
sDAllRows = []
sSDAllRows = []
tUDAllRows = []
tfDAllRows = []
tfSAllRows = []
mtfDAllRows = []
msSDAllRows = []


# # I. Create Functions

# ## 1. Retrieving Article Data

# In[ ]:


def get_articleData(analytics, s_dt, e_dt, token = None):
    """Queries the Analytics Reporting API V4 for Article Data.

    Args:
        analytics: An authorized Analytics Reporting API V4 service object.
        s_dt: Start Date
        e_dt: End Date
        token: nextPageToken
    Returns:
        The Analytics Reporting API V4 response for Article Data.
    """
    response = analytics.reports().batchGet(
            body={
                'reportRequests': [
                {
                    'viewId': VIEW_ID,
                    'pageToken': token,
                    'pageSize': 100000,
                    'samplingLevel': 'LARGE',
                    'dateRanges': [{'startDate': s_dt, 'endDate': e_dt}],
                    'segments':[{'segmentId': '<SEGMENT_ID>'}],
                    'metrics': [{'expression': 'ga:uniquePageviews'},
                                {'expression': 'ga:users'},
                                {'expression': 'ga:sessions'},],
                    'dimensions': [{'name': 'ga:country'},
                                {'name': 'ga:hostname'},
                                {'name': 'ga:pagePath'},
                                {'name': 'ga:pageTitle'},
                                {'name': 'ga:yearMonth'},
                                {'name': 'ga:previousPagePath'},
                                {'name': 'ga:dimension1'},
                                {'name': 'ga:segment'}]
                }]
            }
    ).execute()
    
    # Check for 'nextPageToken'
    try:
        if response['reports'][0]['nextPageToken']:
            token = response['reports'][0]['nextPageToken']
    except KeyError:
        pass

    aRows = response['reports'][0]['data']['rows']
    global aDAllRows
    aDAllRows.extend(aRows)
    
    # recursive function
    try:
        if response['reports'][0]['nextPageToken']:
            get_articleData(analytics, s_dt, e_dt, token)
    except KeyError:
        pass

    return aDAllRows


# ## 2. Retrieving Article Deflection Data

# In[ ]:


def get_articleDeflectionData(analytics, s_dt, e_dt, token = None):
    """Queries the Analytics Reporting API V4 for Article Deflection Data.

    Args:
        analytics: An authorized Analytics Reporting API V4 service object.
        s_dt: Start Date
        e_dt: End Date
        token: nextPageToken
    Returns:
        The Analytics Reporting API V4 response for Article Deflection Data.
    """
    response = analytics.reports().batchGet(
            body={
                'reportRequests': [
                {
                    'viewId': VIEW_ID,
                    'pageToken': token,
                    'pageSize': 100000,
                    'samplingLevel': 'LARGE',
                    'dateRanges': [{'startDate': s_dt, 'endDate': e_dt}],
                    'segments':[{'segmentId':'<SEGMENT_ID>'}],
                    'metrics': [{'expression': 'ga:exits'},
                                {'expression': 'ga:sessions'},
                                {'expression': 'ga:uniquePageviews'},
                                {'expression': 'ga:users'}],
                    'dimensions': [{'name': 'ga:country'},
                                {'name': 'ga:exitPagePath'},
                                {'name': 'ga:hostname'},
                                {'name': 'ga:pageTitle'},
                                {'name': 'ga:yearMonth'},
                                {'name': 'ga:previousPagePath'},
                                {'name': 'ga:dimension1'},
                                {'name': 'ga:segment'}]
                }]
            }
    ).execute()
    
    # Check for 'nextPageToken'
    try:
        if response['reports'][0]['nextPageToken']:
            token = response['reports'][0]['nextPageToken']
    except KeyError:
        pass

    aRows = response['reports'][0]['data']['rows']
    global aDDAllRows
    aDDAllRows.extend(aRows)
    
    # recursive function
    try:
        if response['reports'][0]['nextPageToken']:
            get_articleDeflectionData(analytics, s_dt, e_dt, token)
    except KeyError:
        pass

    return aDDAllRows


# ## 3. Retrieving Self-Service Session Data

# In[ ]:


def get_selfServiceScoreData(analytics, s_dt, e_dt, token = None):
    """Queries the Analytics Reporting API V4 for Self-Service Score Data.

    Args:
        analytics: An authorized Analytics Reporting API V4 service object.
        s_dt: Start Date
        e_dt: End Date
        token: nextPageToken
    Returns:
        The Analytics Reporting API V4 response for Self-Service Score Data.
    """
    response = analytics.reports().batchGet(
            body={
                'reportRequests': [
                {
                    'viewId': VIEW_ID,
                    #'pageToken':pageTokenVariable,
                    'pageSize': 100000,
                    'samplingLevel': 'LARGE',
                    'dateRanges': [{'startDate': s_dt, 'endDate': e_dt}],
                    'segments':[{'segmentId': '<SEGMENT_ID>'}],
                    'metrics': [{'expression': 'ga:sessions'},
                                {'expression': 'ga:users'},
                                {'expression': 'ga:searchExits'},
                                {'expression': 'ga:searchRefinements'},
                                {'expression': 'ga:searchResultViews'},
                                {'expression': 'ga:searchSessions'},
                                {'expression': 'ga:searchUniques'}],
                    'dimensions': [{'name': 'ga:country'},
                                {'name': 'ga:hostname'},
                                {'name': 'ga:year'},
                                {'name': 'ga:yearMonth'},
                                {'name': 'ga:dimension1'},
                                {'name': 'ga:segment'}]
                }]
            }
    ).execute()
    
    # Check for 'nextPageToken'
    try:
        if response['reports'][0]['nextPageToken']:
            token = response['reports'][0]['nextPageToken']
    except KeyError:
        pass

    aRows = response['reports'][0]['data']['rows']
    global sSDAllRows
    sSDAllRows.extend(aRows)
    
    # recursive function
    try:
        if response['reports'][0]['nextPageToken']:
            get_selfServiceScoreData(analytics, s_dt, e_dt, token)
    except KeyError:
        pass

    return sSDAllRows


# ## 4. Retrieving Ticket User Data

# In[ ]:


def get_ticketUserData(analytics, s_dt, e_dt, token = None):
    """Queries the Analytics Reporting API V4 for Ticket User Data.

    Args:
        analytics: An authorized Analytics Reporting API V4 service object.
        s_dt: Start Date
        e_dt: End Date
        token: nextPageToken
    Returns:
        The Analytics Reporting API V4 response for Ticket User Data.
    """
    response = analytics.reports().batchGet(
            body={
                'reportRequests': [
                {
                    'viewId': VIEW_ID,
                    #'pageToken':pageTokenVariable,
                    'pageSize': 100000,
                    'samplingLevel': 'LARGE',
                    'dateRanges': [{'startDate': s_dt, 'endDate': e_dt}],
                    'segments':[{'segmentId': '<SEGMENT_ID>'}],
                    'metrics': [{'expression': 'ga:sessions'},
                                {'expression': 'ga:users'}],
                    'dimensions': [{'name': 'ga:country'},
                                {'name': 'ga:hostname'},
                                {'name': 'ga:yearMonth'},
                                {'name': 'ga:dimension1'},
                                {'name': 'ga:segment'}]
                }]
            }
    ).execute()
    
    # Check for 'nextPageToken'
    try:
        if response['reports'][0]['nextPageToken']:
            token = response['reports'][0]['nextPageToken']
    except KeyError:
        pass

    aRows = response['reports'][0]['data']['rows']
    global tUDAllRows
    tUDAllRows.extend(aRows)
    
    # recursive function
    try:
        if response['reports'][0]['nextPageToken']:
            get_ticketUserData(analytics, s_dt, e_dt, token)
    except KeyError:
        pass

    return tUDAllRows


# ## 5. Retrieve Ticket Form Deflection Data

# In[ ]:


def get_ticketFormDeflectionData(analytics, s_dt, e_dt, token = None):
    """Queries the Analytics Reporting API V4 for Article Deflection Data.

    Args:
        analytics: An authorized Analytics Reporting API V4 service object.
        s_dt: Start Date
        e_dt: End Date
        token: nextPageToken
    Returns:
        The Analytics Reporting API V4 response for Article Deflection Data.
    """
    response = analytics.reports().batchGet(
            body={
                'reportRequests': [
                {
                    'viewId': VIEW_ID,
                    'pageToken': token,
                    'pageSize': 100000,
                    'samplingLevel': 'LARGE',
                    'dateRanges': [{'startDate': s_dt, 'endDate': e_dt}],
                    'segments':[{'segmentId':'<SEGMENT_ID>'}],
                    'metrics': [{'expression': 'ga:exits'},
                                {'expression': 'ga:sessions'},
                                {'expression': 'ga:users'}],
                    'dimensions': [{'name': 'ga:country'},
                                {'name': 'ga:exitPagePath'},
                                {'name': 'ga:hostname'},
                                {'name': 'ga:pageTitle'},
                                {'name': 'ga:yearMonth'},
                                {'name': 'ga:previousPagePath'},
                                {'name': 'ga:dimension1'},
                                {'name': 'ga:segment'}]
                }]
            }
    ).execute()
    
    # Check for 'nextPageToken'
    try:
        if response['reports'][0]['nextPageToken']:
            token = response['reports'][0]['nextPageToken']
    except KeyError:
        pass

    aRows = response['reports'][0]['data']['rows']
    global tfDAllRows
    tfDAllRows.extend(aRows)
    
    # recursive function
    try:
        if response['reports'][0]['nextPageToken']:
            get_ticketFormDeflectionData(analytics, s_dt, e_dt, token)
    except KeyError:
        pass

    return tfDAllRows


# ## 6. Retrieve Ticket Form Session Data

# In[ ]:


def get_ticketFormSessionData(analytics, s_dt, e_dt, token = None):
    """Queries the Analytics Reporting API V4 for Self-Service Score Data.

    Args:
        analytics: An authorized Analytics Reporting API V4 service object.
        s_dt: Start Date
        e_dt: End Date
        token: nextPageToken
    Returns:
        The Analytics Reporting API V4 response for Self-Service Score Data.
    """
    response = analytics.reports().batchGet(
            body={
                'reportRequests': [
                {
                    'viewId': VIEW_ID,
                    #'pageToken':pageTokenVariable,
                    'pageSize': 100000,
                    'samplingLevel': 'LARGE',
                    'dateRanges': [{'startDate': s_dt, 'endDate': e_dt}],
                    'segments':[{'segmentId': '<SEGMENT_ID>'}],
                    'metrics': [{'expression': 'ga:sessions'},
                                {'expression': 'ga:users'}],
                    'dimensions': [{'name': 'ga:country'},
                                {'name': 'ga:hostname'},
                                {'name': 'ga:year'},
                                {'name': 'ga:yearMonth'},
                                {'name': 'ga:dimension1'},
                                {'name': 'ga:segment'}]
                }]
            }
    ).execute()
    
    # Check for 'nextPageToken'
    try:
        if response['reports'][0]['nextPageToken']:
            token = response['reports'][0]['nextPageToken']
    except KeyError:
        pass

    aRows = response['reports'][0]['data']['rows']
    global tfSAllRows
    tfSAllRows.extend(aRows)
    
    # recursive function
    try:
        if response['reports'][0]['nextPageToken']:
            get_ticketFormSessionData(analytics, s_dt, e_dt, token)
    except KeyError:
        pass

    return tfSAllRows


# ## 7. Retrieving Missed Ticket Form Deflections

# In[ ]:


def get_missedTicketFormDeflectionData(analytics, s_dt, e_dt, token = None):
    """Queries the Analytics Reporting API V4 for Article Deflection Data.

    Args:
        analytics: An authorized Analytics Reporting API V4 service object.
        s_dt: Start Date
        e_dt: End Date
        token: nextPageToken
    Returns:
        The Analytics Reporting API V4 response for Article Deflection Data.
    """
    response = analytics.reports().batchGet(
            body={
                'reportRequests': [
                {
                    'viewId': VIEW_ID,
                    'pageToken': token,
                    'pageSize': 100000,
                    'samplingLevel': 'LARGE',
                    'dateRanges': [{'startDate': s_dt, 'endDate': e_dt}],
                    'segments':[{'segmentId': '<SEGMENT_ID>'}],
                    'metrics': [{'expression': 'ga:exits'},
                                {'expression': 'ga:sessions'},
                                {'expression': 'ga:users'}],
                    'dimensions': [{'name': 'ga:country'},
                                {'name': 'ga:exitPagePath'},
                                {'name': 'ga:hostname'},
                                {'name': 'ga:pageTitle'},
                                {'name': 'ga:yearMonth'},
                                {'name': 'ga:previousPagePath'},
                                {'name': 'ga:dimension1'},
                                {'name': 'ga:segment'}]
                }]
            }
    ).execute()
    
    # Check for 'nextPageToken'
    try:
        if response['reports'][0]['nextPageToken']:
            token = response['reports'][0]['nextPageToken']
    except KeyError:
        pass

    aRows = response['reports'][0]['data']['rows']
    global mtfDAllRows
    mtfDAllRows.extend(aRows)
    
    # recursive function
    try:
        if response['reports'][0]['nextPageToken']:
            get_missedTicketFormDeflectionData(analytics, s_dt, e_dt, token)
    except KeyError:
        pass

    return mtfDAllRows


# ## 8. Retrieving Missed Self-Service Deflections

# In[ ]:


def get_missedSelfServiceDeflectionData(analytics, s_dt, e_dt, token = None):
    """Queries the Analytics Reporting API V4 for Article Deflection Data.

    Args:
        analytics: An authorized Analytics Reporting API V4 service object.
        s_dt: Start Date
        e_dt: End Date
        token: nextPageToken
    Returns:
        The Analytics Reporting API V4 response for Article Deflection Data.
    """
    response = analytics.reports().batchGet(
            body={
                'reportRequests': [
                {
                    'viewId': VIEW_ID,
                    'pageToken': token,
                    'pageSize': 100000,
                    'samplingLevel': 'LARGE',
                    'dateRanges': [{'startDate': s_dt, 'endDate': e_dt}],
                    'segments':[{'segmentId': '<SEGMENT_ID>'}],
                    'metrics': [{'expression': 'ga:exits'},
                                {'expression': 'ga:sessions'},
                                {'expression': 'ga:users'}],
                    'dimensions': [{'name': 'ga:country'},
                                {'name': 'ga:exitPagePath'},
                                {'name': 'ga:hostname'},
                                {'name': 'ga:pageTitle'},
                                {'name': 'ga:yearMonth'},
                                {'name': 'ga:previousPagePath'},
                                {'name': 'ga:dimension1'},
                                {'name': 'ga:segment'}]
                }]
            }
    ).execute()
    
    # Check for 'nextPageToken'
    try:
        if response['reports'][0]['nextPageToken']:
            token = response['reports'][0]['nextPageToken']
    except KeyError:
        pass

    aRows = response['reports'][0]['data']['rows']
    global msSDAllRows
    msSDAllRows.extend(aRows)
    
    # recursive function
    try:
        if response['reports'][0]['nextPageToken']:
            get_missedSelfServiceDeflectionData(analytics, s_dt, e_dt, token)
    except KeyError:
        pass

    return msSDAllRows


# ## 9. Get by Month Year increments (avoid sampling limitation)

# In[ ]:


def getMonthData(year, month, function):
    lastDay = cl.monthrange(year, month)[1]
    indexDay = 1
    list_ = []
    while indexDay > 0 and indexDay < lastDay:
        startDate = "{:%Y-%m-%d}".format(dt.datetime(year, month, indexDay))
        indexDay = lastDay
        
        while indexDay > 0:
            try:
                endDate = "{:%Y-%m-%d}".format(dt.datetime(year, month, indexDay))

                response = function(analytics, startDate, endDate)
                if type(response) != str:
                    list_ = response
                    indexDay += 1
                    break;
            except:
                indexDay -= 1
                pass

    print(startDate, endDate, len(response), len(list_))
    return list_


# # II. Test Functions - Print Response

# In[ ]:


def print_response(response):
    """Parses and prints the Analytics Reporting API V4 response.

    Args:
        response: An Analytics Reporting API V4 response.
    """
    for report in response.get('reports', []):
        #pagetoken = report.get('nextPageToken', None)
        columnHeader = report.get('columnHeader', {})
        dimensionHeaders = columnHeader.get('dimensions', [])
        metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])

        for row in report.get('data', {}).get('rows', []):
            dimensions = row.get('dimensions', [])
            dateRangeValues = row.get('metrics', [])

            for header, dimension in zip(dimensionHeaders, dimensions):
                print(header, dimension, sep =  ': ')

            for i, values in enumerate(dateRangeValues):
                print('Date range: ', str(i))
                for metricHeader, value in zip(metricHeaders, values.get('values')):
                    print(metricHeader.get('name'), ': ', value)


def main():
    analytics = initialize_analyticsreporting()
    response = get_articleData(analytics, '7daysago', 'today')
    print_response(response)

#if __name__ == '__main__':
#    main()


# # III. Generate Data Frames

# In[ ]:


# Update with your mysql connection info
engine = create_engine("mysql+pymysql://lportal:lportal@googledb/google_analytics?charset=utf8mb4")


# In[ ]:


with engine.begin() as connection:
    connection.execute('drop table if exists articledata')
    connection.execute('drop table if exists articledeflectiondata')
    connection.execute('drop table if exists searchdata')
    connection.execute('drop table if exists selfservicescoredata')
    connection.execute('drop table if exists ticketuserdata')
    connection.execute('drop table if exists ticketformdefl')
    connection.execute('drop table if exists ticketformsession')
    connection.execute('drop table if exists missedticketformdefl')
    connection.execute('drop table if exists missedselfservicedefl')


# ## 0. Define Date Range to Pull

# In[ ]:


# Pulling data from 2018-12-01 until now
#dates = ["2018-12-01", datetime.now().strftime("%Y-%m-%d")]
# Custom dimension User Role added in Feb 2019
dates = ["2019-03-01", datetime.now().strftime("%Y-%m-%d")]
# Ticket Submit button added Feb 2020
dates_button = ["2020-02-01", datetime.now().strftime("%Y-%m-%d")]
def monthlist(dates):
    start, end = [datetime.strptime(_, "%Y-%m-%d") for _ in dates]
    total_months = lambda dt: dt.month + 12 * dt.year
    mlist = []
    for tot_m in range(total_months(start)-1, total_months(end)):
        y, m = divmod(tot_m, 12)
        mlist.append((datetime(y, m+1, 1).year, datetime(y, m+1, 1).month),)
    return mlist


# ## 1. Generate Article Data

# In[ ]:


analytics = initialize_analyticsreporting()


# In[ ]:


for year, month in monthlist(dates):
    getMonthData(year, month, get_articleData)


# In[ ]:


countries = [v['dimensions'][0] for v in aDAllRows]
hostname = [v['dimensions'][1] for v in aDAllRows]
page = [v['dimensions'][2] for v in aDAllRows]
pageTitle = [v['dimensions'][3] for v in aDAllRows]
monthYear = [v['dimensions'][4] for v in aDAllRows]
previousPagePath = [v['dimensions'][5] for v in aDAllRows]
userRole = [v['dimensions'][6] for v in aDAllRows]
uniquePageviews = [v['metrics'][0]['values'][0] for v in aDAllRows]
users = [v['metrics'][0]['values'][1] for v in aDAllRows]
sessions = [v['metrics'][0]['values'][2] for v in aDAllRows]


# In[ ]:


articleData_df = pd.DataFrame()
articleData_df['Country'] = countries
articleData_df['Hostname'] = hostname
articleData_df['Page'] = page
articleData_df['PageTitle'] = pageTitle
articleData_df['MonthofYear'] = monthYear
articleData_df['PreviousPagePath'] = previousPagePath
articleData_df['UserRole'] = userRole
articleData_df['UniquePageviews'] = uniquePageviews
articleData_df['Users'] = users
articleData_df['Sessions'] = sessions


# In[ ]:


int_cols = ['MonthofYear', 'UniquePageviews', 'Users', 'Sessions']
for col in int_cols:
    articleData_df[col] = pd.to_numeric(articleData_df[col], errors='coerce')


# In[ ]:


# N/A User role
articleData_df['UserRole'] = articleData_df['UserRole'].replace('Dynamic Segment',np.NaN)


# In[ ]:


# Adding custom fields
articleData_df['ArticleId'] = articleData_df['Page'].str.extract('^.*articles\/([0-9]{12})', expand = False)
articleData_df['LocaleCode'] = articleData_df['Page'].str.extract('\/hc\/(en-us|es|zh-cn|ja|pt)\/', expand = False)
articleData_df['TicketId'] = articleData_df['PreviousPagePath'].str.extract('^.*requests\/([0-9]{3,6})', expand = False)
articleData_df['Date'] = pd.to_datetime(articleData_df['MonthofYear'], format = '%Y%m')
articleData_df['SupportRegion'] = np.where(articleData_df['Country'].str.contains('Australia'), 'Australia',
                   np.where(articleData_df['Country'].str.contains('Belize|Costa Rica|El Salvador|Guatemala|Honduras|Jamaica|Mexico|Nicaragua|Panama|Argentina|Bolivia|Brazil|Chile|Colombia|Ecuador|Paraguay|Peru|Uraguay|Uruguay|Venezuela|Cuba|Dominican Republic|Haiti'), 'Brazil',
                   np.where(articleData_df['Country'].str.contains('Cambodia|China|Hong Kong|Indonesia|Laos|Macau|Malaysia|Myanmar|New Zealand|Philippines|Pakistan|Singapore|South Korea|Sri Lanka|Taiwan|Thailand|Vietnam'), 'China', 
                   np.where(articleData_df['Country'].str.contains('lbania|Algeria|Andorra|Angola|Austria|Bahrain|Belarus|Belgium|Benin|Bosnia and Herzegovina|Bosnia & Herzegovina|Botswana|Bulgaria|Burkina Faso|Burundi|Cameroon|Cape Verde|Central African Republic|Chad|Comoros|Croatia|Cyprus|Czechia|Czech Republic|Democratic Republic of the Congo|Denmark|Djibouti|Egypt|Equatorial Guinea|Eritrea|Estonia|Ethiopia|Faroe Islands|Finland|France|Gabon|Gambia|Georgia|Germany|Ghana|Gibraltar|Greece|Guernsey|Guinea|Hungary|Iceland|Iran|Iraq|Ireland|Israel|Italy|Ivory Coast|Jordan|Kazakhstan|Kenya|Kosovo|Kuwait|Latvia|Lebanon|Lesotho|Liberia|Libya|Liechtenstein|Lithuania|Luxembourg|Macedonia|Madagascar|Malawi|Mali|Malta|Mauritania|Mauritius|Moldova|Monaco|Montenegro|Morocco|Mozambique|Namibia|Netherlands|Niger|Nigeria|Norway|Poland|Qatar|Romania|Russia|Rwanda|San Marino|Saudi Arabia|Senegal|Serbia|Slovakia|Slovenia|Somalia|South Africa|Sudan|Swaziland|Sweden|Switzerland|Syria|Tanzania|Togo|Tunisia|Turkey|Uganda|Ukraine|United Arab Emirates|United Kingdom|Western Sahara|Yemen|Zambia|Zimbabwe'), 'Hungary', 
                   np.where(articleData_df['Country'].str.contains('India|Bangladesh'), 'India',
                   np.where(articleData_df['Country'].str.contains('Japan'), 'Japan',
                   np.where(articleData_df['Country'].str.contains('Spain|Portugal'), 'Spain',
                   np.where(articleData_df['Country'].str.contains('United States|Canada'), 'US',            
                               'other'))))))))


# In[ ]:


# Testing Article Id Regex
#test = articleData_df
#test['ArticleId'] = test['Page'].str.extract('^.*articles\/([0-9]{12})', expand = False)
#test = test[test['Page'].str.contains("articles/")]
#test.head()


# In[ ]:


articleData_df.reset_index(level=articleData_df.index.names, inplace=True) 
#articleData_df.head()


# In[ ]:


articleData_df.head()


# In[ ]:


articleData_df['Users'].sum()


# In[ ]:


articleData_df['UniquePageviews'].sum()


# In[ ]:


#Generate articleData table
articleData_table_dtypes = {
}

articleData_df.to_sql('articledata', engine, index=False, chunksize=10000, dtype=articleData_table_dtypes)


# In[ ]:


#Adding indexes
with engine.begin() as connection:
    connection.execute('create index GA_INDEX_01 on articledata(`index`)')
    connection.execute('create index GA_INDEX_02 on articledata(MonthofYear)')


# In[ ]:


del countries
del hostname
del page
del pageTitle
del monthYear
del previousPagePath
del userRole
del uniquePageviews
del users
del sessions


# In[ ]:


del aDAllRows
del articleData_df


# ## 2. Generate Article Deflection Data

# In[ ]:


#articleDeflectionData = get_articleDeflectionData(analytics, '2018-12-01', 'today')

for year, month in monthlist(dates):
    getMonthData(year, month, get_articleDeflectionData)


# In[ ]:


countries = [v['dimensions'][0] for v in aDDAllRows]
exitPage = [v['dimensions'][1] for v in aDDAllRows]
hostname = [v['dimensions'][2] for v in aDDAllRows]
pageTitle = [v['dimensions'][3] for v in aDDAllRows]
monthYear = [v['dimensions'][4] for v in aDDAllRows]
previousPagePath = [v['dimensions'][5] for v in aDDAllRows]
userRole = [v['dimensions'][6] for v in aDDAllRows]
exits = [v['metrics'][0]['values'][0] for v in aDDAllRows]
sessions = [v['metrics'][0]['values'][1] for v in aDDAllRows]
uniquePageviews = [v['metrics'][0]['values'][2] for v in aDDAllRows]
users = [v['metrics'][0]['values'][3] for v in aDDAllRows]


# In[ ]:


articleDefData_df = pd.DataFrame()
articleDefData_df['Country'] = countries
articleDefData_df['ExitPage'] = exitPage
articleDefData_df['Hostname'] = hostname
articleDefData_df['PageTitle'] = pageTitle
articleDefData_df['MonthofYear'] = monthYear
articleDefData_df['PreviousPagePath'] = previousPagePath
articleDefData_df['UserRole'] = userRole
articleDefData_df['Exits'] = exits
articleDefData_df['Sessions'] = sessions
articleDefData_df['UniquePageviews'] = uniquePageviews
articleDefData_df['Users'] = users


# In[ ]:


# Adding custom fields
articleDefData_df['ArticleId_ExitPage'] = articleDefData_df['ExitPage'].str.extract('^.*articles\/([0-9]{12})', expand = False)
articleDefData_df['LocaleCode_ExitPage'] = articleDefData_df['ExitPage'].str.extract('\/hc\/(en-us|es|zh-cn|ja|pt)\/', expand = False)
articleDefData_df['TicketId'] = articleDefData_df['PreviousPagePath'].str.extract('^.*requests\/([0-9]{3,6})', expand = False)
articleDefData_df['Date'] = pd.to_datetime(articleDefData_df['MonthofYear'], format = '%Y%m')
articleDefData_df['SupportRegion'] = np.where(articleDefData_df['Country'].str.contains('Australia'), 'Australia',
                   np.where(articleDefData_df['Country'].str.contains('Belize|Costa Rica|El Salvador|Guatemala|Honduras|Jamaica|Mexico|Nicaragua|Panama|Argentina|Bolivia|Brazil|Chile|Colombia|Ecuador|Paraguay|Peru|Uraguay|Uruguay|Venezuela|Cuba|Dominican Republic|Haiti'), 'Brazil',
                   np.where(articleDefData_df['Country'].str.contains('Cambodia|China|Hong Kong|Indonesia|Laos|Macau|Malaysia|Myanmar|New Zealand|Philippines|Pakistan|Singapore|South Korea|Sri Lanka|Taiwan|Thailand|Vietnam'), 'China', 
                   np.where(articleDefData_df['Country'].str.contains('lbania|Algeria|Andorra|Angola|Austria|Bahrain|Belarus|Belgium|Benin|Bosnia and Herzegovina|Bosnia & Herzegovina|Botswana|Bulgaria|Burkina Faso|Burundi|Cameroon|Cape Verde|Central African Republic|Chad|Comoros|Croatia|Cyprus|Czechia|Czech Republic|Democratic Republic of the Congo|Denmark|Djibouti|Egypt|Equatorial Guinea|Eritrea|Estonia|Ethiopia|Faroe Islands|Finland|France|Gabon|Gambia|Georgia|Germany|Ghana|Gibraltar|Greece|Guernsey|Guinea|Hungary|Iceland|Iran|Iraq|Ireland|Israel|Italy|Ivory Coast|Jordan|Kazakhstan|Kenya|Kosovo|Kuwait|Latvia|Lebanon|Lesotho|Liberia|Libya|Liechtenstein|Lithuania|Luxembourg|Macedonia|Madagascar|Malawi|Mali|Malta|Mauritania|Mauritius|Moldova|Monaco|Montenegro|Morocco|Mozambique|Namibia|Netherlands|Niger|Nigeria|Norway|Poland|Qatar|Romania|Russia|Rwanda|San Marino|Saudi Arabia|Senegal|Serbia|Slovakia|Slovenia|Somalia|South Africa|Sudan|Swaziland|Sweden|Switzerland|Syria|Tanzania|Togo|Tunisia|Turkey|Uganda|Ukraine|United Arab Emirates|United Kingdom|Western Sahara|Yemen|Zambia|Zimbabwe'), 'Hungary', 
                   np.where(articleDefData_df['Country'].str.contains('India|Bangladesh'), 'India',
                   np.where(articleDefData_df['Country'].str.contains('Japan'), 'Japan',
                   np.where(articleDefData_df['Country'].str.contains('Spain|Portugal'), 'Spain',
                   np.where(articleDefData_df['Country'].str.contains('United States|Canada'), 'US',            
                               'other'))))))))


# In[ ]:


int_cols = ['MonthofYear', 'Exits', 'Sessions', 'UniquePageviews', 'Users']
for col in int_cols:
    articleDefData_df[col] = pd.to_numeric(articleDefData_df[col], errors='coerce')


# In[ ]:


articleDefData_df.reset_index(level=articleDefData_df.index.names, inplace=True) 
articleDefData_df.head()


# In[ ]:


#Generate articleDeflectionData table
articleDeflectionData_table_dtypes = {
}

articleDefData_df.to_sql('articledeflectiondata', engine, index=False, chunksize=10000, dtype=articleDeflectionData_table_dtypes)


# In[ ]:


#Adding indexes
with engine.begin() as connection:
    connection.execute('create unique index GA_INDEX_03 on articledeflectiondata(`index`)')
    connection.execute('create index GA_INDEX_04 on articledeflectiondata(MonthofYear)')


# In[ ]:


del countries
del exitPage
del hostname
del pageTitle
del monthYear
del previousPagePath
del userRole
del exits
del sessions
del uniquePageviews
del users


# In[ ]:


del aDDAllRows
del articleDefData_df


# ## 3. Generate Self-Service Session Data

# In[ ]:


#selfServiceScoreData = get_selfServiceScoreData(analytics, '2018-12-01', 'today')

for year, month in monthlist(dates):
    getMonthData(year, month, get_selfServiceScoreData)


# In[ ]:


countries = [v['dimensions'][0] for v in sSDAllRows]
hostname = [v['dimensions'][1] for v in sSDAllRows]
year = [v['dimensions'][2] for v in sSDAllRows]
monthYear =[v['dimensions'][3] for v in sSDAllRows]
userRole =[v['dimensions'][4] for v in sSDAllRows]
sessions = [v['metrics'][0]['values'][0] for v in sSDAllRows]
users = [v['metrics'][0]['values'][1] for v in sSDAllRows]
searchExits = [v['metrics'][0]['values'][2] for v in sSDAllRows]
searchRefinements = [v['metrics'][0]['values'][3] for v in sSDAllRows]
searchResultViews = [v['metrics'][0]['values'][4] for v in sSDAllRows]
searchSessions = [v['metrics'][0]['values'][5] for v in sSDAllRows]
searchUniques = [v['metrics'][0]['values'][6] for v in sSDAllRows]


# In[ ]:


selfServiceScoreData_df = pd.DataFrame()
selfServiceScoreData_df['Country'] = countries
selfServiceScoreData_df['Hostname'] = hostname
selfServiceScoreData_df['Year'] = year
selfServiceScoreData_df['MonthofYear'] = monthYear
selfServiceScoreData_df['UserRole'] = userRole
selfServiceScoreData_df['Sessions'] = sessions
selfServiceScoreData_df['Users'] = users
selfServiceScoreData_df['SearchExits'] = searchExits
selfServiceScoreData_df['SearchRefinements'] = searchRefinements
selfServiceScoreData_df['SearchResultViews'] = searchResultViews
selfServiceScoreData_df['SearchSessions'] = searchSessions
selfServiceScoreData_df['SearchUniques'] = searchUniques


# In[ ]:


int_cols = ['Year', 'MonthofYear', 'Sessions', 'Users', 'SearchExits', 'SearchRefinements', 'SearchResultViews', 'SearchSessions', 'SearchUniques']
for col in int_cols:
    selfServiceScoreData_df[col] = pd.to_numeric(selfServiceScoreData_df[col], errors='coerce')


# In[ ]:


# Adding custom fields
selfServiceScoreData_df['Date'] = pd.to_datetime(selfServiceScoreData_df['MonthofYear'], format = '%Y%m')
selfServiceScoreData_df['SupportRegion'] = np.where(selfServiceScoreData_df['Country'].str.contains('Australia'), 'Australia',
                   np.where(selfServiceScoreData_df['Country'].str.contains('Belize|Costa Rica|El Salvador|Guatemala|Honduras|Jamaica|Mexico|Nicaragua|Panama|Argentina|Bolivia|Brazil|Chile|Colombia|Ecuador|Paraguay|Peru|Uraguay|Uruguay|Venezuela|Cuba|Dominican Republic|Haiti'), 'Brazil',
                   np.where(selfServiceScoreData_df['Country'].str.contains('Cambodia|China|Hong Kong|Indonesia|Laos|Macau|Malaysia|Myanmar|New Zealand|Philippines|Pakistan|Singapore|South Korea|Sri Lanka|Taiwan|Thailand|Vietnam'), 'China', 
                   np.where(selfServiceScoreData_df['Country'].str.contains('lbania|Algeria|Andorra|Angola|Austria|Bahrain|Belarus|Belgium|Benin|Bosnia and Herzegovina|Bosnia & Herzegovina|Botswana|Bulgaria|Burkina Faso|Burundi|Cameroon|Cape Verde|Central African Republic|Chad|Comoros|Croatia|Cyprus|Czechia|Czech Republic|Democratic Republic of the Congo|Denmark|Djibouti|Egypt|Equatorial Guinea|Eritrea|Estonia|Ethiopia|Faroe Islands|Finland|France|Gabon|Gambia|Georgia|Germany|Ghana|Gibraltar|Greece|Guernsey|Guinea|Hungary|Iceland|Iran|Iraq|Ireland|Israel|Italy|Ivory Coast|Jordan|Kazakhstan|Kenya|Kosovo|Kuwait|Latvia|Lebanon|Lesotho|Liberia|Libya|Liechtenstein|Lithuania|Luxembourg|Macedonia|Madagascar|Malawi|Mali|Malta|Mauritania|Mauritius|Moldova|Monaco|Montenegro|Morocco|Mozambique|Namibia|Netherlands|Niger|Nigeria|Norway|Poland|Qatar|Romania|Russia|Rwanda|San Marino|Saudi Arabia|Senegal|Serbia|Slovakia|Slovenia|Somalia|South Africa|Sudan|Swaziland|Sweden|Switzerland|Syria|Tanzania|Togo|Tunisia|Turkey|Uganda|Ukraine|United Arab Emirates|United Kingdom|Western Sahara|Yemen|Zambia|Zimbabwe'), 'Hungary', 
                   np.where(selfServiceScoreData_df['Country'].str.contains('India|Bangladesh'), 'India',
                   np.where(selfServiceScoreData_df['Country'].str.contains('Japan'), 'Japan',
                   np.where(selfServiceScoreData_df['Country'].str.contains('Spain|Portugal'), 'Spain',
                   np.where(selfServiceScoreData_df['Country'].str.contains('United States|Canada'), 'US',            
                               'other'))))))))


# In[ ]:


selfServiceScoreData_df.reset_index(level=selfServiceScoreData_df.index.names, inplace=True) 
selfServiceScoreData_df.head()


# In[ ]:


#Generate selfServiceScoreData table
selfServiceScoreData_table_dtypes = {
}

selfServiceScoreData_df.to_sql('selfservicescoredata', engine, index=False, chunksize=10000, dtype=selfServiceScoreData_table_dtypes)


# In[ ]:


#Adding indexes
with engine.begin() as connection:
    connection.execute('create unique index GA_INDEX_07 on selfservicescoredata(`index`)')
    connection.execute('create index GA_INDEX_08 on selfservicescoredata(MonthofYear)')


# In[ ]:


del countries
del hostname
del year
del monthYear
del userRole
del sessions
del users
del searchExits
del searchRefinements
del searchResultViews
del searchSessions
del searchUniques


# In[ ]:


del sSDAllRows
del selfServiceScoreData_df


# ## 4. Generate Ticket User Data

# In[ ]:


#ticketUserData = get_ticketUserData(analytics, '2018-12-01', 'today')

for year, month in monthlist(dates):
    getMonthData(year, month, get_ticketUserData)


# In[ ]:


countries = [v['dimensions'][0] for v in tUDAllRows]
hostname = [v['dimensions'][1] for v in tUDAllRows]
monthYear =[v['dimensions'][2] for v in tUDAllRows]
userRole =[v['dimensions'][3] for v in tUDAllRows]
sessions = [v['metrics'][0]['values'][0] for v in tUDAllRows]
users = [v['metrics'][0]['values'][1] for v in tUDAllRows]


# In[ ]:


ticketUserData_df = pd.DataFrame()
ticketUserData_df['Country'] = countries
ticketUserData_df['Hostname'] = hostname
ticketUserData_df['MonthofYear'] = monthYear
ticketUserData_df['UserRole'] = userRole
ticketUserData_df['Sessions'] = sessions
ticketUserData_df['Users'] = users


# In[ ]:


int_cols = ['MonthofYear', 'Sessions', 'Users']
for col in int_cols:
    ticketUserData_df[col] = pd.to_numeric(ticketUserData_df[col], errors='coerce')


# In[ ]:


# Adding custom fields
ticketUserData_df['Date'] = pd.to_datetime(ticketUserData_df['MonthofYear'], format = '%Y%m')
ticketUserData_df['SupportRegion'] = np.where(ticketUserData_df['Country'].str.contains('Australia'), 'Australia',
                   np.where(ticketUserData_df['Country'].str.contains('Belize|Costa Rica|El Salvador|Guatemala|Honduras|Jamaica|Mexico|Nicaragua|Panama|Argentina|Bolivia|Brazil|Chile|Colombia|Ecuador|Paraguay|Peru|Uraguay|Uruguay|Venezuela|Cuba|Dominican Republic|Haiti'), 'Brazil',
                   np.where(ticketUserData_df['Country'].str.contains('Cambodia|China|Hong Kong|Indonesia|Laos|Macau|Malaysia|Myanmar|New Zealand|Philippines|Pakistan|Singapore|South Korea|Sri Lanka|Taiwan|Thailand|Vietnam'), 'China', 
                   np.where(ticketUserData_df['Country'].str.contains('lbania|Algeria|Andorra|Angola|Austria|Bahrain|Belarus|Belgium|Benin|Bosnia and Herzegovina|Bosnia & Herzegovina|Botswana|Bulgaria|Burkina Faso|Burundi|Cameroon|Cape Verde|Central African Republic|Chad|Comoros|Croatia|Cyprus|Czechia|Czech Republic|Democratic Republic of the Congo|Denmark|Djibouti|Egypt|Equatorial Guinea|Eritrea|Estonia|Ethiopia|Faroe Islands|Finland|France|Gabon|Gambia|Georgia|Germany|Ghana|Gibraltar|Greece|Guernsey|Guinea|Hungary|Iceland|Iran|Iraq|Ireland|Israel|Italy|Ivory Coast|Jordan|Kazakhstan|Kenya|Kosovo|Kuwait|Latvia|Lebanon|Lesotho|Liberia|Libya|Liechtenstein|Lithuania|Luxembourg|Macedonia|Madagascar|Malawi|Mali|Malta|Mauritania|Mauritius|Moldova|Monaco|Montenegro|Morocco|Mozambique|Namibia|Netherlands|Niger|Nigeria|Norway|Poland|Qatar|Romania|Russia|Rwanda|San Marino|Saudi Arabia|Senegal|Serbia|Slovakia|Slovenia|Somalia|South Africa|Sudan|Swaziland|Sweden|Switzerland|Syria|Tanzania|Togo|Tunisia|Turkey|Uganda|Ukraine|United Arab Emirates|United Kingdom|Western Sahara|Yemen|Zambia|Zimbabwe'), 'Hungary', 
                   np.where(ticketUserData_df['Country'].str.contains('India|Bangladesh'), 'India',
                   np.where(ticketUserData_df['Country'].str.contains('Japan'), 'Japan',
                   np.where(ticketUserData_df['Country'].str.contains('Spain|Portugal'), 'Spain',
                   np.where(ticketUserData_df['Country'].str.contains('United States|Canada'), 'US',            
                               'other'))))))))


# In[ ]:


#ticketUserData_df['index1'] = ticketUserData_df.index
ticketUserData_df.reset_index(level=ticketUserData_df.index.names, inplace=True) 
ticketUserData_df.head()


# In[ ]:


#Generate ticketUserData table
ticketUserData_table_dtypes = {
}

ticketUserData_df.to_sql('ticketuserdata', engine, index=False, chunksize=10000, dtype=ticketUserData_table_dtypes)


# In[ ]:


#Adding indexes
with engine.begin() as connection:
    connection.execute('create unique index GA_INDEX_09 on ticketuserdata(`index`)')
    connection.execute('create index GA_INDEX_10 on ticketuserdata(MonthofYear)')


# In[ ]:


del countries
del hostname
del monthYear
del userRole
del sessions
del users


# In[ ]:


del tUDAllRows
del ticketUserData_df


# ## 5. Generate Ticket Form Deflection Data

# In[ ]:


for year, month in monthlist(dates):
    getMonthData(year, month, get_ticketFormDeflectionData)


# In[ ]:


countries = [v['dimensions'][0] for v in tfDAllRows]
exitPage = [v['dimensions'][1] for v in tfDAllRows]
hostname = [v['dimensions'][2] for v in tfDAllRows]
pageTitle = [v['dimensions'][3] for v in tfDAllRows]
monthYear = [v['dimensions'][4] for v in tfDAllRows]
previousPagePath = [v['dimensions'][5] for v in tfDAllRows]
userRole = [v['dimensions'][6] for v in tfDAllRows]
exits = [v['metrics'][0]['values'][0] for v in tfDAllRows]
sessions = [v['metrics'][0]['values'][1] for v in tfDAllRows]
users = [v['metrics'][0]['values'][2] for v in tfDAllRows]


# In[ ]:


ticketFormDeflectionData_df = pd.DataFrame()
ticketFormDeflectionData_df['Country'] = countries
ticketFormDeflectionData_df['ExitPage'] = exitPage
ticketFormDeflectionData_df['Hostname'] = hostname
ticketFormDeflectionData_df['PageTitle'] = pageTitle
ticketFormDeflectionData_df['MonthofYear'] = monthYear
ticketFormDeflectionData_df['PreviousPagePath'] = previousPagePath
ticketFormDeflectionData_df['UserRole'] = userRole
ticketFormDeflectionData_df['Exits'] = exits
ticketFormDeflectionData_df['Sessions'] = sessions
ticketFormDeflectionData_df['Users'] = users


# In[ ]:


# Adding custom fields
ticketFormDeflectionData_df['ArticleId_ExitPage'] = ticketFormDeflectionData_df['ExitPage'].str.extract('^.*articles\/([0-9]{12})', expand = False)
ticketFormDeflectionData_df['LocaleCode_ExitPage'] = ticketFormDeflectionData_df['ExitPage'].str.extract('\/hc\/(en-us|es|zh-cn|ja|pt)\/', expand = False)
ticketFormDeflectionData_df['TicketId'] = ticketFormDeflectionData_df['PreviousPagePath'].str.extract('^.*requests\/([0-9]{3,6})', expand = False)
ticketFormDeflectionData_df['Date'] = pd.to_datetime(ticketFormDeflectionData_df['MonthofYear'], format = '%Y%m')
ticketFormDeflectionData_df['SupportRegion'] = np.where(ticketFormDeflectionData_df['Country'].str.contains('Australia'), 'Australia',
                   np.where(ticketFormDeflectionData_df['Country'].str.contains('Belize|Costa Rica|El Salvador|Guatemala|Honduras|Jamaica|Mexico|Nicaragua|Panama|Argentina|Bolivia|Brazil|Chile|Colombia|Ecuador|Paraguay|Peru|Uraguay|Uruguay|Venezuela|Cuba|Dominican Republic|Haiti'), 'Brazil',
                   np.where(ticketFormDeflectionData_df['Country'].str.contains('Cambodia|China|Hong Kong|Indonesia|Laos|Macau|Malaysia|Myanmar|New Zealand|Philippines|Pakistan|Singapore|South Korea|Sri Lanka|Taiwan|Thailand|Vietnam'), 'China', 
                   np.where(ticketFormDeflectionData_df['Country'].str.contains('lbania|Algeria|Andorra|Angola|Austria|Bahrain|Belarus|Belgium|Benin|Bosnia and Herzegovina|Bosnia & Herzegovina|Botswana|Bulgaria|Burkina Faso|Burundi|Cameroon|Cape Verde|Central African Republic|Chad|Comoros|Croatia|Cyprus|Czechia|Czech Republic|Democratic Republic of the Congo|Denmark|Djibouti|Egypt|Equatorial Guinea|Eritrea|Estonia|Ethiopia|Faroe Islands|Finland|France|Gabon|Gambia|Georgia|Germany|Ghana|Gibraltar|Greece|Guernsey|Guinea|Hungary|Iceland|Iran|Iraq|Ireland|Israel|Italy|Ivory Coast|Jordan|Kazakhstan|Kenya|Kosovo|Kuwait|Latvia|Lebanon|Lesotho|Liberia|Libya|Liechtenstein|Lithuania|Luxembourg|Macedonia|Madagascar|Malawi|Mali|Malta|Mauritania|Mauritius|Moldova|Monaco|Montenegro|Morocco|Mozambique|Namibia|Netherlands|Niger|Nigeria|Norway|Poland|Qatar|Romania|Russia|Rwanda|San Marino|Saudi Arabia|Senegal|Serbia|Slovakia|Slovenia|Somalia|South Africa|Sudan|Swaziland|Sweden|Switzerland|Syria|Tanzania|Togo|Tunisia|Turkey|Uganda|Ukraine|United Arab Emirates|United Kingdom|Western Sahara|Yemen|Zambia|Zimbabwe'), 'Hungary', 
                   np.where(ticketFormDeflectionData_df['Country'].str.contains('India|Bangladesh'), 'India',
                   np.where(ticketFormDeflectionData_df['Country'].str.contains('Japan'), 'Japan',
                   np.where(ticketFormDeflectionData_df['Country'].str.contains('Spain|Portugal'), 'Spain',
                   np.where(ticketFormDeflectionData_df['Country'].str.contains('United States|Canada'), 'US',            
                               'other'))))))))


# In[ ]:


int_cols = ['MonthofYear', 'Exits', 'Sessions', 'Users']
for col in int_cols:
    ticketFormDeflectionData_df[col] = pd.to_numeric(ticketFormDeflectionData_df[col], errors='coerce')


# In[ ]:


ticketFormDeflectionData_df.reset_index(level=ticketFormDeflectionData_df.index.names, inplace=True) 
ticketFormDeflectionData_df.head()


# In[ ]:


#Generate ticketFormDeflectionData table
ticketformdeflectiondata_table_dtypes = {
}

ticketFormDeflectionData_df.to_sql('ticketformdefl', engine, index=False, chunksize=10000, dtype=ticketformdeflectiondata_table_dtypes)


# In[ ]:


#Adding indexes
with engine.begin() as connection:
    connection.execute('create unique index GA_INDEX_11 on ticketformdefl(`index`)')
    connection.execute('create index GA_INDEX_12 on ticketformdefl(MonthofYear)')


# In[ ]:


del countries
del exitPage
del hostname
del pageTitle
del monthYear
del previousPagePath
del userRole
del exits
del sessions
del users


# In[ ]:


del tfDAllRows
del ticketFormDeflectionData_df


# ## 6. Generate Ticket Form Session Data

# In[ ]:


for year, month in monthlist(dates_button):
    getMonthData(year, month, get_ticketFormSessionData)


# In[ ]:


countries = [v['dimensions'][0] for v in tfSAllRows]
hostname = [v['dimensions'][1] for v in tfSAllRows]
year = [v['dimensions'][2] for v in tfSAllRows]
monthYear =[v['dimensions'][3] for v in tfSAllRows]
userRole =[v['dimensions'][4] for v in tfSAllRows]
sessions = [v['metrics'][0]['values'][0] for v in tfSAllRows]
users = [v['metrics'][0]['values'][1] for v in tfSAllRows]


# In[ ]:


ticketFormSessionData_df = pd.DataFrame()
ticketFormSessionData_df['Country'] = countries
ticketFormSessionData_df['Hostname'] = hostname
ticketFormSessionData_df['Year'] = year
ticketFormSessionData_df['MonthofYear'] = monthYear
ticketFormSessionData_df['UserRole'] = userRole
ticketFormSessionData_df['Sessions'] = sessions
ticketFormSessionData_df['Users'] = users


# In[ ]:


int_cols = ['Year', 'MonthofYear', 'Sessions', 'Users']
for col in int_cols:
    ticketFormSessionData_df[col] = pd.to_numeric(ticketFormSessionData_df[col], errors='coerce')


# In[ ]:


# Adding custom fields
ticketFormSessionData_df['Date'] = pd.to_datetime(ticketFormSessionData_df['MonthofYear'], format = '%Y%m')
ticketFormSessionData_df['SupportRegion'] = np.where(ticketFormSessionData_df['Country'].str.contains('Australia'), 'Australia',
                   np.where(ticketFormSessionData_df['Country'].str.contains('Belize|Costa Rica|El Salvador|Guatemala|Honduras|Jamaica|Mexico|Nicaragua|Panama|Argentina|Bolivia|Brazil|Chile|Colombia|Ecuador|Paraguay|Peru|Uraguay|Uruguay|Venezuela|Cuba|Dominican Republic|Haiti'), 'Brazil',
                   np.where(ticketFormSessionData_df['Country'].str.contains('Cambodia|China|Hong Kong|Indonesia|Laos|Macau|Malaysia|Myanmar|New Zealand|Philippines|Pakistan|Singapore|South Korea|Sri Lanka|Taiwan|Thailand|Vietnam'), 'China', 
                   np.where(ticketFormSessionData_df['Country'].str.contains('lbania|Algeria|Andorra|Angola|Austria|Bahrain|Belarus|Belgium|Benin|Bosnia and Herzegovina|Bosnia & Herzegovina|Botswana|Bulgaria|Burkina Faso|Burundi|Cameroon|Cape Verde|Central African Republic|Chad|Comoros|Croatia|Cyprus|Czechia|Czech Republic|Democratic Republic of the Congo|Denmark|Djibouti|Egypt|Equatorial Guinea|Eritrea|Estonia|Ethiopia|Faroe Islands|Finland|France|Gabon|Gambia|Georgia|Germany|Ghana|Gibraltar|Greece|Guernsey|Guinea|Hungary|Iceland|Iran|Iraq|Ireland|Israel|Italy|Ivory Coast|Jordan|Kazakhstan|Kenya|Kosovo|Kuwait|Latvia|Lebanon|Lesotho|Liberia|Libya|Liechtenstein|Lithuania|Luxembourg|Macedonia|Madagascar|Malawi|Mali|Malta|Mauritania|Mauritius|Moldova|Monaco|Montenegro|Morocco|Mozambique|Namibia|Netherlands|Niger|Nigeria|Norway|Poland|Qatar|Romania|Russia|Rwanda|San Marino|Saudi Arabia|Senegal|Serbia|Slovakia|Slovenia|Somalia|South Africa|Sudan|Swaziland|Sweden|Switzerland|Syria|Tanzania|Togo|Tunisia|Turkey|Uganda|Ukraine|United Arab Emirates|United Kingdom|Western Sahara|Yemen|Zambia|Zimbabwe'), 'Hungary', 
                   np.where(ticketFormSessionData_df['Country'].str.contains('India|Bangladesh'), 'India',
                   np.where(ticketFormSessionData_df['Country'].str.contains('Japan'), 'Japan',
                   np.where(ticketFormSessionData_df['Country'].str.contains('Spain|Portugal'), 'Spain',
                   np.where(ticketFormSessionData_df['Country'].str.contains('United States|Canada'), 'US',            
                               'other'))))))))


# In[ ]:


ticketFormSessionData_df.reset_index(level=ticketFormSessionData_df.index.names, inplace=True) 
ticketFormSessionData_df.head()


# In[ ]:


#Generate ticketFormSessionData table
ticketFormSessionData_table_dtypes = {
}

ticketFormSessionData_df.to_sql('ticketformsession', engine, index=False, chunksize=10000, dtype=ticketFormSessionData_table_dtypes)


# In[ ]:


#Adding indexes
with engine.begin() as connection:
    connection.execute('create unique index GA_INDEX_13 on ticketformsession(`index`)')
    connection.execute('create index GA_INDEX_14 on ticketformsession(MonthofYear)')


# In[ ]:


del countries
del hostname
del year
del monthYear
del userRole
del sessions
del users


# In[ ]:


del tfSAllRows
del ticketFormSessionData_df


# ## 7. Generate Missed Ticket Form Deflection Data

# In[ ]:


for year, month in monthlist(dates_button):
    getMonthData(year, month, get_missedTicketFormDeflectionData)


# In[ ]:


countries = [v['dimensions'][0] for v in mtfDAllRows]
exitPage = [v['dimensions'][1] for v in mtfDAllRows]
hostname = [v['dimensions'][2] for v in mtfDAllRows]
pageTitle = [v['dimensions'][3] for v in mtfDAllRows]
monthYear = [v['dimensions'][4] for v in mtfDAllRows]
previousPagePath = [v['dimensions'][5] for v in mtfDAllRows]
userRole = [v['dimensions'][6] for v in mtfDAllRows]
exits = [v['metrics'][0]['values'][0] for v in mtfDAllRows]
sessions = [v['metrics'][0]['values'][1] for v in mtfDAllRows]
users = [v['metrics'][0]['values'][2] for v in mtfDAllRows]


# In[ ]:


missedticketFormDeflectionData_df = pd.DataFrame()
missedticketFormDeflectionData_df['Country'] = countries
missedticketFormDeflectionData_df['ExitPage'] = exitPage
missedticketFormDeflectionData_df['Hostname'] = hostname
missedticketFormDeflectionData_df['PageTitle'] = pageTitle
missedticketFormDeflectionData_df['MonthofYear'] = monthYear
missedticketFormDeflectionData_df['PreviousPagePath'] = previousPagePath
missedticketFormDeflectionData_df['UserRole'] = userRole
missedticketFormDeflectionData_df['Exits'] = exits
missedticketFormDeflectionData_df['Sessions'] = sessions
missedticketFormDeflectionData_df['Users'] = users


# In[ ]:


# Adding custom fields
missedticketFormDeflectionData_df['ArticleId_ExitPage'] = missedticketFormDeflectionData_df['ExitPage'].str.extract('^.*articles\/([0-9]{12})', expand = False)
missedticketFormDeflectionData_df['LocaleCode_ExitPage'] = missedticketFormDeflectionData_df['ExitPage'].str.extract('\/hc\/(en-us|es|zh-cn|ja|pt)\/', expand = False)
missedticketFormDeflectionData_df['TicketId'] = missedticketFormDeflectionData_df['PreviousPagePath'].str.extract('^.*requests\/([0-9]{3,6})', expand = False)
missedticketFormDeflectionData_df['Date'] = pd.to_datetime(missedticketFormDeflectionData_df['MonthofYear'], format = '%Y%m')
missedticketFormDeflectionData_df['SupportRegion'] = np.where(missedticketFormDeflectionData_df['Country'].str.contains('Australia'), 'Australia',
                   np.where(missedticketFormDeflectionData_df['Country'].str.contains('Belize|Costa Rica|El Salvador|Guatemala|Honduras|Jamaica|Mexico|Nicaragua|Panama|Argentina|Bolivia|Brazil|Chile|Colombia|Ecuador|Paraguay|Peru|Uraguay|Uruguay|Venezuela|Cuba|Dominican Republic|Haiti'), 'Brazil',
                   np.where(missedticketFormDeflectionData_df['Country'].str.contains('Cambodia|China|Hong Kong|Indonesia|Laos|Macau|Malaysia|Myanmar|New Zealand|Philippines|Pakistan|Singapore|South Korea|Sri Lanka|Taiwan|Thailand|Vietnam'), 'China', 
                   np.where(missedticketFormDeflectionData_df['Country'].str.contains('lbania|Algeria|Andorra|Angola|Austria|Bahrain|Belarus|Belgium|Benin|Bosnia and Herzegovina|Bosnia & Herzegovina|Botswana|Bulgaria|Burkina Faso|Burundi|Cameroon|Cape Verde|Central African Republic|Chad|Comoros|Croatia|Cyprus|Czechia|Czech Republic|Democratic Republic of the Congo|Denmark|Djibouti|Egypt|Equatorial Guinea|Eritrea|Estonia|Ethiopia|Faroe Islands|Finland|France|Gabon|Gambia|Georgia|Germany|Ghana|Gibraltar|Greece|Guernsey|Guinea|Hungary|Iceland|Iran|Iraq|Ireland|Israel|Italy|Ivory Coast|Jordan|Kazakhstan|Kenya|Kosovo|Kuwait|Latvia|Lebanon|Lesotho|Liberia|Libya|Liechtenstein|Lithuania|Luxembourg|Macedonia|Madagascar|Malawi|Mali|Malta|Mauritania|Mauritius|Moldova|Monaco|Montenegro|Morocco|Mozambique|Namibia|Netherlands|Niger|Nigeria|Norway|Poland|Qatar|Romania|Russia|Rwanda|San Marino|Saudi Arabia|Senegal|Serbia|Slovakia|Slovenia|Somalia|South Africa|Sudan|Swaziland|Sweden|Switzerland|Syria|Tanzania|Togo|Tunisia|Turkey|Uganda|Ukraine|United Arab Emirates|United Kingdom|Western Sahara|Yemen|Zambia|Zimbabwe'), 'Hungary', 
                   np.where(missedticketFormDeflectionData_df['Country'].str.contains('India|Bangladesh'), 'India',
                   np.where(missedticketFormDeflectionData_df['Country'].str.contains('Japan'), 'Japan',
                   np.where(missedticketFormDeflectionData_df['Country'].str.contains('Spain|Portugal'), 'Spain',
                   np.where(missedticketFormDeflectionData_df['Country'].str.contains('United States|Canada'), 'US',            
                               'other'))))))))


# In[ ]:


int_cols = ['MonthofYear', 'Exits', 'Sessions', 'Users']
for col in int_cols:
    missedticketFormDeflectionData_df[col] = pd.to_numeric(missedticketFormDeflectionData_df[col], errors='coerce')


# In[ ]:


missedticketFormDeflectionData_df.reset_index(level=missedticketFormDeflectionData_df.index.names, inplace=True) 
missedticketFormDeflectionData_df.head()


# In[ ]:


#Generate missedticketFormDeflectionData table
missedticketFormDeflectionData_table_dtypes = {
}

missedticketFormDeflectionData_df.to_sql('missedticketformdefl', engine, chunksize=10000, index=False, dtype=missedticketFormDeflectionData_table_dtypes)


# In[ ]:


#Adding indexes
with engine.begin() as connection:
    connection.execute('create unique index GA_INDEX_15 on missedticketformdefl(`index`)')
    connection.execute('create index GA_INDEX_16 on missedticketformdefl(MonthofYear)')


# In[ ]:


del countries
del exitPage
del hostname
del pageTitle
del monthYear
del previousPagePath
del userRole
del exits
del sessions
del users


# In[ ]:


del mtfDAllRows
del missedticketFormDeflectionData_df


# ## 8. Generate Missed Self-Service Deflection Data

# In[ ]:


for year, month in monthlist(dates):
    getMonthData(year, month, get_missedSelfServiceDeflectionData)


# In[ ]:


countries = [v['dimensions'][0] for v in msSDAllRows]
exitPage = [v['dimensions'][1] for v in msSDAllRows]
hostname = [v['dimensions'][2] for v in msSDAllRows]
pageTitle = [v['dimensions'][3] for v in msSDAllRows]
monthYear = [v['dimensions'][4] for v in msSDAllRows]
previousPagePath = [v['dimensions'][5] for v in msSDAllRows]
userRole = [v['dimensions'][6] for v in msSDAllRows]
exits = [v['metrics'][0]['values'][0] for v in msSDAllRows]
sessions = [v['metrics'][0]['values'][1] for v in msSDAllRows]
users = [v['metrics'][0]['values'][2] for v in msSDAllRows]


# In[ ]:


missedSelfServiceDeflectionData_df = pd.DataFrame()
missedSelfServiceDeflectionData_df['Country'] = countries
missedSelfServiceDeflectionData_df['ExitPage'] = exitPage
missedSelfServiceDeflectionData_df['Hostname'] = hostname
missedSelfServiceDeflectionData_df['PageTitle'] = pageTitle
missedSelfServiceDeflectionData_df['MonthofYear'] = monthYear
missedSelfServiceDeflectionData_df['PreviousPagePath'] = previousPagePath
missedSelfServiceDeflectionData_df['UserRole'] = userRole
missedSelfServiceDeflectionData_df['Exits'] = exits
missedSelfServiceDeflectionData_df['Sessions'] = sessions
missedSelfServiceDeflectionData_df['Users'] = users


# In[ ]:


# Adding custom fields
missedSelfServiceDeflectionData_df['ArticleId_ExitPage'] = missedSelfServiceDeflectionData_df['ExitPage'].str.extract('^.*articles\/([0-9]{12})', expand = False)
missedSelfServiceDeflectionData_df['LocaleCode_ExitPage'] = missedSelfServiceDeflectionData_df['ExitPage'].str.extract('\/hc\/(en-us|es|zh-cn|ja|pt)\/', expand = False)
missedSelfServiceDeflectionData_df['TicketId'] = missedSelfServiceDeflectionData_df['PreviousPagePath'].str.extract('^.*requests\/([0-9]{3,6})', expand = False)
missedSelfServiceDeflectionData_df['Date'] = pd.to_datetime(missedSelfServiceDeflectionData_df['MonthofYear'], format = '%Y%m')
missedSelfServiceDeflectionData_df['SupportRegion'] = np.where(missedSelfServiceDeflectionData_df['Country'].str.contains('Australia'), 'Australia',
                   np.where(missedSelfServiceDeflectionData_df['Country'].str.contains('Belize|Costa Rica|El Salvador|Guatemala|Honduras|Jamaica|Mexico|Nicaragua|Panama|Argentina|Bolivia|Brazil|Chile|Colombia|Ecuador|Paraguay|Peru|Uraguay|Uruguay|Venezuela|Cuba|Dominican Republic|Haiti'), 'Brazil',
                   np.where(missedSelfServiceDeflectionData_df['Country'].str.contains('Cambodia|China|Hong Kong|Indonesia|Laos|Macau|Malaysia|Myanmar|New Zealand|Philippines|Pakistan|Singapore|South Korea|Sri Lanka|Taiwan|Thailand|Vietnam'), 'China', 
                   np.where(missedSelfServiceDeflectionData_df['Country'].str.contains('lbania|Algeria|Andorra|Angola|Austria|Bahrain|Belarus|Belgium|Benin|Bosnia and Herzegovina|Bosnia & Herzegovina|Botswana|Bulgaria|Burkina Faso|Burundi|Cameroon|Cape Verde|Central African Republic|Chad|Comoros|Croatia|Cyprus|Czechia|Czech Republic|Democratic Republic of the Congo|Denmark|Djibouti|Egypt|Equatorial Guinea|Eritrea|Estonia|Ethiopia|Faroe Islands|Finland|France|Gabon|Gambia|Georgia|Germany|Ghana|Gibraltar|Greece|Guernsey|Guinea|Hungary|Iceland|Iran|Iraq|Ireland|Israel|Italy|Ivory Coast|Jordan|Kazakhstan|Kenya|Kosovo|Kuwait|Latvia|Lebanon|Lesotho|Liberia|Libya|Liechtenstein|Lithuania|Luxembourg|Macedonia|Madagascar|Malawi|Mali|Malta|Mauritania|Mauritius|Moldova|Monaco|Montenegro|Morocco|Mozambique|Namibia|Netherlands|Niger|Nigeria|Norway|Poland|Qatar|Romania|Russia|Rwanda|San Marino|Saudi Arabia|Senegal|Serbia|Slovakia|Slovenia|Somalia|South Africa|Sudan|Swaziland|Sweden|Switzerland|Syria|Tanzania|Togo|Tunisia|Turkey|Uganda|Ukraine|United Arab Emirates|United Kingdom|Western Sahara|Yemen|Zambia|Zimbabwe'), 'Hungary', 
                   np.where(missedSelfServiceDeflectionData_df['Country'].str.contains('India|Bangladesh'), 'India',
                   np.where(missedSelfServiceDeflectionData_df['Country'].str.contains('Japan'), 'Japan',
                   np.where(missedSelfServiceDeflectionData_df['Country'].str.contains('Spain|Portugal'), 'Spain',
                   np.where(missedSelfServiceDeflectionData_df['Country'].str.contains('United States|Canada'), 'US',            
                               'other'))))))))


# In[ ]:


int_cols = ['MonthofYear', 'Exits', 'Sessions', 'Users']
for col in int_cols:
    missedSelfServiceDeflectionData_df[col] = pd.to_numeric(missedSelfServiceDeflectionData_df[col], errors='coerce')


# In[ ]:


missedSelfServiceDeflectionData_df.reset_index(level=missedSelfServiceDeflectionData_df.index.names, inplace=True) 
missedSelfServiceDeflectionData_df.head()


# In[ ]:


#Generate missedSelfServiceDeflectionData table
missedSelfServiceDeflectionData_table_dtypes = {
}

missedSelfServiceDeflectionData_df.to_sql('missedselfservicedefl', engine, chunksize=10000, index=False, dtype=missedSelfServiceDeflectionData_table_dtypes)


# In[ ]:


#Adding indexes
with engine.begin() as connection:
    connection.execute('create unique index GA_INDEX_17 on missedselfservicedefl(`index`)')
    connection.execute('create index GA_INDEX_18 on missedselfservicedefl(MonthofYear)')


# In[ ]:


del countries
del exitPage
del hostname
del pageTitle
del monthYear
del previousPagePath
del userRole
del exits
del sessions
del users


# In[ ]:


del msSDAllRows
del missedSelfServiceDeflectionData_df


# In[ ]:


log('GA Extract complete')

