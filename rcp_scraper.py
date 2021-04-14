'''
Real Clear Politics Web Scaper to Retrieve Polling Data
'''
from bs4 import BeautifulSoup
import requests
import pandas as pd

# Helper function for cleaning (date spliting)
def split_date(df):
    new_col_start = []
    new_col_end = []
    for date in df['Date']:
        new_col_start.append(date.split('-')[0].strip())
        new_col_end.append(date.split('-')[1].strip())
    df['Start_Date'] = new_col_start
    df['End_Date'] = new_col_end 
    df.drop(columns='Date', inplace=True)
    return df

def split_sample(df):
    sample_size = []
    type = []
    for sample in df['Sample']:
        if sample == 'LV' or sample == 'RV':
            sample_size.append(0)
            type.append(sample)
        else:
            sample_size.append(sample.split()[0].strip())
            type.append(sample.split()[1].strip())
    df['Sample_Size'] = sample_size
    df['Type'] = type
    df.drop(columns='Sample', inplace=True)
    df['Sample_Size'] = df['Sample_Size'].astype('int64')
    df['Type'].replace(['RV','LV'],['Registered Voters','Likely Voters'], inplace=True)
    return df

def split_spread(df):
    spreads = []
    leaders = []
    for spread in df['Spread']:
        spreads.append(float(spread.split()[1].strip()))
        leaders.append(spread.split()[0].strip())
    df['Spread'] = spreads
    df['Leader'] = leaders
    return df

# SCRAPER FUNCTIONS
 
def scrap_trump_approval(ret='aggregate'):
    # Argument Check
    if ret not in ['aggregate', 'all', 'both']:
        raise Exception("Invalid input argument")
        
    URL = 'https://www.realclearpolitics.com/epolls/other/president_trump_job_approval-6179.html'
    page = requests.get(URL, timeout=0.1)
    soup = BeautifulSoup(page.content, 'lxml')
    tables = soup.find_all('table')
    df_aggregate = pd.read_html(str(tables[0]))[0]
    df_all = pd.read_html(str(tables[3]))[0]
    
    # Data Cleaning
    # Handle 'Tie' value
    df_aggregate.replace('Tie', 0, inplace=True)
    df_all.replace('Tie', 0, inplace=True)
    df_aggregate['Spread'] = df_aggregate['Spread'].astype('float64')
    df_all['Spread'] = df_all['Spread'].astype('float64')
    # Split Dates into Start and End
    df_aggregate = split_date(df_aggregate)
    df_all = split_date(df_all)
    # Remove RCP Average Row
    rcp_avg = df_aggregate.iloc[0].drop('Sample')
    df_aggregate = df_aggregate.drop(0).reset_index(drop=True)
    df_all = df_aggregate.drop(0).reset_index(drop=True)
    # Split Sample into Sample Size and Poll Type
    df_aggregate = split_sample(df_aggregate)
    df_all = split_sample(df_all)
    
    if ret == 'aggregate':
        return df_aggregate, rcp_avg
    if ret == 'all':
        return df_all, rcp_avg
    if ret == 'both':
        return df_aggregate, df_all, rcp_avg
    
def scrap_trump_biden_general(ret='aggregate'):
    # Argument Check
    if ret not in ['aggregate', 'all', 'both']:
        raise Exception("Invalid input argument")
        
    URL = 'https://www.realclearpolitics.com/epolls/2020/president/us/general_election_trump_vs_biden-6247.html'
    page = requests.get(URL, timeout=0.1)
    soup = BeautifulSoup(page.content, 'lxml')
    tables = soup.find_all('table')
    df_aggregate = pd.read_html(str(tables[0]))[0]
    df_all = pd.read_html(str(tables[3]))[0]
    
    # Clean Data
    # Clean Poll Names
    df_aggregate_names = ['RCP Average']
    df_all_names = ['RCP Average']
    as_agg = tables[0].find_all('a')
    as_all = tables[3].find_all('a')
    for a in as_agg:
        if a['class'][0] == 'normal_pollster_name':
            df_aggregate_names.append(a.string)
        else:
            continue
    for a in as_all:
        if a['class'][0] == 'normal_pollster_name':
            df_all_names.append(a.string)
    df_aggregate['Poll'] = df_aggregate_names
    df_all['Poll'] = df_all_names
    # Handle 'Tie' value
    df_aggregate.replace('Tie', 'Tie 0', inplace=True)
    df_all.replace('Tie', 'Tie 0', inplace=True)
    # Split spread into Spread and Leader
    df_aggregate = split_spread(df_aggregate)
    df_all = split_spread(df_all)
    # Split Dates into Start and End
    df_aggregate = split_date(df_aggregate)
    df_all = split_date(df_all)
    # Remove RCP Average Row
    rcp_avg = df_aggregate.iloc[0].drop(['Sample','MoE'])
    df_aggregate = df_aggregate.drop(0).reset_index(drop=True)
    df_all = df_all.drop(0).reset_index(drop=True)
    # Split Sample into Sample Size and Poll Type
    df_aggregate = split_sample(df_aggregate)
    df_all = split_sample(df_all) 
    # Handle MoE
    df_aggregate['MoE'].replace('--', None, inplace=True)
    df_all['MoE'].replace('--', None, inplace=True)
    
    if ret == 'aggregate':
        return df_aggregate, rcp_avg
    if ret == 'all':
        return df_all, rcp_avg
    if ret == 'both':
        return df_aggregate, df_all, rcp_avg  
    
def scrap_latest_polls(arg='all'):
    if arg not in ['all', 'election']:
        raise Exception("Invalid input argument")
    
    if arg == 'all':
        URL = 'https://www.realclearpolitics.com/epolls/latest_polls/'
    else:
        URL = 'https://www.realclearpolitics.com/epolls/latest_polls/elections/'
        
    page = requests.get(URL, timeout=0.1)
    soup = BeautifulSoup(page.content, 'lxml')
    tables = soup.find_all('table')
    """
    This scrap turns out a bit weird with how the tables get formated
    so a different approach is used in accessing the tables list to
    read to a pdf in that I only need to read index 1 to get every table
    except the first rather than read each individual index from 'tables'
    """
    # Initalization: get most recent date, init. an empty dataframe, a list of months
    date = tables[0].td.text
    df_full = pd.DataFrame()
    months = ['January','Feburary','March','April',
              'May','June','July','August','September',
              'October','November','December']
    # Read all tables into a list of pandas dataframes
    df_tables = pd.read_html(str(tables[1]))
    # Iterate through 'df_tables' to add a date column and concat tables to form
    # a full table
    for table in df_tables:
        # Check if table is a 'date' table and update date variable
        if table.iloc[0,0].split()[1] in months:
            date = table.iloc[0,0]
        else:
            # if statement only for first loop
            if df_full.empty:
                df_full = table
                df_full['Date'] = [date] * df_full.shape[0]
            else:
                table['Date'] = [date] * table.shape[0]
                df_full = pd.concat([df_full, table], ignore_index=True)
                
    return df_full
'''
df1, df2, rcp = scrap_trump_biden_general('both')

print(rcp)
print(df1)
print(df2)
'''
