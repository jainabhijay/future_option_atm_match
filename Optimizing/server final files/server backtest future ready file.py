import os
import pandas as pd
from pytz import timezone
from datetime import time, timedelta, datetime
import glob
from dateutil import parser
import subprocess

headers=["DateTime", "Ticker", "ExpiryDT", "Strike", "F&O", "Option", "Volume", "Open", "High", "Low", "Close", "OpenInterest"]
# df =pd.read_csv("FINNIFTY-I.NFO_2020-01-03.csv", header=None).reset_index(drop=True)
# df.columns=headers
# print(df.head(10))
ExeDF=pd.DataFrame(columns=["Symbol", "Pos", "Date", "Strike", "ExpiryDT", "Option", "EnTime", "SPrice", "ExTime","BPrice"])

OptionDf = pd.DataFrame()
PEOptionDf = pd.DataFrame()
FilteredDf = pd.DataFrame()
UnavailDateList = []
count=0; flag=None; curr_date=None
Pprice=0
high= float("-inf")
low = float("inf")


i=0
def query(**kwargs):
    """
    :param instrument: String
    :param expry_dt: Datetime
    :param strike: numpy int
    :param option_type: CE  PE
    :param start_date: In Datetime
    :param end_date: In Datetime
    """
    # instrument, f_o, expry_dt, strike, option_type, start_date, end_date)
    global ticker, UnavailDateList

    start_date = kwargs['start_date'].strftime("%Y-%m-%d") + 'T' + "09:15:00"
    end_date = kwargs['end_date'].strftime("%Y-%m-%d") + 'T' + "15:30:00"
    if kwargs['f_o'] == 'O':
        ticker = (kwargs['instrument'] + kwargs['expiry_dt'].strftime("%d%b%y") + str(kwargs['strike']) + kwargs[
            'option_type']).upper() + '.NFO'  # nfo FOR OHLCV
    elif kwargs['f_o'] == 'F':
        ticker = kwargs['instrument'] + '-I' + '.NFO'  #+kwargs['start_date'].strftime("%Y-%m-%d")

    print(ticker, start_date, end_date)
    try:
        subprocess.call(["/home/admin/query_client/query_ohlcv", ticker, start_date, end_date])

        # df = pd.read_csv(f"~/query_client/{ticker}.csv", parse_dates=['__time'])

        df = pd.read_csv(f"{ticker}.csv", header=None, low_memory=False).reset_index(drop=True)

        # print(df.head())

        df.columns = ['DateTime', 'Ticker', 'ExpiryDT', 'Strike', 'FnO', 'Option', 'Volume',
                    'Open', 'High', 'Low', 'Close', 'OI']
        # df['Time'] = pd.to_datetime((df['DateTime'])).apply(lambda x: x.time())

        df['Time'] = pd.to_datetime((df['DateTime'])).dt.strftime("%H:%M:%S")

        df["Date"] = pd.to_datetime((df['DateTime'])).dt.strftime("%Y-%m-%d")
        subprocess.call(['unlink', ticker + '.csv'])  # This deletes the file from storage after reading it to memory
        
        # print(df.tail())
        return df
    
    except Exception as e:

        print("Exception occured",e)
        df=pd.DataFrame()
        date = kwargs['start_date'].strftime("%Y-%m-%d")
        if date not in UnavailDateList:
            UnavailDateList.append(date)
        return df


def get_expiry(date):

    ExpDf = pd.read_excel("NIFTYData_20230626.xls")

    ExpDf["DataTime"] = pd.to_datetime(ExpDf["DataTime"])

    date=pd.to_datetime(date)

    mask = ExpDf["DataTime"] >= date
    
    # Find the index of the first occurrence of True in the mask
    next_greater_index = mask.idxmax()

    # Select the row with the next greater date
    next_greater_date_row = ExpDf.loc[next_greater_index]

    return next_greater_date_row["DataTime"]

#---------------------------------------------------------------------------------------------------

def calculate_ema_and_entry_points(resampled_data , future_df):
    resampled_data = future_df
    resampled_data['ema5'] = resampled_data['Close'].ewm(span=5).mean()
    # Define conditions for entry points
    resampled_data['Long_Call_Entry'] = resampled_data['Close'] > resampled_data['ema5']
    resampled_data['Long Call Entry Point'] = None
    resampled_data.loc[resampled_data['Long_Call_Entry'], 'Long Call Entry Point'] = "Enter the Long Call trade at - " + resampled_data['Close'].astype(str)
    resampled_data['Short_Put_Entry'] = resampled_data['Close'] < resampled_data['ema5']
    resampled_data['Short Put Entry Point'] = None
    resampled_data.loc[resampled_data['Short_Put_Entry'], 'Short Put Entry Point'] = "Enter the short put trade at - " + resampled_data['Close'].astype(str)
    # Determine Call or Put Status
    resampled_data['Call Or Put Status'] = None
    resampled_data.loc[resampled_data['Long_Call_Entry'], 'Call Or Put Status'] = "Buy Call and Sell Put at rounded off value (Price: " + (round(resampled_data['Close'] / 100) * 100).astype(str) + ")"
    resampled_data.loc[resampled_data['Short_Put_Entry'], 'Call Or Put Status'] = "Buy Put and Sell Call at rounded off value (Price: " + (round(resampled_data['Close'] / 100) * 100).astype(str) + ")"
    
    # Additional processing and cleanup
    resampled_data.reset_index(inplace=True)
    resampled_data = resampled_data[['Ticker', 'datetime', 'FnO', 'Volume', 'Open', 'High', 'Low', 'Close', 'ema5', 'Long_Call_Entry', 'Long Call Entry Point', 'Short_Put_Entry', 'Short Put Entry Point', 'Call Or Put Status']]
    print(resampled_data)
    #resampled_data.to_csv('/Users/abhijayjain/Desktop/internship/Indira Securities/F&O Prediction/hehe.csv')
    return resampled_data


def main():


    future_df = query(f_o='F', instrument='NIFTY', start_date=pd.to_datetime('2022-01-01'), end_date=pd.to_datetime('2022-01-05'), STime="09:15:00")
    #future_df = pd.read_csv('/Users/abhijayjain/Desktop/internship/Indira Securities/F&O Prediction/future.csv')
    dates = list(future_df["Date"].unique())
    resampled_data = []

    for i in dates:
        future_df = future_df.loc[future_df["Date"] == i].copy()
        future_df['datetime'] = future_df['Date'] + " " + future_df["Time"]
        future_df['datetime'] = pd.to_datetime(future_df['datetime'])
        future_df = future_df.set_index(['datetime']).resample("5T").agg({"Ticker": "first", "ExpiryDT": "first", "FnO": "first", "Volume": "sum", "Open": "first", "High": "max", "Low": "min", "Close": "last", "Time": "first", "Date": "first"})
        future_df = future_df.dropna()
        print(future_df)
        resampled_data.append(calculate_ema_and_entry_points(resampled_data, future_df))

if __name__ == "__main__":
    main()