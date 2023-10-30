import pandas as pd
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


#i=0
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
    
    next_greater_index = mask.idxmax()

    next_greater_date_row = ExpDf.loc[next_greater_index]

    return next_greater_date_row["DataTime"]

#---------------------------------------------------------------------------------------------------
def calculate_ema_and_entry_points_in_futures (one_day_future_df):
    resampled_data = one_day_future_df
    resampled_data['ema5'] = resampled_data['Close'].ewm(span=5).mean()
    resampled_data['Long_Call_Entry'] = resampled_data['Close'] > resampled_data['ema5']
    resampled_data['Long Call Entry Point'] = None
    resampled_data.loc[resampled_data['Long_Call_Entry'], 'Long Call Entry Point'] = "Enter the Long Call trade at - " + resampled_data['Close'].astype(str)
    resampled_data['Short_Put_Entry'] = resampled_data['Close'] < resampled_data['ema5']
    resampled_data['Short Put Entry Point'] = None
    resampled_data.loc[resampled_data['Short_Put_Entry'], 'Short Put Entry Point'] = "Enter the short put trade at - " + resampled_data['Close'].astype(str)
    resampled_data['Call Or Put Status'] = None
    resampled_data.loc[resampled_data['Long_Call_Entry'], 'Call Or Put Status'] = "Buy Call and Sell Put at rounded off value (Price: " + (round(resampled_data['Close'] / 100) * 100).astype(str) + ")"
    resampled_data.loc[resampled_data['Short_Put_Entry'], 'Call Or Put Status'] = "Buy Put and Sell Call at rounded off value (Price: " + (round(resampled_data['Close'] / 100) * 100).astype(str) + ")"
    resampled_data['Rounded Value'] = (round(resampled_data['Close'] / 100) * 100).astype(int)

    average_rounded_value = (resampled_data['Rounded Value'].astype(int)).mean()
    rounded_average = int(round(average_rounded_value / 100) * 100)

    resampled_data.reset_index(inplace=True)
    resampled_data = resampled_data[['Ticker', 'datetime', 'FnO', 'Volume', 'Open', 'High', 'Low', 'Close', 'ema5', 'Long_Call_Entry', 'Long Call Entry Point', 'Short_Put_Entry', 'Short Put Entry Point', 'Call Or Put Status', 'Rounded Value']]
    
    return rounded_average , resampled_data 

#---------------------------------------------------------------------------------------------------
def calculating_ema_of_option(CEOptionDf, PEOptionDf):
    
    call_5t_optiondf = CEOptionDf
    call_1t_optiondf = CEOptionDf
    put_5t_optiondf = PEOptionDf
    put_1t_optiondf = PEOptionDf

    call_5t_optiondf['ema5'] = call_5t_optiondf['Close'].ewm(span=5).mean()
    call_5t_optiondf['datetime'] =  call_5t_optiondf['Date'] + " " +  call_5t_optiondf["Time"]
    call_5t_optiondf['datetime'] = pd.to_datetime( call_5t_optiondf['datetime'])
    call_5t_optiondf =  call_5t_optiondf.set_index(['datetime']).resample("5T").agg({ "Strike": "first","Ticker": "first", "ExpiryDT": "first", "FnO": "first", "Volume": "sum", "Open": "first", "High": "max", "Low": "min", "Close": "last", "Time": "first", "Date": "first"})
    
    call_1t_optiondf['ema5'] = call_1t_optiondf['Close'].ewm(span=5).mean()
    call_1t_optiondf['datetime'] =  call_1t_optiondf['Date'] + " " +  call_1t_optiondf["Time"]
    call_1t_optiondf['datetime'] = pd.to_datetime( call_1t_optiondf['datetime'])
    call_1t_optiondf = call_1t_optiondf.set_index(['datetime']).resample("0.1T").agg({ "Strike": "first","Ticker": "first", "ExpiryDT": "first", "FnO": "first", "Volume": "sum", "Open": "first", "High": "max", "Low": "min", "Close": "last", "Time": "first", "Date": "first"})

    put_5t_optiondf['ema5'] = put_5t_optiondf['Close'].ewm(span=5).mean()
    put_5t_optiondf['datetime'] =  put_5t_optiondf['Date'] + " " +  put_5t_optiondf["Time"]
    put_5t_optiondf['datetime'] = pd.to_datetime( put_5t_optiondf['datetime'])
    put_5t_optiondf =  put_5t_optiondf.set_index(['datetime']).resample("5T").agg({ "Strike": "first","Ticker": "first", "ExpiryDT": "first", "FnO": "first", "Volume": "sum", "Open": "first", "High": "max", "Low": "min", "Close": "last", "Time": "first", "Date": "first"})
    
    put_1t_optiondf['ema5'] = put_1t_optiondf['Close'].ewm(span=5).mean()
    put_1t_optiondf['datetime'] =  put_1t_optiondf['Date'] + " " +  put_1t_optiondf["Time"]
    put_1t_optiondf['datetime'] = pd.to_datetime( put_1t_optiondf['datetime'])
    put_1t_optiondf = put_1t_optiondf.set_index(['datetime']).resample("0.1T").agg({ "Strike": "first","Ticker": "first", "ExpiryDT": "first", "FnO": "first", "Volume": "sum", "Open": "first", "High": "max", "Low": "min", "Close": "last", "Time": "first", "Date": "first"})

    return call_5t_optiondf, call_1t_optiondf, put_5t_optiondf, put_1t_optiondf

#---------------------------------------------------------------------------------------------------
def getting_the_strike(one_day_future_df):
    one_day_future_df['Datetime'] = pd.to_datetime(one_day_future_df['Date'] + ' ' + one_day_future_df['Time'], format='%Y-%m-%d %H:%M:%S')
    filtered_long_call_data = one_day_future_df[one_day_future_df['Long Call Entry Point'].notnull()]
    
    extracted_long_call_data_df = pd.DataFrame({
        'Rounded Value': filtered_long_call_data['Rounded Value'],
        'Long Call Entry Point': filtered_long_call_data['Long Call Entry Point'],
        'Time Slot': filtered_long_call_data['Datetime'].dt.strftime('%H:%M:%S')
    })

    filtered_short_put_data = one_day_future_df[one_day_future_df['Short Put Entry Point'].notnull()]

    extracted_short_put_data_df = pd.DataFrame({
        'Rounded Value': filtered_short_put_data['Rounded Value'],
        'Short Put Entry Point': filtered_short_put_data['Short Put Entry Point'],
        'Time Slot': filtered_short_put_data['Datetime'].dt.strftime('%H:%M:%S')
    })

    return extracted_long_call_data_df , extracted_short_put_data_df

#---------------------------------------------------------------------------------------------------
def matching_the_strike(call_5t_optiondf, put_5t_optiondf, extracted_long_call_data_df , extracted_short_put_data_df):
    call_5t_optiondf['Time'] = call_5t_optiondf['Time'].astype(str)
    extracted_long_call_data_df['Time Slot'] = extracted_long_call_data_df['Time Slot'].astype(str)

    call_5t_optiondf['Strike'] = call_5t_optiondf['Strike'].astype(float)  # Convert to float if it's a number
    extracted_long_call_data_df['Rounded Value'] = extracted_long_call_data_df['Rounded Value'].astype(float)

    put_5t_optiondf['Time'] = put_5t_optiondf['Time'].astype(str)
    extracted_short_put_data_df['Time Slot'] = extracted_short_put_data_df['Time Slot'].astype(str)

    put_5t_optiondf['Strike'] = put_5t_optiondf['Strike'].astype(float)  # Convert to float if it's a number
    extracted_short_put_data_df['Rounded Value'] = extracted_short_put_data_df['Rounded Value'].astype(float)
    
    extracted_long_call_data_df = extracted_long_call_data_df[extracted_long_call_data_df['Time Slot'] != '09:15:00']
    extracted_short_put_data_df = extracted_short_put_data_df[extracted_short_put_data_df['Time Slot'] != '09:15:00']

    call_merged_df = call_5t_optiondf.merge(extracted_long_call_data_df, left_on=['Time', 'Strike'], right_on=['Time Slot', 'Rounded Value'], how='inner')
    
    def update_call(row):
        if row['Long Call Entry Point']:
            return f"Buy Call"
        else:
            return f"Sell Call"

    call_merged_df['Buy/Sell Call'] = call_merged_df.apply(update_call, axis=1)
    
    put_merged_df = put_5t_optiondf.merge(extracted_short_put_data_df, left_on=['Time', 'Strike'], right_on=['Time Slot', 'Rounded Value'], how='inner')

    def update_put(row):
        if row['Short Put Entry Point']:
            return f"Buy Put"
        else:
            return f"Sell Put"

    put_merged_df['Buy/Sell Put'] = put_merged_df.apply(update_put, axis=1)
    column_order_call = [
        'Date',
        'Time',
        'Buy/Sell Call',
        'Open',
        'High',
        'Low',
        'Close',
    ]

    column_order_put = [
        'Date',
        'Time',
        'Buy/Sell Put',
        'Open',
        'High',
        'Low',
        'Close',
    ]

    call_merged_df = call_merged_df[column_order_call]
    put_merged_df = put_merged_df[column_order_put]
    # Concatenate the two DataFrames vertically
    merged_df = pd.concat([call_merged_df, put_merged_df], ignore_index=True)
    merged_df['trade'] = merged_df['Buy/Sell Call'].fillna('') + merged_df['Buy/Sell Put'].fillna('')

    # Sort the DataFrame by the 'Time' column
    merged_df['Time'] = pd.to_datetime(merged_df['Time'])
    merged_df['Time'] = merged_df['Time'].dt.strftime('%H:%M:%S')
    merged_df.sort_values(by='Time', inplace=True)
    

# Reset the index
    merged_df = merged_df.drop(['Buy/Sell Call', 'Buy/Sell Put'], axis=1)
    merged_df.reset_index(drop=True, inplace=True)
    # merged_df.to_csv('merged.csv')
    return merged_df
    

#---------------------------------------------------------------------------------------------------
#---------------------------------------------------------------------------------------------------
def buy_sell_execution(call_1t_optiondf, put_1t_optiondf ,merged_df):

    # Convert the 'Time' column to datetime format
    call_1t_optiondf['Time'] = pd.to_datetime(call_1t_optiondf['Time'], format='%H:%M:%S')
    put_1t_optiondf['Time'] = pd.to_datetime(put_1t_optiondf['Time'], format='%H:%M:%S')

    # Set the 'Time' column as the index
    call_1t_optiondf.set_index('Time', inplace=True)
    put_1t_optiondf.set_index('Time', inplace=True)

    # Resample to 1-second timeframe using asfreq
    call_1t_optiondf = call_1t_optiondf.asfreq('10S')
    put_1t_optiondf = put_1t_optiondf.asfreq('10S')

    # Reset the index
    call_1t_optiondf = call_1t_optiondf.reset_index()
    put_1t_optiondf = put_1t_optiondf.reset_index()

    call_1t_optiondf['Time'] = pd.to_datetime(call_1t_optiondf['Time'], format='%H:%M:%S').dt.time
    put_1t_optiondf['Time'] = pd.to_datetime(put_1t_optiondf['Time'], format='%H:%M:%S').dt.time
    merged_df['Time'] = pd.to_datetime(merged_df['Time'], format='%H:%M:%S').dt.time
    call_1t_optiondf.to_csv('call_1t_optiondf.csv')
    put_1t_optiondf.to_csv('put_1t_optiondf.csv')
    merged_df.to_csv('merged_df.csv')
    print(merged_df)
    
    # Initialize trading variables
    in_trade = None  # Use None to represent no trade initially
    entry_time = None
    entry_price = None
    exit_time = None
    exit_price = None
    last_exit_price = None
    trades = []

    # Loop through the rows of merged_df
    for index, row in merged_df.iterrows():
        action = row['trade']

        if action == "Buy Put":
            if in_trade is None:
                in_trade = "Put"  # Set the current trade type to "Put"
                entry_time = row['Time']
                entry_price = put_1t_optiondf.loc[put_1t_optiondf['Time'] == entry_time, 'Open'].values[0]
            elif in_trade == "Call":
                # Simultaneous exit of the call and entry of the put
                simultaneous_exit_time = row['Time']
                simultaneous_exit_price = call_1t_optiondf.loc[call_1t_optiondf['Time'] == simultaneous_exit_time, 'Open'].values[0]
                trades.append([row['Date'], entry_time, entry_price, simultaneous_exit_time, simultaneous_exit_price])
                in_trade = "Put"  # Set the current trade type to "Put"
                entry_time = row['Time']
                entry_price = put_1t_optiondf.loc[put_1t_optiondf['Time'] == entry_time, 'Open'].values[0]

        elif action == "Buy Call":
            if in_trade is None:
                in_trade = "Call"  # Set the current trade type to "Call"
                entry_time = row['Time']
                entry_price = call_1t_optiondf.loc[call_1t_optiondf['Time'] == entry_time, 'Open'].values[0]
            elif in_trade == "Put":
                # Simultaneous exit of the put and entry of the call
                simultaneous_exit_time = row['Time']
                simultaneous_exit_price = put_1t_optiondf.loc[put_1t_optiondf['Time'] == simultaneous_exit_time, 'Open'].values[0]
                trades.append([row['Date'], entry_time, entry_price, simultaneous_exit_time, simultaneous_exit_price])
                in_trade = "Call"  # Set the current trade type to "Call"
                entry_time = row['Time']
                entry_price = call_1t_optiondf.loc[call_1t_optiondf['Time'] == entry_time, 'Open'].values[0]

        elif in_trade is not None and action == f"Sell {in_trade}":
            exit_time = row['Time']
            if in_trade == "Call":
                exit_price = call_1t_optiondf.loc[call_1t_optiondf['Time'] == exit_time, 'Close'].values[0]
            else:
                exit_price = put_1t_optiondf.loc[put_1t_optiondf['Time'] == exit_time, 'Close'].values[0]
            trades.append([row['Date'], entry_time, entry_price, exit_time, exit_price])
            in_trade = None  # Reset the current trade type
            last_exit_price = exit_price  # Update last_exit_price

    # If there's an open trade at the end, close it at the last closing price of the respective option dataframe
    if in_trade is not None:
        if in_trade == "Call":
            last_option = call_1t_optiondf
        else:
            last_option = put_1t_optiondf

        last_exit_time = last_option['Time'].iloc[-1]
        last_exit_price = last_option['Close'].iloc[-1]
        trades.append([merged_df['Date'].iloc[-1], entry_time, entry_price, last_exit_time, last_exit_price])



    # Create a dataframe from the trades list
    trade_df = pd.DataFrame(trades, columns=['Date', 'Entry Time', 'Entry Price', 'Exit Time', 'Exit Price'])

    # Calculate profit or loss for each trade
    trade_df['Profit/Loss'] = (trade_df['Exit Price'] - trade_df['Entry Price'])*50

    # Display the final trading data
    print(trade_df)
    # Display the final trading data
    print("this the trade detail:\n",trade_df)
    trade_df.to_csv('trade.csv')
    return trade_df

#---------------------------------------------------------------------------------------------------
def main():
    # future_df = query(f_o='F', instrument='NIFTY', start_date=pd.to_datetime('2022-01-01'), end_date=pd.to_datetime('2022-01-05'), STime="09:15:00")
    future_df = pd.read_csv('/Users/abhijayjain/Desktop/internship/Indira Securities/F&O Prediction/testing stats/demo stats/future_df.csv')
    dates = list(future_df["Date"].unique())

    future_df['datetime'] = future_df['Date'] + " " + future_df["Time"]
    future_df['datetime'] = pd.to_datetime(future_df['datetime'])
    future_df = future_df.set_index(['datetime']).resample("5T").agg({"Ticker": "first", "ExpiryDT": "first", "FnO": "first", "Volume": "sum", "Open": "first", "High": "max", "Low": "min", "Close": "last", "Time": "first", "Date": "first"})
    future_df = future_df.dropna()
    final_future_df = []
    call_final_results_df = pd.DataFrame(columns=["Date", "Time", "Option", "SL % 2", "Target % 2", "Exit Time % 2", "Buy Price % 2", "Exit Price % 2", "Profit/Loss % 2", "SL % 5", "Target % 5", "Exit Time % 5", "Buy Price % 5", "Exit Price % 5", "Profit/Loss % 5", "SL % 7", "Target % 7", "Exit Time % 7", "Buy Price % 7", "Exit Price % 7", "Profit/Loss % 7", "SL % 10", "Target % 10", "Exit Time % 10", "Buy Price % 10", "Exit Price % 10", "Profit/Loss % 10"])  
    put_final_results_df = pd.DataFrame(columns=["Date", "Time", "Option", "SL % 2", "Target % 2", "Exit Time % 2", "Buy Price % 2", "Exit Price % 2", "Profit/Loss % 2", "SL % 5", "Target % 5", "Exit Time % 5", "Buy Price % 5", "Exit Price % 5", "Profit/Loss % 5", "SL % 7", "Target % 7", "Exit Time % 7", "Buy Price % 7", "Exit Price % 7", "Profit/Loss % 7", "SL % 10", "Target % 10", "Exit Time % 10", "Buy Price % 10", "Exit Price % 10", "Profit/Loss % 10"])

    for i in dates:
        one_day_future_df = future_df.loc[future_df["Date"] == i].copy()
        calculate_ema_and_entry_points_in_futures (one_day_future_df)
        rounded_average,resampled_data = calculate_ema_and_entry_points_in_futures (one_day_future_df) 
        
        # Expiry = get_expiry(i)
        data_type = type(rounded_average)   
       
        try:     
            # CEOptionDf = query(f_o='O', instrument='NIFTY', expiry_dt = Expiry, strike= rounded_average, option_type='CE', 
                            # start_date=pd.to_datetime(i), end_date=pd.to_datetime(i))
            CEOptionDf = pd.read_csv('/Users/abhijayjain/Desktop/internship/Indira Securities/F&O Prediction/testing stats/demo stats/ce_data.csv')

            # PEOptionDf = query(f_o='O', instrument='NIFTY', expiry_dt = Expiry, strike= rounded_average, option_type='PE', 
                        # start_date=pd.to_datetime(i), end_date=pd.to_datetime(i))
            PEOptionDf = pd.read_csv('/Users/abhijayjain/Desktop/internship/Indira Securities/F&O Prediction/testing stats/demo stats/pe_data.csv')
            
            call_5t_optiondf, call_1t_optiondf, put_5t_optiondf, put_1t_optiondf = calculating_ema_of_option(CEOptionDf, PEOptionDf) 
            extracted_long_call_data_df , extracted_short_put_data_df = getting_the_strike(one_day_future_df)
            merged_df = matching_the_strike(call_5t_optiondf, put_5t_optiondf, extracted_long_call_data_df , extracted_short_put_data_df)
            # stop_loss(merged_df)        
            trade_df = buy_sell_execution(call_1t_optiondf, put_1t_optiondf ,merged_df)
            
            # final_future_df = pd.concat([resampled_data])
            # call_final_results_df = pd.concat([call_final_results_df, call_results_df])
            # put_final_results_df = pd.concat([put_final_results_df, put_results_df])

            # final_future_df.to_csv('final_future.csv')
            # call_final_results_df.to_csv('call_final_results.csv')
            # put_final_results_df.to_csv('put_final_results.csv')



        except Exception as e:
            print(f"Error processing data for date {i}: {str(e)}")
            continue

#---------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    main()
