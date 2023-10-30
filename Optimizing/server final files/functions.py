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
    
    print("this is the resampled data for test: \n",resampled_data)

    average_rounded_value = (resampled_data['Rounded Value'].astype(int)).mean()
    rounded_average = int(round(average_rounded_value / 100) * 100)

    print("Strike for options is: \n", rounded_average)
    resampled_data.reset_index(inplace=True)
    resampled_data = resampled_data[['Ticker', 'datetime', 'FnO', 'Volume', 'Open', 'High', 'Low', 'Close', 'ema5', 'Long_Call_Entry', 'Long Call Entry Point', 'Short_Put_Entry', 'Short Put Entry Point', 'Call Or Put Status', 'Rounded Value']]
    print("this is the resampled data: \n",resampled_data)
    
    return rounded_average

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

    print("5t call option data frame:\n",call_5t_optiondf)
    print("1t call option data frame:\n",call_1t_optiondf)
    print("5t put option data frame:\n",put_5t_optiondf)
    print("1t put option data frame:\n",put_1t_optiondf)
    
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

    print("Long Call Data Extracted in Another DF:\n",extracted_long_call_data_df)
    print("short put Data Extracted in Another DF:\n",extracted_short_put_data_df)
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
            return f"Buy Call at {row['Open']}"
        else:
            return f"Sell Call at {row['Open']}"

    call_merged_df['Buy/Sell Call'] = call_merged_df.apply(update_call, axis=1)
    
    put_merged_df = put_5t_optiondf.merge(extracted_short_put_data_df, left_on=['Time', 'Strike'], right_on=['Time Slot', 'Rounded Value'], how='inner')

    def update_put(row):
        if row['Short Put Entry Point']:
            return f"Buy Put at {row['Open']}"
        else:
            return f"Sell Put at {row['Open']}"

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
    print("call shotlisted time frame of trade:\n",call_merged_df)
    print("put shotlisted time frame of trade:\n",put_merged_df)
    return call_merged_df, put_merged_df

#---------------------------------------------------------------------------------------------------
def stop_loss(call_merged_df , put_merged_df):
    stop_loss_percentages = [2, 5, 7, 10]
    call_merged_df ['Stop Loss'] = 0.0  
    call_merged_df ['Target'] = 0.0  

    put_merged_df ['Stop Loss'] = 0.0
    put_merged_df ['Target'] = 0.0 

    for percentage in stop_loss_percentages:
        call_merged_df[f"Stop Loss {percentage}%"] = call_merged_df['Open'] * (1 - percentage / 100)
        call_merged_df[f"Target {percentage}%"] = call_merged_df['Open'] * (1 + percentage / 100)
        put_merged_df[f"Stop Loss {percentage}%"] = put_merged_df['Open'] * (1 - percentage / 100)
        put_merged_df[f"Target {percentage}%"] = put_merged_df['Open'] * (1 + percentage / 100)

    print("call target and stop loss data:\n",call_merged_df)
    print("Put target and stop loss data:\n",put_merged_df)

    return call_merged_df , put_merged_df

#---------------------------------------------------------------------------------------------------
def buy_sell_execution(call_1t_optiondf, put_1t_optiondf ,call_merged_df,put_merged_df):

    call_1t_optiondf['Time'] = pd.to_datetime(call_1t_optiondf['Time'], format='%H:%M:%S')
    put_1t_optiondf['Time'] = pd.to_datetime(put_1t_optiondf['Time'], format='%H:%M:%S')

    time_window = pd.Timedelta(minutes=5)

    option_data_map = {
        'Call': {
            'data': call_merged_df,
            'second_data': call_1t_optiondf,
            'results': []
        },
        'Put': {
            'data': put_merged_df,
            'second_data': put_1t_optiondf,
            'results': []
        }
    }

    for option_type, option_info in option_data_map.items():
        option_data = option_info['data']
        option_second_data = option_info['second_data']
        option_results = option_info['results']

        for index, row in option_data.iterrows():
            time = pd.to_datetime(row['Time'], format='%H:%M:%S').replace(second=0)
            
            date = row['Date']
            
            matching_time = option_second_data['Time'].apply(lambda x: x.replace(second=0)).eq(time).idxmax()
            
            # Extract the 5-second time frame
            start_time = option_second_data.at[matching_time, 'Time']
            end_time = start_time + time_window

            filtered_data = option_second_data[
                (option_second_data['Time'] >= start_time) & (option_second_data['Time'] <= end_time)
            ]

            result_dict = {'Date': date, 'Time': time, 'Option': option_type}
            
            for percentage in [2, 5, 7, 10]:
                sl_price = row[f"Stop Loss {percentage}%"]
                target_price = row[f"Target {percentage}%"]
                
                sl_executed = (filtered_data['Low'] <= sl_price).any()
                target_executed = (filtered_data['High'] >= target_price).any()
                
                exit_condition = None
                exit_time = None
                buy_price = None
                exit_price = None

                if sl_executed and target_executed:
                    sl_time = filtered_data[filtered_data['Low'] <= sl_price]['Time'].min()
                    target_time = filtered_data[filtered_data['High'] >= target_price]['Time'].min()
                    
                    if sl_time < target_time:
                        exit_condition = 'SL Hit First'
                        exit_time = sl_time
                        buy_price = row['Open']
                        exit_price = sl_price
                    else:
                        exit_condition = 'Target Hit First'
                        exit_time = target_time
                        buy_price = row['Open']
                        exit_price = target_price
                elif sl_executed:
                    exit_condition = 'SL Hit First'
                    exit_time = filtered_data[filtered_data['Low'] <= sl_price]['Time'].min()
                    buy_price = row['Open']
                    exit_price = sl_price
                elif target_executed:
                    exit_condition = 'Target Hit First'
                    exit_time = filtered_data[filtered_data['High'] >= target_price]['Time'].min()
                    buy_price = row['Open']
                    exit_price = target_price
                else:
                    exit_condition = 'Close Price'
                    exit_time = filtered_data['Time'].max()  
                    buy_price = row['Open']
                    exit_price = filtered_data[filtered_data['Time'] == exit_time]['Close'].values[0]
                
                result_dict[f'SL % {percentage}'] = exit_condition
                result_dict[f'Target % {percentage}'] = exit_condition
                result_dict[f'Exit Time % {percentage}'] = exit_time
                result_dict[f'Buy Price % {percentage}'] = buy_price
                result_dict[f'Exit Price % {percentage}'] = exit_price

                # Calculate Profit/Loss
                if exit_condition == 'SL Hit First':
                    profit_loss = (exit_price - buy_price) * 50
                elif exit_condition == 'Target Hit First':
                    profit_loss = (exit_price - buy_price) * 50
                else:
                    profit_loss = (exit_price - buy_price) * 50  
                
                result_dict[f'Profit/Loss % {percentage}'] = profit_loss
            
            option_results.append(result_dict)

    call_results_df = pd.DataFrame(option_data_map['Call']['results'])
    put_results_df = pd.DataFrame(option_data_map['Put']['results'])

    print("results for call:\n",call_results_df)
    print("results for put:\n",put_results_df)
    return call_results_df , put_results_df

#---------------------------------------------------------------------------------------------------
def main():
    future_df = query(f_o='F', instrument='NIFTY', start_date=pd.to_datetime('2022-01-01'), end_date=pd.to_datetime('2022-01-05'), STime="09:15:00")
    # future_df = pd.read_csv('/Users/abhijayjain/Desktop/internship/Indira Securities/F&O Prediction/future_df.csv')
    dates = list(future_df["Date"].unique())

    future_df['datetime'] = future_df['Date'] + " " + future_df["Time"]
    future_df['datetime'] = pd.to_datetime(future_df['datetime'])
    future_df = future_df.set_index(['datetime']).resample("5T").agg({"Ticker": "first", "ExpiryDT": "first", "FnO": "first", "Volume": "sum", "Open": "first", "High": "max", "Low": "min", "Close": "last", "Time": "first", "Date": "first"})
    future_df = future_df.dropna()

    for i in dates:
        one_day_future_df = future_df.loc[future_df["Date"] == i].copy()
        calculate_ema_and_entry_points_in_futures (one_day_future_df)
        rounded_average = calculate_ema_and_entry_points_in_futures (one_day_future_df)
        print("rounded average :\n",rounded_average)
        
        Expiry = get_expiry(i)
        data_type = type(rounded_average)
        print("rounded average data type: \n",data_type)


        CEOptionDf = query(f_o='O', instrument='NIFTY', expiry_dt = Expiry, strike= rounded_average, option_type='CE', 
                        start_date=pd.to_datetime(i), end_date=pd.to_datetime(i))
        # CEOptionDf = pd.read_csv('/Users/abhijayjain/Desktop/internship/Indira Securities/F&O Prediction/ce_data.csv')
        print("this is the call option DF",CEOptionDf)   

        PEOptionDf = query(f_o='O', instrument='NIFTY', expiry_dt = Expiry, strike= rounded_average, option_type='PE', 
                    start_date=pd.to_datetime(i), end_date=pd.to_datetime(i))
        # PEOptionDf = pd.read_csv('/Users/abhijayjain/Desktop/internship/Indira Securities/F&O Prediction/pe_data.csv')
        print("this is the call option DF",PEOptionDf)     
        
        call_5t_optiondf, call_1t_optiondf, put_5t_optiondf, put_1t_optiondf = calculating_ema_of_option(CEOptionDf, PEOptionDf) 
        extracted_long_call_data_df , extracted_short_put_data_df = getting_the_strike(one_day_future_df)
        call_merged_df,put_merged_df = matching_the_strike(call_5t_optiondf, put_5t_optiondf, extracted_long_call_data_df , extracted_short_put_data_df)
        stop_loss(call_merged_df, put_merged_df)        
        buy_sell_execution(call_1t_optiondf, put_1t_optiondf ,call_merged_df,put_merged_df)

#---------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    main()
