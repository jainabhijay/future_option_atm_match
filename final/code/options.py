import pandas as pd
#----------------------------loading option finlr------------------------------------------
# # Define a function to extract name, date, strike, and option type from the ticker
# def extract_ticker_info(ticker):
#     ticker = ticker.rstrip('.NFO')
#     name_length = 5
#     date_length = 7
#     strike_length = 5
#     option_type_length = 2

#     name = ticker[:name_length]
#     date = ticker[name_length:name_length + date_length]
#     strike = ticker[name_length + date_length:name_length + date_length + strike_length]
#     option_type = ticker[-option_type_length]

#     return name, date, strike, option_type

# # Define time intervals for resampling
# time_intervals = ['5T', '0.1T']  # Customize as needed

# # Create a directory to store the CSV files
# output_directory = '/Users/abhijayjain/Desktop/internship/Indira Securities/F&O Prediction/final/output/'

# for option_type in ['CE', 'PE']:
#     for time_interval in time_intervals:
#         # Load your call or put ticker data
#         ticker_data = pd.read_csv(f'/Users/abhijayjain/Desktop/internship/Indira Securities/F&O Prediction/final/Input/Nifty Future Input/03oct/NIFTY06OCT2217100{option_type}.NFO.csv')

#         # Extract information and add to DataFrame
#         ticker_data[['Name', 'Date', 'Strike', 'Option_Type']] = ticker_data['Ticker'].apply(extract_ticker_info).apply(pd.Series)

#         # Combine Date and Time columns into a single Datetime column
#         ticker_data['Datetime'] = pd.to_datetime(ticker_data['Date'] + ' ' + ticker_data['Time'])

#         # Add separate Date and Time columns
#         ticker_data['Date'] = ticker_data['Datetime'].dt.date
#         ticker_data['Time'] = ticker_data['Datetime'].dt.time

#         # Set Datetime as the index
#         ticker_data.set_index('Datetime', inplace=True)

#         # Resample the data at the specified time interval and calculate OHLC
#         ohlc_data = ticker_data['LTP'].resample(time_interval).ohlc()
#         ohlc_data['BuyPrice'] = ticker_data['BuyPrice'].resample(time_interval).first()
#         ohlc_data['SellPrice'] = ticker_data['SellPrice'].resample(time_interval).first()
#         ohlc_data['Strike'] = ticker_data['Strike'].resample(time_interval).first()

#         # Calculate the 5EMA for options data
#         ohlc_data['EMA5'] = ohlc_data['close'].ewm(span=5).mean()

#         # Add 'Date' and 'Time' columns to the OHLC data
#         ohlc_data['Date'] = ohlc_data.index.date
#         ohlc_data['Time'] = ohlc_data.index.time

#         # Define the output CSV file path
#         output_csv_file = f'{output_directory}outputNIFTY06OCT2217100_{option_type}_{time_interval}.csv'

#         # Save the OHLC data (including 'Date' and 'Time' columns) to a CSV file
#         ohlc_data.to_csv(output_csv_file)

#         print(f'OHLC data for {option_type} options at {time_interval} interval saved to {output_csv_file}')
# # #------------------------------rounded values---------------------------------------------
# # # Load your future data
# future_data = pd.read_csv('/Users/abhijayjain/Desktop/internship/Indira Securities/F&O Prediction/final/Input/Nifty Future Input/03oct/output data for nifty future.csv')

# # Convert the 'Datetime' column to datetime format
# future_data['Datetime'] = pd.to_datetime(future_data['Date'] + ' ' + future_data['Time'], format='%Y-%m-%d %H:%M:%S')

# # Filter rows where 'Long Call Entry Point' is not null
# filtered_long_call_data = future_data[future_data['Long Call Entry Point'].notnull()]

# # Create a DataFrame with the extracted values, long call entry points, and time slots
# extracted_long_call_data_df = pd.DataFrame({
#     'Rounded-off Values': filtered_long_call_data['round off values'],
#     'Long Call Entry Point': filtered_long_call_data['Long Call Entry Point'],
#     'Time Slot': filtered_long_call_data['Datetime'].dt.strftime('%H:%M:%S')
# })

# # Filter rows where 'Short Put Entry Point' is not null
# filtered_short_put_data = future_data[future_data['Short Put Entry Point'].notnull()]

# # Create a DataFrame with the extracted values, short put entry points, and time slots
# extracted_short_put_data_df = pd.DataFrame({
#     'Rounded-off Values': filtered_short_put_data['round off values'],
#     'Short Put Entry Point': filtered_short_put_data['Short Put Entry Point'],
#     'Time Slot': filtered_short_put_data['Datetime'].dt.strftime('%H:%M:%S')
# })

# # Save the Long Call Entry Point DataFrame to a CSV file
# extracted_long_call_data_df.to_csv('/Users/abhijayjain/Desktop/internship/Indira Securities/F&O Prediction/final/output/call rounded value.csv', index=False)

# # Save the Short Put Entry Point DataFrame to a CSV file
# extracted_short_put_data_df.to_csv('/Users/abhijayjain/Desktop/internship/Indira Securities/F&O Prediction/final/output/put rounded value.csv', index=False)
#----------------------------------Matching Strike --------------------------------------------
# # Load the CSV files for call and put options
# call_options_df = pd.read_csv('/Users/abhijayjain/Desktop/internship/Indira Securities/F&O Prediction/final/output/outputNIFTY06OCT2217100_CE_5T.csv')
# put_options_df = pd.read_csv('/Users/abhijayjain/Desktop/internship/Indira Securities/F&O Prediction/final/output/outputNIFTY06OCT2217100_PE_5T.csv')

# # Load the CSV files for rounded values for call and put options
# call_rounded_values_df = pd.read_csv('/Users/abhijayjain/Desktop/internship/Indira Securities/F&O Prediction/final/output/call rounded value.csv')
# put_rounded_values_df = pd.read_csv('/Users/abhijayjain/Desktop/internship/Indira Securities/F&O Prediction/final/output/put rounded value.csv')

# # Filter out rows with 'Time' not equal to '09:15:00' in the rounded values DataFrames
# call_rounded_values_df = call_rounded_values_df[call_rounded_values_df['Time Slot'] != '09:15:00']
# put_rounded_values_df = put_rounded_values_df[put_rounded_values_df['Time Slot'] != '09:15:00']

# # Merge the DataFrames based on 'Time' and 'Rounded-off Values' for call options
# call_merged_df = call_options_df.merge(call_rounded_values_df, left_on=['Time', 'Strike'], right_on=['Time Slot', 'Rounded-off Values'], how='inner')

# # Define a function to update 'Buy/Sell Call' and related columns based on 'Long_Call_Entry'
# def update_call(row):
#     if row['Long Call Entry Point']:
#         return f"Buy Call at {row['open']}"
#     else:
#         return f"Sell Call at {row['open']}"

# # Apply the update_call function to create 'Buy/Sell Call' column for call options
# call_merged_df['Buy/Sell Call'] = call_merged_df.apply(update_call, axis=1)

# # Merge the DataFrames based on 'Time' and 'Rounded-off Values' for put options
# put_merged_df = put_options_df.merge(put_rounded_values_df, left_on=['Time', 'Strike'], right_on=['Time Slot', 'Rounded-off Values'], how='inner')

# # Define a function to update 'Buy/Sell Put' and related columns based on 'Short_Put_Entry'
# def update_put(row):
#     if row['Short Put Entry Point']:
#         return f"Buy Put at {row['open']}"
#     else:
#         return f"Sell Put at {row['open']}"

# # Apply the update_put function to create 'Buy/Sell Put' column for put options
# put_merged_df['Buy/Sell Put'] = put_merged_df.apply(update_put, axis=1)

# # Rearrange the columns in both DataFrames
# column_order_call = [
#     'Date',
#     'Time',
#     'Buy/Sell Call',
#     'open',
#     'high',
#     'low',
#     'close',
# ]
# # Rearrange the columns in both DataFrames
# column_order_put = [
#     'Date',
#     'Time',
#     'Buy/Sell Put',
#     'open',
#     'high',
#     'low',
#     'close',
# ]

# call_merged_df = call_merged_df[column_order_call]
# put_merged_df = put_merged_df[column_order_put]

# # Save the matching data to separate CSV files for call and put options
# call_merged_df.to_csv('/Users/abhijayjain/Desktop/internship/Indira Securities/F&O Prediction/final/output/call_impdata_extraction_from_future.csv', index=False)
# put_merged_df.to_csv('/Users/abhijayjain/Desktop/internship/Indira Securities/F&O Prediction/final/output/put_impdata_extraction_from_future.csv', index=False)
#----------------------------------stop loss-------------------------------------------------------------------------------
# Calculate and add stop loss and target prices for call options at specified percentages
stop_loss_percentages = [2, 5, 7, 10]
call_merged_df ['Stop Loss'] = 0.0  # Create a new column for stop loss
call_merged_df ['Target'] = 0.0  # Create a new column for target

put_merged_df ['Stop Loss'] = 0.0  # Create a new column for stop loss
put_merged_df ['Target'] = 0.0  # Create a new column for target

for percentage in stop_loss_percentages:
    call_merged_df[f"Stop Loss {percentage}%"] = call_merged_df['open'] * (1 - percentage / 100)
    call_merged_df[f"Target {percentage}%"] = call_merged_df['open'] * (1 + percentage / 100)

# Calculate and add stop loss and target prices for put options at specified percentages
for percentage in stop_loss_percentages:
    put_merged_df[f"Stop Loss {percentage}%"] = put_merged_df['open'] * (1 - percentage / 100)
    put_merged_df[f"Target {percentage}%"] = put_merged_df['open'] * (1 + percentage / 100)

# Print the DataFrames with stop loss and target columns for both call and put options
print("Call Options Data:")
print(call_merged_df[['Date', 'Time', 'open', 'Buy/Sell Call', 'Stop Loss', 'Target']])
print("\nPut Options Data:")
print(put_merged_df[['Date', 'Time', 'open', 'Buy/Sell Put', 'Stop Loss', 'Target']])

# Save the matching call data with stop loss and target prices to a CSV file
call_merged_df.to_csv('/Users/abhijayjain/Desktop/internship/Indira Securities/F&O Prediction/final/output/call_impdata_extraction_from_future_with_sl_target.csv', index=False)

# Save the matching put data with stop loss and target prices to a CSV file
put_merged_df.to_csv('/Users/abhijayjain/Desktop/internship/Indira Securities/F&O Prediction/final/output/put_impdata_extraction_from_future_with_sl_target.csv', index=False)
# #---------------------------------buy and sell execution----------------------------------------------------------------------------------------------

# Load your second-wise data for call and put options
call_second_data = pd.read_csv('/Users/abhijayjain/Desktop/internship/Indira Securities/F&O Prediction/final/output/outputNIFTY06OCT2217100_CE_0.1T.csv')
put_second_data = pd.read_csv('/Users/abhijayjain/Desktop/internship/Indira Securities/F&O Prediction/final/output/outputNIFTY06OCT2217100_PE_0.1T.csv')

# Convert 'Time' column to a datetime format for call and put second data
call_second_data['Time'] = pd.to_datetime(call_second_data['Time'], format='%H:%M:%S')
put_second_data['Time'] = pd.to_datetime(put_second_data['Time'], format='%H:%M:%S')

# Define the time window for 5 minutes
time_window = pd.Timedelta(minutes=5)

# Create a dictionary to map option types to their respective data and results lists
option_data_map = {
    'Call': {
        'data': pd.read_csv('/Users/abhijayjain/Desktop/internship/Indira Securities/F&O Prediction/final/output/call_impdata_extraction_from_future_with_sl_target.csv'),
        'second_data': call_second_data,
        'results': []
    },
    'Put': {
        'data': pd.read_csv('/Users/abhijayjain/Desktop/internship/Indira Securities/F&O Prediction/final/output/put_impdata_extraction_from_future_with_sl_target.csv'),
        'second_data': put_second_data,
        'results': []
    }
}

# Iterate through each option type (Call and Put)
for option_type, option_info in option_data_map.items():
    option_data = option_info['data']
    option_second_data = option_info['second_data']
    option_results = option_info['results']

    # Iterate through each row in the option_data
    for index, row in option_data.iterrows():
        time = pd.to_datetime(row['Time'], format='%H:%M:%S').replace(second=0)
        
        # Extract the date from the option_data row
        date = row['Date']
        
        # Find the corresponding second in the option_second_data
        matching_time = option_second_data['Time'].apply(lambda x: x.replace(second=0)).eq(time).idxmax()
        
        # Extract the 5-second time frame
        start_time = option_second_data.at[matching_time, 'Time']
        end_time = start_time + time_window

        # Filter the data within the 5-second time frame
        filtered_data = option_second_data[
            (option_second_data['Time'] >= start_time) & (option_second_data['Time'] <= end_time)
        ]

        # Initialize dictionaries to store results for each percentage
        result_dict = {'Date': date, 'Time': time, 'Option': option_type}
        
        # Check execution conditions for each percentage
        for percentage in [2, 5, 7, 10]:
            sl_price = row[f"Stop Loss {percentage}%"]
            target_price = row[f"Target {percentage}%"]
            
            # Check if any price condition was met within the time frame
            sl_executed = (filtered_data['low'] <= sl_price).any()
            target_executed = (filtered_data['high'] >= target_price).any()
            
            exit_condition = None
            exit_time = None
            buy_price = None
            exit_price = None

            if sl_executed and target_executed:
                # Both SL and Target were executed
                sl_time = filtered_data[filtered_data['low'] <= sl_price]['Time'].min()
                target_time = filtered_data[filtered_data['high'] >= target_price]['Time'].min()
                
                if sl_time < target_time:
                    exit_condition = 'SL Hit First'
                    exit_time = sl_time
                    buy_price = row['open']
                    exit_price = sl_price
                else:
                    exit_condition = 'Target Hit First'
                    exit_time = target_time
                    buy_price = row['open']
                    exit_price = target_price
            elif sl_executed:
                exit_condition = 'SL Hit First'
                exit_time = filtered_data[filtered_data['low'] <= sl_price]['Time'].min()
                buy_price = row['open']
                exit_price = sl_price
            elif target_executed:
                exit_condition = 'Target Hit First'
                exit_time = filtered_data[filtered_data['high'] >= target_price]['Time'].min()
                buy_price = row['open']
                exit_price = target_price
            else:
                exit_condition = 'Close Price'
                exit_time = filtered_data['Time'].max()  # Last time frame within the 5-second window
                buy_price = row['open']
                exit_price = filtered_data[filtered_data['Time'] == exit_time]['close'].values[0]
            
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
                profit_loss = (exit_price - buy_price) * 50  # Close Price
            
            result_dict[f'Profit/Loss % {percentage}'] = profit_loss
        
        option_results.append(result_dict)

# Create DataFrames for the results for call and put options
call_results_df = pd.DataFrame(option_data_map['Call']['results'])
put_results_df = pd.DataFrame(option_data_map['Put']['results'])

# Save the results to separate CSV files for call and put options
call_results_df.to_csv('/Users/abhijayjain/Desktop/internship/Indira Securities/F&O Prediction/final/output/call/call_results.csv', index=False)
put_results_df.to_csv('/Users/abhijayjain/Desktop/internship/Indira Securities/F&O Prediction/final/output/put/put_results.csv', index=False)
#