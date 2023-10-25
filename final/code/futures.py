import pandas as pd
import matplotlib.dates as mdates
import mplfinance as mpf
# #------------------------------------future calculations------------------------------------------------------------------
#Editing Of Future Data Starts Here
# Future File Ka Location 
data = pd.read_csv('/Users/abhijayjain/Desktop/internship/Indira Securities/F&O Prediction/future.csv')
 
# seperating the date and time
data['Date'] = pd.to_datetime(data['Date'])
data['Time'] = pd.to_datetime(data['Time']).dt.time

# now combining them to create a single column Datetime
data['Datetime'] = data['Date'] + pd.to_timedelta(data['Time'].astype(str))

# setting Datetime as first column
data.set_index('Datetime', inplace=True)

# Getting the time interval
time_interval = '5T'

# #------------------------------------------------------------------------------------------------------

# Reordering of the file
resampled_data = data['LTP'].resample(time_interval).ohlc()
resampled_data['BuyPrice'] = data['BuyPrice'].resample(time_interval).first()
resampled_data['SellPrice'] = data['SellPrice'].resample(time_interval).first()

# 5EMA Calculation
resampled_data['ema5'] = resampled_data['close'].ewm(span=5).mean()

# # printing the csv sheet in the Python terminal
# print(resampled_data)


# #------------------------------------------------------------------------------------------------------
# #chart creation

# # formating the candle stick chart
# plot_config = {
#     'type': 'candle',  # Type of chart
#     'style': 'yahoo',  # Chart style
#     'figratio': (12, 6),  # Figure size
#     'ylabel': 'Price',  # Y-axis label
#     'title': '5-Minute Candlestick Chart for future data',  # Chart title
# }
# # Create the candlestick chart using mplfinance
# mpf.plot(resampled_data, **plot_config)
# # Plot the ema5 line
# plt.plot(resampled_data.index, resampled_data['ema5'], label='ema5', color='blue')

# # Formatting the chart
# plt.xticks(rotation=45)
# plt.xlabel('Time')
# plt.ylabel('Price')
# plt.title('5-Minute 5EMA chart for future data')
# plt.legend()
# plt.tight_layout()
# plt.show()

# #------------------------------------------------------------------------------------------------------
# Check For Call Entry Position
resampled_data['Long_Call_Entry'] = resampled_data['close'] > resampled_data['ema5']

# Creating a new column for  call entry points
resampled_data['Long Call Entry Point'] = None  

# saving the call position in the memory
resampled_data.loc[resampled_data['Long_Call_Entry'], 'Long Call Entry Point'] = "Enter the Long Call trade at - " + resampled_data['close'].astype(str)

# # Printing the CSV sheet in the Python terminal
# print(resampled_data)

# # Printing CSV file
# resampled_data.to_csv('output data with long call.csv')

# #------------------------------------------------------------------------------------------------------

# Checking for put condition
resampled_data['short_put_entry'] = resampled_data['close'] < resampled_data['ema5']

# Creating a new column for put entry points 
resampled_data['Short Put Entry Point'] = None  

# saving the put position in the memory 
resampled_data.loc[resampled_data['short_put_entry'], 'Short Put Entry Point'] = "Enter the short put trade at - " + resampled_data['close'].astype(str)

# # Printing the CSV sheet in the Python terminal
# print(resampled_data)

# # Printing CSV file
# resampled_data.to_csv('output data with short put.csv')

# #------------------------------------------------------------------------------------------------------

# Create a column to track Call entry
resampled_data['Long_Call_Entry'] = resampled_data['close'] > resampled_data['ema5']

# Create a column to track Put entry
resampled_data['Short_Put_Entry'] = resampled_data['close'] < resampled_data['ema5']

# Creating a column 
resampled_data['Call Or Put Status'] = None

# Using the condition to sell or buy respectively
resampled_data.loc[resampled_data['Long_Call_Entry'], 'Call Or Put Status'] = "Buy Call and Sell Put at "
resampled_data.loc[resampled_data['Short_Put_Entry'], 'Call Or Put Status'] = "Buy Put and Sell Call at "

# #------------------------------------------------------------------------------------------------------
# Check if  Call entry condition is true
long_call_condition = resampled_data['Long_Call_Entry']

# Rounded off values condition apply
resampled_data.loc[long_call_condition, 'round off values'] = "Buy Call and Sell Put at rounded off value (Price: " + (round(resampled_data['close'] / 100) * 100).astype(str) + ")"

# #------------------------------------------------------------------------------------------------------

# Rounded off values condition apply
resampled_data['round off values'] = (round(resampled_data['close'] / 100) * 100).astype(str)

# # Print the updated DataFrame with rounded-off values
# print(resampled_data)

# Separate the 'Datetime' column just before printing
resampled_data['Datetime'] = resampled_data.index
resampled_data['Date'] = resampled_data['Datetime'].dt.date
resampled_data['Time'] = resampled_data['Datetime'].dt.time

# Delete the previous 'Datetime' column
resampled_data.drop(columns=['Datetime'], inplace=True)

# Rearrange columns to put 'Date' and 'Time' first
resampled_data = resampled_data[['Date', 'Time', 'open', 'high', 'low', 'close', 'BuyPrice', 'SellPrice', 'ema5', 'Long_Call_Entry', 'Long Call Entry Point', 'short_put_entry', 'Short Put Entry Point', 'Call Or Put Status', 'round off values' ]]

# #------------------------------------------------------------------------------------------------------
output_path = ('/Users/abhijayjain/Desktop/internship/Indira Securities/F&O Prediction/final/Input/Nifty Future Input/03oct/output data for nifty future.csv')

# Print the DataFrame
print(resampled_data)

# Save the DataFrame to the specified path
resampled_data.to_csv(output_path, index=False)
resampled_data.to_csv('output data for nifty future.csv')