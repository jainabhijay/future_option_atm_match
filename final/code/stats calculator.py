import pandas as pd

# Read the CSV file into a DataFrame
df = pd.read_csv('/Users/abhijayjain/Desktop/internship/Indira Securities/F&O Prediction/call_stats/output_10%.csv')

# Create a dictionary to store data for each Profit/Loss percentage
sheets = {}
# Loop through the columns for each Profit/Loss percentage
for col_name in ["p/l"]:
    # Create a DataFrame for each percentage
    sheet_df = df[[col_name]].copy()
    # Calculate metrics
    sheet_df['Cumulative Profit/Loss'] = sheet_df[col_name].cumsum()
    sheet_df['Continuous Days of Profit'] = sheet_df[col_name].gt(0).groupby(sheet_df[col_name].lt(0).cumsum()).cumsum()
    sheet_df['Continuous Days of Loss'] = sheet_df[col_name].lt(0).groupby(sheet_df[col_name].gt(0).cumsum()).cumsum()
    sheet_df['Maximum Profit in a Single Day'] = sheet_df[col_name].max()
    sheet_df['Maximum Loss in a Single Day'] = sheet_df[col_name].min()
    sheet_df['Total Number of Trades'] = sheet_df[col_name].count()
    sheet_df['Total Number of Profit Trades'] = sheet_df[col_name].gt(0).sum()
    sheet_df['Total Number of Losing Trades'] = sheet_df[col_name].lt(0).sum()
    sheet_df['Win Percentage'] = (sheet_df['Total Number of Profit Trades'] / sheet_df['Total Number of Trades']) * 100
    sheet_df['Loss Percentage'] = (sheet_df['Total Number of Losing Trades'] / sheet_df['Total Number of Trades']) * 100
    sheet_df['Cost Per Trade'] = 20
    sheet_df['Profit/Loss Per Trade'] = sheet_df[col_name] - sheet_df['Cost Per Trade']  # Individual trade profit/loss
    
    # Calculate maximum winning and losing streaks
    sheet_df['Maximum Winning Streak'] = sheet_df['Continuous Days of Profit'].max()
    sheet_df['Maximum Losing Streak'] = sheet_df['Continuous Days of Loss'].max()
    
    # Store the result in the dictionary
    sheets[col_name] = sheet_df

# Concatenate all DataFrames into one
final_stats_df = pd.concat(sheets.values())

# Write the final DataFrame to a CSV file
final_stats_df.to_csv('/Users/abhijayjain/Desktop/internship/Indira Securities/F&O Prediction/call_stats/stats/01call_output_10%.csv')
