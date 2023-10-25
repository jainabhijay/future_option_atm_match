import pandas as pd

# Load the original CSV file
df = pd.read_csv('/Users/abhijayjain/Desktop/internship/Indira Securities/F&O Prediction/testing stats/backtesting /call /call_final_results.csv')

# Create a list of percentages to process (2%, 5%, 7%, 10%)
percentages = [2, 5, 7, 10]

# Loop through each percentage and create a separate CSV for each
for percentage in percentages:
    # Create a new DataFrame for the specific percentage
    new_df = pd.DataFrame()

    # Populate the new DataFrame with the required columns
    new_df['Symbol'] = df['Symbol']
    new_df['Option'] = df['Option']
    new_df['Date'] = df['Date']
    # new_df['ExpiryDT'] = df['Date']
    new_df['EnTime'] = df['Time']
    new_df['BPrice'] = df['Buy Price % {}'.format(percentage)]
    new_df['ExTime'] = df['Exit Time % {}'.format(percentage)]
    new_df['SPrice'] = df['Exit Price % {}'.format(percentage)]
    new_df['StopLoss'] = new_df['BPrice'] - (percentage / 100) * new_df['BPrice']
    new_df['Target'] = new_df['BPrice'] + (percentage / 100) * new_df['BPrice']
    new_df['p/l'] = df['Profit/Loss % {}'.format(percentage)]

    # Save the new DataFrame to a separate CSV file
    new_df.to_csv('call_output_{}%.csv'.format(percentage), index=False)
