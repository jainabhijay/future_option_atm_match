import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from scipy.stats import skew, kurtosis
from docx import Document
import os

st.set_page_config(layout="wide")

# Get a list of directories in the current directory
folder_list = [item for item in os.listdir() if os.path.isdir(item)]

# Use st.sidebar.selectbox to allow the user to select a folder
folder_path = st.sidebar.selectbox("WELCOME ! Please select your Strategy", folder_list)

st.markdown('<h2 style="text-align:center; color:turquoise; font-weight:bold; text-transform:uppercase;">Strategy backtest data Ananlys</h2>', unsafe_allow_html=True)

# Check if a folder path is provided
if folder_path:
    folder_path = os.path.abspath(folder_path)  # Get the absolute path of the selected folder
    # Look for any .docx and .csv files in the selected folder
    docx_file = None
    csv_file = None
    for filename in os.listdir(folder_path):
        if filename.endswith(".docx"):
            docx_file = os.path.join(folder_path, filename)
        elif filename.endswith(".csv"):
            csv_file = os.path.join(folder_path, filename)

    # Check if the Word document exists at the provided path
    if docx_file and os.path.exists(docx_file):
        doc = Document(docx_file)
        content = ""
        for paragraph in doc.paragraphs:
            content += paragraph.text + "\n"
        st.markdown('<h2 style="text-align:center; color:yellow; font-weight:bold; text-transform:uppercase;">Strategy Script</h2>', unsafe_allow_html=True)    
        st.text_area("STRATEGY DETAILS", content, height=(len(content.split('\n')) * 25), key='textarea')
    else:
        st.warning("Word document not found in the selected folder")

    # Check if the CSV file exists at the provided path
    if csv_file and os.path.exists(csv_file):
        df = pd.read_csv(csv_file)
  
        st.markdown('<h2 style="text-align:center; color:yellow; font-weight:bold; text-transform:uppercase;"> Trading Data</h2>', unsafe_allow_html=True)

        min_pl = df['p/l'].min()
        max_pl = df['p/l'].max()

        # Define the custom style function
        def custom_style(x):
            background_color = 'red' if x == min_pl else ('turquoise' if x == max_pl else '')
            color = 'black' if x == min_pl or  x == max_pl else 'orange' if x < 0 else 'turquoise'
            return f'color: {color}; background-color: {background_color}'

        # Apply the custom style to the entire DataFrame
        styled_df = df.style.applymap(custom_style, subset=['p/l'])
       
        

        # Calculate the total profit/loss
        total_profit_loss = df['p/l'].sum()

        # Calculate the total number of positive and negative trades
        positive_trades = df[df['p/l'] > 0]
        negative_trades = df[df['p/l'] < 0]

        # Calculate winning and losing percentages
        total_trades = len(df)

        # Page 2: Cumulative Line Graph
        st.markdown('<h2 style="text-align:center; color:yellow; font-weight:bold; text-transform:uppercase;">Cumulative Line Graph</h2>', unsafe_allow_html=True)

        # Create a figure with two traces: Cumulative P/L line and P/L bars
        fig_cumulative = go.Figure()

        df['Cumulative P/L'] = df['p/l'].cumsum()
        # Add Cumulative P/L line trace
        fig_cumulative.add_trace(go.Scatter(x=df['Date'], y=df['Cumulative P/L'], mode='lines', name='Cumulative P/L'))

        # Add P/L bars trace with custom colors
        bar_colors = ['turquoise' if val > 0 else 'orange' for val in df['p/l']]
        bar_colors[df['p/l'].idxmax()] = 'blue'  # Set maximum P/L bar to blue
        bar_colors[df['p/l'].idxmin()] = 'red'   # Set minimum P/L bar to red

        fig_cumulative.add_trace(go.Bar(x=df['Date'], y=df['p/l'], marker_color=bar_colors, name='P/L'))

        # Customize layout
        fig_cumulative.update_layout(
            xaxis_title='Date',
            yaxis_title='Profit/Loss',
            xaxis=dict(showline=True, showgrid=False),
            yaxis=dict(showline=True, showgrid=False),
            legend=dict(x=0, y=1, traceorder='normal'),
            barmode='relative',
        )

        # Display the chart
        st.plotly_chart(fig_cumulative, use_container_width=True)

        # Page 3: Summary Table


        # Calculate Average P/L
        average_pl = total_profit_loss / total_trades


        # Calculate Win Rate
        win_rate = (len(positive_trades) / total_trades) * 100


        # Calculate Average Winning Trade
        average_winning_trade = positive_trades['p/l'].mean()


        # Calculate Average Losing Trade
        average_losing_trade = negative_trades['p/l'].mean()


        # Calculate Largest Winning Trade
        largest_winning_trade = positive_trades['p/l'].max()


        # Calculate Largest Losing Trade
        largest_losing_trade = negative_trades['p/l'].min()


        # Calculate Profit Factor
        total_profits = positive_trades['p/l'].sum()
        total_losses = negative_trades['p/l'].sum()
        profit_factor = total_profits / abs(total_losses) if total_losses != 0 else None


        # Calculate Risk-Reward Ratio
        risk_reward_ratio = average_winning_trade / abs(average_losing_trade) if average_losing_trade != 0 else None


        # Calculate Maximum Drawdown
        cumulative_pl = df['Cumulative P/L']
        max_drawdown = (cumulative_pl - cumulative_pl.expanding().max()).min()

        drawdown_percentage = ((max_drawdown *100)/ cumulative_pl.max() ) 


        # Create a summary table
        summary_table = pd.DataFrame({
            'Metric': ['Total Profit/Loss', 'Average Profit/Loss per Trade', 'Total Number of Trades',
                    'Number of Winning Trades', 'Number of Losing Trades', 'Win Rate',
                    'Average Winning Trade', 'Average Losing Trade', 'Largest Winning Trade',
                    'Largest Losing Trade', 'Profit Factor', 'Risk-Reward Ratio', 'Maximum Drawdown', 'drawdown_percentage'],
            'Value': [total_profit_loss, average_pl, total_trades, len(positive_trades), len(negative_trades),
                    (len(positive_trades) / total_trades) * 100, average_winning_trade, average_losing_trade,
                    largest_winning_trade, largest_losing_trade, profit_factor, risk_reward_ratio, max_drawdown, drawdown_percentage]
        })
        

        def style_cell(val):
            # Modify this function to dynamically set background color based on a gradient
            colors = ['red', 'skyblue']
            
            # Calculate a color index based on the cell value
            color_index = int((val + 1) / 2 * len(colors))
            color_index = max(0, min(color_index, len(colors) - 1))  # Ensure it's within bounds

            background_color = colors[color_index]

            # Define the style
            style = f"font-size: 30px; {'color: black;' if val >= 0 else 'color: black;'}"
            style += f"background-color: {background_color};"

            return style

        # Define a function to style the 'Metric' column
        def style_metric_cell(val):
            # Modify this function to apply a different style to 'Metric' cells
            # For example, you can change the font size and text color
            style = "font-size: 30px; color: white;"

            return style
        
        # Apply styling to the 'Value' column
        styled_table = summary_table.style.applymap(style_cell, subset=['Value'])

        # Apply styling to the 'Metric' column
        styled_table = styled_table.applymap(style_metric_cell, subset=['Metric'])

        st.markdown('<h2 style="text-align:center; color:yellow; font-weight:bold; text-transform:uppercase;">Trading P/L statical analysis</h2>', unsafe_allow_html=True)


        st.table(styled_table)

        max_trades_in_a_day = df.groupby('Date')['Option'].count().max()
        min_trades_in_a_day = df.groupby('Date')['Option'].count().min()

        # Find the type of trades (buy call, buy put, sell call, or sell put) on the day with the maximum trades
        day_with_max_trades = df.groupby('Date')['Option'].count().idxmax()
        type_of_trades_on_max_day = df[df['Date'] == day_with_max_trades]['Option'].values[0]

        # Find the type of trades (buy call, buy put, sell call, or sell put) on the day with the minimum trades
        day_with_min_trades = df.groupby('Date')['Option'].count().idxmin()
        type_of_trades_on_min_day = df[df['Date'] == day_with_min_trades]['Option'].values[0]

        total_buy_call_trades = len(df[df['Option'] == 'Buy Call'])
        total_buy_put_trades = len(df[df['Option'] == 'Buy Put'])

        # Print the results
        st.write(f"Total number of 'buy call' trades: {total_buy_call_trades}")
        st.write(f"Total number of 'buy put' trades: {total_buy_put_trades}")
        # Print the results
        st.write(f"Maximum trades in a day: {max_trades_in_a_day}, on {day_with_max_trades} ")
        st.write(f"Minimum trades in a day: {min_trades_in_a_day}, on {day_with_min_trades} ")


        # Display the DataFrame using st.table()
        st.table(styled_df)

    else:
        st.warning("CSV file not found in the selected folder")