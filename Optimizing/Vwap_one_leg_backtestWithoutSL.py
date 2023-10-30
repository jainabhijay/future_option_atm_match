import pandas as pd
import subprocess

headers=["DateTime", "Ticker", "ExpiryDT", "Strike", "F&O", "Option", "Volume", "Open", "High", "Low", "Close", "OpenInterest"]
df =pd.read_csv("FINNIFTY-I.NFO_2020-01-03.csv", header=None).reset_index(drop=True)
df.columns=headers
print(df.head(10))
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

#-------------------------------------------------------------------------------------------------

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
#-------------------------------------------------------------------------------------------------
def createPos(Date, Close, time, option):

    global ExeDF,i

    print("InsidecreatePos")
    strike=round(Close/100)*100

    Expiry = get_expiry(Date)
    
    CEOptionDf = query(f_o='O', instrument='FINNIFTY', expiry_dt = Expiry, strike=strike, option_type=option, 
                       start_date=pd.to_datetime(Date), end_date=pd.to_datetime(Date))
    
    # print("DF in createPosition =",(CEOptionDf.loc[(CEOptionDf["Time"]==time)]))

    if not CEOptionDf.empty:

        df = (CEOptionDf.loc[(CEOptionDf["Time"]>=time) & (CEOptionDf["Time"]<(pd.to_datetime(time)+pd.Timedelta(3, unit='m')).strftime("%H:%M:%S"))])

        if not df.empty:

            # SPrice = ((CEOptionDf.loc[(CEOptionDf["Time"]==time)]).reset_index(drop=True)).loc[0, "Close"]

            # print(df.tail())

            SPrice = (df.reset_index(drop=True)).loc[len(df)-1, "Close"]

            dic = {"Symbol":"FINNIFTY","Pos":"Open", "Date":Date, "Strike":strike, "ExpiryDT":Expiry, "Option":option, "EnTime":time, "SPrice":SPrice, "ExTime":"","BPrice":""}
            
            ExeDF = pd.concat([ExeDF, pd.DataFrame(dic, index=[i])], ignore_index=True)
            print(ExeDF)
            
            i+=1
            return [True,CEOptionDf]
        else:
            return [False, pd.DataFrame()]
    else:
        return [False, pd.DataFrame()]
#--------------------------------------------------------------------------------------------------
def closePos(Price, Option, Time):

    global ExeDF

    ExeDF.loc[(ExeDF['Option'] == Option) & (ExeDF['BPrice'] == ""), 'BPrice'] = Price

    ExeDF.loc[(ExeDF['Option'] == Option) & (ExeDF['ExTime'] == ""), "ExTime"] = Time

    ExeDF.loc[(ExeDF['Option'] == Option) & (ExeDF['Pos'] == "Open"), "Pos"] = "Close"

    print(ExeDF)
#--------------------------------------------------------------------------------------------------
def TradeExecution(df):

    # print("Future df =",df)

    TempOdf = pd.DataFrame()

    
    global OptionDf, FilteredDf, ExeDF, UnavailDateList, count, flag, high, low, Pprice, curr_date



    if df["Date"]!= curr_date:
        count=0
        flag=None
        high= float("-inf")
        low = float("inf")
        curr_date=df["Date"]
        print("Date changed", df["Date"])

    if df["Time"]<= "09:18:00":
        if high<df["High"]:
            high=df["High"]
        if low>df["Low"]:
            low=df["Low"]

    # if df["Time"]>="09:17:55" and count==0:
    #     # high = df["High"]
    #     # low = df["Low"]

    #     curr_date=df["Date"]
    #     count+=1
    #     print(high, low)
    if df["Close"]>high and count==0:
        flag="Above"
        print("high break")
        count+=1
        print(high, low)

    if df["Close"]<low and count==0:
        flag="Below"
        print("Low break")
        count+=1
        print(high, low)

    if df["Time"]>"09:15:00" and df["Date"] not in UnavailDateList:


        if (ExeDF.loc[(ExeDF["Pos"]=="Open")]).empty==False: 

            # TempOdf = (OptionDf.loc[(OptionDf["Time"]==df["Time"])])

            TempOdf = (OptionDf.loc[(OptionDf["Time"]>=df["Time"]) & (OptionDf["Time"]<(pd.to_datetime(df["Time"])+pd.Timedelta(3, unit='m')).strftime("%H:%M:%S"))])

            tempIdx = TempOdf.index

            # print(tempIdx, tempIdx[-1], len(OptionDf)-2)

            TempOdf = TempOdf.reset_index(drop=True)

            if not TempOdf.empty:

                CPrice = TempOdf.loc[len(TempOdf)-1, "Close"]

                # print(tempIdx[0], len(CEOptionDf))
                # if CPrice+(CPrice*50*0.01) < FilteredDf.loc[0, "StopLoss"]:

                #     ExeDF.loc[(ExeDF['Pos'] == "Open"), "StopLoss"] = CPrice+(CPrice*50*0.01)
                #     FilteredDf = ExeDF.loc[ExeDF["Pos"]=="Open"].reset_index()

                if FilteredDf.loc[0, "Option"]=="CE":

                    if df["Close"]>df["vwap"] or tempIdx[-1]==len(OptionDf)-2  or df["Time"]>="15:15:00":

                        closePos(CPrice, FilteredDf.loc[0, "Option"], df["Time"])
                        ExeDF.to_csv(f"VwapOneLegLogWithoutSL.csv")
                else:

                    if df["Close"]<df["vwap"] or tempIdx[-1]==len(OptionDf)-2  or df["Time"]>="15:15:00":

                        closePos(CPrice, FilteredDf.loc[0, "Option"], df["Time"])
                        ExeDF.to_csv(f"VwapOneLegLogWithoutSL.csv")

                    
  

    if df["Date"] not in UnavailDateList and df["Time"]>="09:15:00" and df["Time"]<"15:15:00" and (ExeDF.loc[ExeDF["Pos"]=="Open"]).empty==True and (flag==0 or flag=="Above" or flag=="Below"):

        if not ExeDF.empty:
            preOpt= ExeDF.loc[len(ExeDF)-1, "Option"]
        else:
            preOpt=None

        

        if flag=="Above" or (df["Close"]>df["vwap"] and preOpt!="PE" and flag==0 and len(ExeDF.loc[ExeDF["Date"]==df["Date"]])<5):
            print("flag, close, vwap, option", flag, df["Close"], df["vwap"], preOpt)

            flag_put = createPos(df["Date"], df["Close"], df["Time"], "PE")

            if flag_put[0]==True:

                FilteredDf = ExeDF.loc[ExeDF["Pos"]=="Open"].reset_index()

                OptionDf = query(f_o='O', instrument='FINNIFTY', expiry_dt = FilteredDf.loc[0, "ExpiryDT"], strike=FilteredDf.loc[0, "Strike"], option_type=FilteredDf.loc[0, "Option"], 
                         start_date=pd.to_datetime(df["Date"]), end_date=pd.to_datetime(df["Date"]))

                OptionDf=flag_put[1].copy()
                # print(OptionDf)
                
                flag=0
            else:
                ExeDF = ExeDF.loc[ExeDF['Pos'] != 'Open']

        elif flag=="Below" or (df["Close"]<df["vwap"] and preOpt!="CE" and flag==0 and len(ExeDF.loc[ExeDF["Date"]==df["Date"]])<5):
            # print("flag, close, vwap, option", flag, df["Close"], df["vwap"], preOpt)

            flag_call = createPos(df["Date"], df["Close"], df["Time"], "CE")

            if flag_call[0]==True:
         
                FilteredDf = ExeDF.loc[ExeDF["Pos"]=="Open"].reset_index()

                # OptionDf = query(f_o='O', instrument='FINNIFTY', expiry_dt = FilteredDf.loc[0, "ExpiryDT"], strike=FilteredDf.loc[0, "Strike"], option_type=FilteredDf.loc[0, "Option"], 
                #             start_date=pd.to_datetime(df["Date"]), end_date=pd.to_datetime(df["Date"]))

                OptionDf=flag_call[1].copy()

                # print(OptionDf)
                
                flag=0
            else:
                ExeDF = ExeDF.loc[ExeDF['Pos'] != 'Open']
       
    Pprice = df["Close"]
    
#--------------------------------------------------------------------------------------------------   
 
def main():

    global UnavailDateList

    FutureDF = query(f_o='F', instrument='FINNIFTY', start_date=pd.to_datetime('2022-01-01'), end_date=pd.to_datetime('2022-12-31'))
    dates=list(FutureDF["Date"].unique())
   

    # print(FutureDF.head(80))
    
    FutDFVWAP = pd.DataFrame()
    
    for i in dates:

        filteredFDF = FutureDF.loc[FutureDF["Date"]==i].copy()

        filteredFDF['datetime']=filteredFDF['Date']+" "+filteredFDF["Time"]
        filteredFDF['datetime']=pd.to_datetime(filteredFDF['datetime'])


        filteredFDF = filteredFDF.set_index(['datetime']).resample("3T").agg({"Ticker":"first", "ExpiryDT":"first", "FnO":"first", "Volume":"sum", "Open":"first", "High":"max", "Low":"min", "Close":"last", "Time":"first", "Date":"first"})
        filteredFDF = filteredFDF.dropna()

        
        

        filteredFDF.loc[:, 'Typical_p']=filteredFDF.eval('(Open+High+Low)/3')

        q = filteredFDF.Volume.values
        
        p = filteredFDF.Typical_p.values
    
        filteredFDF=filteredFDF.assign(vwap=(p * q).cumsum() / q.cumsum())
        
        FutDFVWAP = pd.concat([FutDFVWAP, filteredFDF], ignore_index=True).reset_index(drop=True)

    
    print(FutDFVWAP.round(2).head(20))
    FutDFVWAP.apply(TradeExecution, axis=1)

    FutDFVWAP.to_csv("NFOwithVWAPWithoutSL.csv")

    print(ExeDF)

    ExeDF.to_csv(f"VwapOneLegLogWithoutSL.csv")
    print(UnavailDateList)

    with open('UnavailableData.txt', 'w') as file:
    # Iterate over the list and write each item to the file
        for item in UnavailDateList:
            file.write(item + '\n')
    

main()

