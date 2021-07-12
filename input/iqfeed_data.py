#!/usr/bin/env python
# coding: utf-8

# Helper article:
# https://www.quantstart.com/articles/Downloading-Historical-Intraday-US-Equities-From-DTN-IQFeed-with-Python/

# from pandas.tseries.holiday import USFederalHolidayCalendar
import pandas as pd

from pandas.tseries.offsets import BDay
import sys
import socket
import os
# import paramiko
# import pywinauto
# from pywinauto import Application
import time
import json
import smtplib
import pytz 

import numpy as np
import pandas as pd
from pandas.tseries.offsets import BDay
import sys
import socket
import os
# import pywinauto
# from pywinauto import Application
import time
import json
import smtplib
import pytz 
import io
from copy import deepcopy
import datetime
import pprint

import xlrd
import xlsxwriter
from copy import deepcopy
import re

from tqdm import tqdm



#####################################################################################################
# CUSTOM TRADING HOLIDAYS
#####################################################################################################
from pandas.tseries.holiday import AbstractHolidayCalendar, Holiday, nearest_workday, USMartinLutherKingJr, USPresidentsDay, GoodFriday, USMemorialDay, USLaborDay, USThanksgivingDay

class USTradingCalendar(AbstractHolidayCalendar):
    rules = [
        Holiday('NewYearsDay', month=1, day=1, observance=nearest_workday),
        USMartinLutherKingJr,
        USPresidentsDay,
        GoodFriday,
        USMemorialDay,
        Holiday('USIndependenceDay', month=7, day=4, observance=nearest_workday),
        USLaborDay,
        USThanksgivingDay,
        Holiday('Christmas', month=12, day=25, observance=nearest_workday)
    ]
    
def get_trading_close_holidays(year):
    import datetime
    inst = USTradingCalendar()
    return inst.holidays(datetime.datetime(year-1, 12, 31), datetime.datetime(year, 12, 31))

def get_prev_trade_date(date):
        
    date = pd.to_datetime(pd.to_datetime(date).strftime('%Y-%m-%d'))
    year = date.year

    start_date = date - pd.Timedelta(30, unit='D')

    dr = pd.date_range(start=start_date, end=date, freq=BDay())
    df = pd.DataFrame(dr, columns=['date'])
    holidays = get_trading_close_holidays(year)
    df.loc[:, 'trading_days'] = ~df['date'].isin(holidays)

    df = df[df['date'] != date]
    df = df.loc[df['trading_days'], :]
    df = df.sort_values('date', ascending=False).reset_index(drop=True)
    prev_date = df.loc[0, 'date']
    
    return prev_date

#####################################################################################################
# GLOBAL VARIABLES
#####################################################################################################
SP = '@ESM20'
GSCI = 'GSG'
SYMBOLS = [SP, GSCI]

IQFEED_PATH = 'C:\Program Files (x86)\DTN\IQFeed'
IQFEED_START_EXE = 'IQLinkLauncher.exe'
IQFEED_CLIENT_EXE = 'iqconnect.exe'
IQFEED_LINK_EXE = 'iqlink.exe'

# Monick Windows Box
IQF_LOGIN = '479984'
IQF_PASSWORD = '88216885'


def datetime_to_epoch(dt):
    return pd.to_datetime(dt).value // 10**9
    
    
def utc_to_local(utc_dt, tz='America/New_York'):
    local_tz = pytz.timezone(tz) #THIS NEEDS TO BE PASSED IN
    local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz)
    return local_tz.normalize(local_dt)

# def scp_json_to_server(server, port, user, password, local_path, remote_path, verbose=False):

#     client = paramiko.SSHClient()
#     client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#     client.connect(hostname=server, username=user, password=password, port=port)
    
#     if verbose:
#         stdin, stdout, stderr = client.exec_command('pwd')
#         data = stdout.read() + stderr.read()
#         print(data)
    
#     sftp = client.open_sftp()
#     sftp.put(fp, remote_path)
    
#     sftp.close()
#     client.close()
    
#####################################################################################################
# IQFEED
#####################################################################################################

# def stop_iqlink(close=True):
    
#     try:
#         app = Application().connect(path=IQFEED_LINK_EXE)
#         if close:
#             app.kill()
#             time.sleep(2)
#             return False
#         else:
#             return True
        
#     except pywinauto.application.ProcessNotFoundError:
#         return False
        
        
# def stop_iqlinklauncher(close=True):
    
#     try:
#         app = Application().connect(path=IQFEED_START_EXE)
#         if close:
#             app.kill()
#             time.sleep(2)
#             return False
#         else:
#             return True
        
#     except pywinauto.application.ProcessNotFoundError:
        # return False
        

# def iqf_launcher_start():

#     start_exe = ('{} "{}"'.format(IQFEED_START_EXE, IQFEED_PATH))
    
#     print("\nAttempting to launch IQFeed Launcher...")
#     Application().start(start_exe)
#     app = Application(backend="uia").connect(path=IQFEED_START_EXE)
#     print("\Launched IQFeed Launcher...")
    
#     print("\nAttempting to start IQFeed Launcher...")
#     app['IQLink Launcher']['Start IQLink'].click()
#     print("\nStarted IQFeed Launcher.")
    

# def iqf_launcher_stop():

#     try:

#         app = Application(backend="uia").connect(path=IQFEED_START_EXE)
        
#         app.Dialog.print_control_identifiers()
        
#         print("\nAttempting to stop IQFeed Launcher...")
#         app['IQLink Launcher']['Stop IQLinkButton'].click()
#         print("\nStopped IQFeed Launcher.")
        
#         app['IQLink Launcher']['Exit'].click()
#         print("Exiting...")
        
#         stop_iqlink()
#         stop_iqlinklauncher()
            
#     except pywinauto.application.ProcessNotFoundError:
#         print("IQFEED Launcher not running.")
#         return True
        

# def iqf_client():
    
#     client_exe = ('{} "{}"'.format(IQFEED_CLIENT_EXE, IQFEED_PATH))
    
#     print("\nAttempting to launch IQFeed Client...")
#     Application().start(client_exe)
#     app = Application(backend="uia").connect(path=IQFEED_CLIENT_EXE)
#     print("\Launched IQFeed Client...")
    
#     print("\nAttempting to login to IQFeed Client...")
#     # login textbox
#     app['IQ Connect Login']['Edit1'].set_text(IQF_LOGIN)
#     # password textbox
#     app['IQ Connect Login']['Edit2'].set_text(IQF_PASSWORD)
#     time.sleep(1)
#     # click connect to login
#     app['IQ Connect Login']['Connect'].click()
    
#     print("\nLogged in to IQFeed Client.")


def read_historical_data_socket(sock, recv_buffer=4096):
    """
    Read the information from the socket, in a buffered
    fashion, receiving only 4096 bytes at a time.

    Parameters:
    sock - The socket object
    recv_buffer - Amount in bytes to receive per read
    """
    buffer = ""
    data = ""
    while True:
        data = sock.recv(recv_buffer)
        buffer += data.decode()

        # Check if the end message string arrives
        if "!ENDMSG!" in buffer:
            break
   
    # Remove the end message string
    buffer = buffer[:-12]
    return buffer


def request_iqf_data(syms, start_date, end_date, host='127.0.0.1', port=9100): 

    # define the name of the directory to be created
    path_folder = os.getcwd() + '/AutoData/'
    if not os.path.isdir(path_folder):
        try:
            os.mkdir(path_folder)
        except OSError:
            print ("Creation of the directory %s failed" % path_folder)
        else:
            print ("Successfully created the directory %s " % path_folder)
    else:
        print ("The directory %s already exists" % path_folder)
    
    # Download each symbol to disk
    time_interval = 1 * 60 #half hour to seconds
    end_date = pd.to_datetime(end_date)
    start_date = pd.to_datetime(start_date)
    end_date_ymd = pd.to_datetime(end_date).strftime('%Y%m%d')
    start_date_ymd = pd.to_datetime(start_date).strftime('%Y%m%d')
    
    print("dates from iqfeed")
    print("start_date: {}".format(start_date_ymd))
    print("run_date: {}".format(end_date_ymd))
    
    df_list = []
    for sym in tqdm(syms):        

        print("Downloading symbol: %s..." % sym)
        # Construct the message needed by IQFeed to retrieve data
        #message = "HIT,%s,10,20200330 000000,20200401 235959,,000000,235959,1\n" % sym
        message = f"HIT,{sym},{time_interval},{start_date_ymd} 000000,{end_date_ymd} 235959,,000000,235959,1\n"
        print(message)
       
        # Open a streaming socket to the IQFeed server locally
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))

        # Send the historical data request
        # message and buffer the data
        sock.sendall(message.encode())
        data = read_historical_data_socket(sock)
        sock.close
        time.sleep(1)
        
        if data != 'E,!NO_DATA!,':

            # Remove all the endlines and line-ending
            # comma delimiter from each record
            data = "".join(data.split("\r"))
            data = data.replace(",\n","\n")[:-1]
            cols = ['timestamp', 'high', 'low', 'open', 'close', 'volume', 'period_volume']
            df = pd.read_csv(io.StringIO(data), names=cols, header=None)
            df.loc[:, 'symbol'] = sym
    
            # concatenate to master CSV
            df_list.append(deepcopy(df))
            
    df_data = pd.concat(df_list)
        
    # write csv to save historical data
    csv_fn = f"iqfeed_year_data_{start_date_ymd}-{end_date_ymd}.csv"
    csv_path = os.path.join(path_folder, csv_fn)
    df_data.to_csv(csv_path, index=False)
    
    return df_data


def send_email(fromaddr, frompwd, toaddr, subject, msg_body, attach_fn=False, attach_fp=False):
    
    import smtplib
    from email import encoders
    try: 
        from email.MIMEMultipart import MIMEMultipart
    except ImportError:
        from email.mime.multipart import MIMEMultipart
    
    try:
        from email.MIMEText import MIMEText
    except ImportError:
        from email.mime.text import MIMEText
        
    try:
        from email.MIMEBase import MIMEBase
    except ImportError:
        from email.mime.base import MIMEBase

    toaddr = toaddr if isinstance(toaddr, list) else [toaddr]

    msg = MIMEMultipart()
    msg['From'] = fromaddr
    msg['To'] = ', '.join(toaddr)
    msg['Subject'] = subject
    msg.attach(MIMEText(msg_body, 'plain'))

    if attach_fn and attach_fp:
        fp = os.path.join(attach_fp, attach_fn)
        attachment = open(fp, "rb")
        part = MIMEBase('application', 'octet-stream')
        part.set_payload((attachment).read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', "attachment; filename= %s" % attach_fn)
        msg.attach(part)
    
    # non-SSL option if desired
    # smtp_host = 'smtp.gmail.com'
    # smtp_port = 587
    # server = smtplib.SMTP(smtp_host, smtp_port)
    # server.starttls()
    
    # more desired SSL option
    smtp_host = 'smtp.gmail.com'
    smtp_ssl_port = 465
    server = smtplib.SMTP_SSL(smtp_host, smtp_ssl_port)
    server.login(fromaddr, frompwd)
    text = msg.as_string()
    server.sendmail(fromaddr, toaddr, text)
    
    server.quit()


if __name__ == "__main__":

    # terminate existing IQFeed processes if running for good measure
    # stop_iqlink()
    # stop_iqlinklauncher()
    
    # stage variables
    msg_builder = ""
    yyyymmdd = utc_to_local(pd.Timestamp.utcnow()).strftime('%Y-%m-%d')
    yyyymmdd_str = utc_to_local(pd.Timestamp.utcnow()).strftime('%Y%m%d')
    fn = 'sp_gsci_indicator.json'
    fp = os.path.join(os.getcwd(), 'AutoData')
    f = os.path.join(fp, fn)
   
    # terminate existing IQFeed processes if running
    # stop_iqlink()
    # stop_iqlinklauncher()
    
    # launch new instance and connect to IQFeed
    # iqf_launcher_start()
    # time.sleep(3)
    # iqf_client()
    
    df = pd.read_excel("IQFeed_Symbols.xlsx", engine="openpyxl")

    start_date = '2020-03-20'
    end_date = '2021-03-20'

    symbols = df["Symbol"].tolist()
    real_symbols = [item for item in symbols if isinstance(item, str)]

    SYMBOLS = real_symbols
    # SYMBOLS = ["QSI#"]
    SYMBOLS = ["QCL#"]

    df_data = request_iqf_data(SYMBOLS, start_date, end_date)
    time.sleep(3)

    # close once we've received the data
    # stop_iqlink()
    # stop_iqlinklauncher()

    print("Done.")
    


        
