import pandas as pd
import numpy as np
import openpyxl
import os
from openpyxl.chart import LineChart, Reference, Series

from datetime import datetime, timedelta, time
from tqdm import tqdm
from pandas_ods_reader import read_ods

def determineTrade(previous, current, threshold):

  if previous * (100 + threshold) / 100 - current < 0:
    return "buy"

  elif previous * (100 - threshold) / 100 - current > 0:
    return "sell"

  else:
    return "keep"

def insertValue(out_dict, buy_count, sell_count, keep_count):
  out_dict["Buy Count"].append(buy_count[0])
  out_dict["Sell Count"].append(sell_count[0])
  out_dict["No trade"].append(keep_count[0])
  out_dict["Buy PNL"].append(buy_count[1])
  out_dict["Sell PNL"].append(sell_count[1])
  out_dict["Total PNL"].append(keep_count[1])
  out_dict["Average"].append(keep_count[2])

def returnDict():
  return {
    "Buy Count": [],
    "Sell Count": [],
    "No trade": [],
    "Buy PNL": [],
    "Sell PNL": [],
    "Total PNL": [],
    "Average": []
  }

def calculateValues(check_points, out_dict, data, single_point, settlement, pre_settlement, threshold, statistics):

  buy_count = [0, 0, 0]
  sell_count = [0, 0, 0]
  keep_count = [0, 0, 0]

  day_before_price = None
  hour_before_price = None

  trade = "keep"
  _ = 0

  for index in tqdm(range(len(data)), desc="processing..."):

    row = data.iloc[index, :]
    check_point = row.tolist()
    check_point.append("")
    check_point.append("")
    check_point.append("")
    check_point.append("")

    time_str = row["timestamp"]

    if time_str[-8:] == pre_settlement:
      if day_before_price != None:
        hour_before_price = float(row["close"])
        trade = determineTrade(day_before_price, hour_before_price, threshold)
        
        check_point[4] = trade

        if trade != "keep":
          _ += 1
          check_point[3] = int(single_point)
          check_point[6] = float(buy_count[1] + sell_count[1])

          check_points.append(check_point)
  
    elif time_str[-8:] == settlement:
      check_point[4] = trade

      if trade == "buy":
        buy_count[0] += 1
        buy_count[1] += int((float(row["close"]) - hour_before_price) * single_point)

        check_point[4] = "result"
        check_point[5] = int((float(row["close"]) - hour_before_price) * single_point)

      elif trade == "sell":
        sell_count[0] += 1
        sell_count[1] += int((hour_before_price - float(row["close"])) * single_point)

        check_point[4] = "result"
        check_point[5] = int((hour_before_price - float(row["close"])) * single_point)

      else:
        keep_count[0] += 1

      trade = "keep"
      day_before_price = float(row["close"])

      check_point[3] = int(single_point)
      check_point[6] = float(buy_count[1] + sell_count[1])

      check_points.append(check_point)
      _ += 1

      statistics.update({"Total PNL": [(buy_count[1] + sell_count[1])]})
      statistics.update({"Days traded": [(buy_count[0] + sell_count[0])]})
      statistics.update({"Total days we could have traded": [(buy_count[0] + sell_count[0] + keep_count[0])]})

  keep_count[1] = buy_count[1] + sell_count[1]
  _counts = buy_count[0] + sell_count[0]
  _value = keep_count[1] / _counts if _counts != 0 else 0
        
  keep_count[2] = round(_value, 3)

  statistics.update({"average profit per trade": keep_count[2]})
  return _
      
if __name__ == "__main__":
  ## path for the input file.
  root_path = "./input"
  out_path = "./output"
  path = "/AutoData/iqfeed_silver_data_20100205-20210208.csv"

  ## prepare the source raw data.
  df_data = pd.read_csv(root_path + path)
  df_data = df_data[["timestamp", "close", "symbol"]]

  ## prepare variables to save data.
  check_points = []
  precious_dict = {}
  item_lens = []

  product_code = "Sl"
  precious_dict.update({product_code: returnDict()})
  single_point = 5000
  settlement = "13:25:00"
  pre_settlement = "12:25:00"
  threshold = 1.81
  statistics = {}

  close_np = df_data["close"].to_numpy()
  sum_np = np.sum(close_np)
  n = len(close_np)
  mean_np = sum_np / n
  abs_np = close_np - mean_np
  sigma_np = np.sum(np.power(abs_np, 2))
  before_root_np = sigma_np / (n - 1)
  stdeva = np.sqrt(before_root_np)
  print(stdeva)

  statistics.update({"STDEVA": [stdeva]})
  statistics.update({"Sharpe": [(mean_np / stdeva ** 0.5)]})

  item_lens.append([product_code, calculateValues(check_points, precious_dict[product_code], df_data, single_point, settlement, pre_settlement, threshold, statistics)])


  ## output checkpoint file.
  df = pd.DataFrame(check_points, columns=["timestamp", "close", "symbol", "single point", "event", "result", "profit"])
  df.to_excel(out_path + "/check_points.xlsx", index=False)

  ## draw graph on checkpoint file.
  book = openpyxl.load_workbook(out_path + "/check_points.xlsx")
  sheet = book.active

  row_over = 0
  for index, item in enumerate(item_lens):

    chart = LineChart()
    chart.title = item[0]
    chart.x_axis.title = "Time"
    chart.y_axis.title = "profit"
    chart.style = 2
    values = Reference(sheet, min_col=6, min_row=row_over+2, max_col=7, max_row=row_over+item[1]+1)
    chart.add_data(values)
    chart.height = 10

    sheet.add_chart(chart, "I" + str(5+row_over))
    row_over += item[1]

  book.save(out_path + "/check_points.xlsx")

  df = pd.DataFrame.from_dict(statistics)
  df.to_excel(out_path + "/silver_statistics.xlsx", index=False)

