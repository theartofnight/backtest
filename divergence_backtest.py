import pandas as pd
import numpy as np
import datetime
import os
from tqdm import tqdm
from copy import deepcopy

## divergence backtest class.
class DBacktest:
    def __init__(self, **args):
        self.input_source = args["input"]
        self.sub_input_souce = args["sub_input"]
        self.output_source = args["output"]
        self.nday = args["Ndays"]
        self.x = args["X"]
        self.before_settlement = args["starting trade"]
        self.columns = [
            "Ticker",
            "Strategy",
            "Percent of days met criteria",
            "Num days met criteria",
            "Daily Mean",
            "Sum",
            "Std",
            "Avg Pnl/Contract",
            "Sharpe",
            "# of Winners",
            "Winners Avg Move",
            "Losers Avg Move",
            "Biggest Winner",
            "Biggest Loser",
            "Win Percentage(of days it actually traded)",
            "Avg Time in Trade (minutes)",
            "avg. p of futures",
            "$ move",
            "tick move",
            "tick size",
        ]
        self.result_list = []
        [self.result_list.append([]) for _ in self.columns]
        self.result = pd.DataFrame(columns=self.columns)
        self.symbol_file = "df_symbols.csv"
        self.product_detail_file = "products.txt"
        self.product_code_file = "BuyStrength_SellWeakness.csv"


    def readFiles(self, product):
        
        self.product = product

        ### read front month data as pandas dataframe.
        self.front_month_pd = pd.read_csv("{}{}{}1.csv".format(self.input_source, self.sub_input_souce, self.product))
        ## convert timestamp column to pandas datetime row.
        self.front_month_pd["timestamp"] = pd.to_datetime(self.front_month_pd["timestamp"])
        self.front_month_pd = self.front_month_pd.sort_values(by="timestamp")

        ### read back month data as pandas dataframe.
        self.second_month_pd = pd.read_csv("{}{}{}2.csv".format(self.input_source, self.sub_input_souce, self.product))
        ### convert timestamp column to pandas datetime row.
        self.second_month_pd["timestamp"] = pd.to_datetime(self.second_month_pd["timestamp"])
        self.second_month_pd = self.second_month_pd.sort_values(by="timestamp")

        ### read symbol file.
        self.symbol_pd = pd.read_csv("{}{}".format(self.input_source, self.symbol_file))

        ### read product detail file.
        self.product_detail = {}
        with open("{}{}".format(self.input_source, self.product_detail_file)) as file:
            for line in file.readlines():
                try:
                    line = line.replace('null', '"null"')
                    _ = eval(line)
                    self.product_detail.update({_["product_code"]: _})
                except:
                    pass

        ### get settlement time for this product.
        self.settlement_time = pd.to_datetime(self.symbol_pd.loc[self.symbol_pd["Symbol"] == "{}#".format(product)]["Settlement"].values[0])

        ### get single point.
        self.single_point = self.symbol_pd[self.symbol_pd["Symbol"] == "{}#".format(product)]["Single point value"].values[0]

        ### calculate tradeable time.
        self.start_time = False
        if self.before_settlement:
            self.start_time = self.settlement_time - pd.Timedelta(minutes=self.before_settlement)

        ### read product code file.
        self.product_code = pd.read_csv("{}{}".format(self.input_source, self.product_code_file))
        product_code = self.product_code.loc[self.product_code["Unnamed: 2"] == self.symbol_pd.loc[self.symbol_pd["Symbol"] == "{}#".format(self.product)]["Product"].values[0]]["Unnamed: 1"].values[0]

        ### get tick_size of this product.
        try:
            self.tick_size = self.product_detail[product_code]["tick_size"]
        except:
            if self.product == "QPA":
                self.tick_size = 0.1
            elif self.product == "@PX":
                self.tick_size = 0.00001
            elif self.product == "@JY":
                self.tick_size = 0.0000005
            else:
                return "exit"

    def process(self):
        for x in tqdm(self.x, desc="iterating over X"):
            self.mainStrategy(x)

    def mainStrategy(self, x):

        ### create a spread like data which is front_month - second_month using close price of minute bar.
        ## implement ffill about all data to avoid mismatching issue.
        front = self.front_month_pd.loc[:, ["close_p", "timestamp"]]
        second = self.second_month_pd.loc[:, ["close_p", "timestamp"]]

        ## found rows that exist on only one side.
        only_in_front = front[~front["timestamp"].isin(second["timestamp"])]
        only_in_front["close_p"] = np.nan
        only_in_second = second[~second["timestamp"].isin(front["timestamp"])]
        only_in_second["close_p"] = np.nan

        ## make two data have the same timestamp columns. 
        second = pd.concat([second, only_in_front])
        second = second.sort_values(by=["timestamp"]).reset_index(drop=True)
        second = second.ffill(axis = 0)
        front = pd.concat([front, only_in_second])
        front = front.sort_values(by=["timestamp"]).reset_index(drop=True)
        front = front.ffill(axis = 0)

        ## create a spread.
        self.spread = deepcopy(front)
        self.spread = self.spread.drop(["close_p"], axis=1)
        self.spread["front"] = front["close_p"]
        self.spread["second"] = second["close_p"]
        self.spread["current_spread"] = front["close_p"] - second["close_p"]


        ### Calculate the difference between the current_spread_price and the previous_day_settlement_spread_price
        ## set timestamp column as index column for convenient.
        self.spread = self.spread.set_index("timestamp")

        ## create new dataframe like data which difference between the current_spread_price and the previous_day_settlement_spread_price.
        _list = [] # the list that will contain difference between previous settlement price and current price.
        __list = [] # the list that will contain previous settlement price.
        temp_spread = deepcopy(self.spread)
        self.total_day = 0
        for day in tqdm(self.spread.resample('D').groups, desc="calculating difference"):
            _day = str(day).split()[0]
            if len(self.spread.loc[_day]) != 0:
                self.total_day += 1
            previous_settlement_time = datetime.datetime(day.year, day.month, day.day, self.settlement_time.hour, self.settlement_time.minute, self.settlement_time.second)
            previous_settlement_time = pd.to_datetime(previous_settlement_time - datetime.timedelta(days=1))
            try:
                _list.append(temp_spread.loc[_day, "current_spread"] - self.spread.loc[previous_settlement_time, "current_spread"])
                temp_spread.loc[_day, "current_spread"] = self.spread.loc[previous_settlement_time, "current_spread"]
                __list.append(temp_spread.loc[_day, "current_spread"])
            except:
                pass
        self.difference_presettlement2current = pd.concat(_list).to_frame()
        self.difference_presettlement2current["previous_settlement_time_price_spread"] = pd.concat(__list).to_frame()

        ### calculate standard deviation.
        try:
            self.difference_presettlement2current["std_for_past_{}_days".format(self.nday)] = self.difference_presettlement2current["current_spread"].rolling('{}D'.format(self.nday)).std()
        except:
            # self.difference_presettlement2current.to_csv("error/{}.csv".format(self.product))
            # self.spread.to_csv("error/spread_{}.csv".format(self.product))
            return False
            

        ### rename a column for convenient.
        self.difference_presettlement2current = self.difference_presettlement2current.rename(columns={"current_spread": "diff_previous_settlement_to_current"})
        
        ### create total data frame that contains all data calculated so far.
        self.total = pd.concat([self.spread, self.difference_presettlement2current], axis=1, join="inner")

        ### do trade.
        ## determine whether buy or sell spread.
        self.total["deter_buy"] = self.total["diff_previous_settlement_to_current"] - x * self.total["std_for_past_{}_days".format(self.nday)]
        self.total["deter_sell"] = -(self.total["diff_previous_settlement_to_current"] + x * self.total["std_for_past_{}_days".format(self.nday)])
        self.total["amount_that_decreases_from_prior_value"] = -1 * abs(self.total["diff_previous_settlement_to_current"].to_frame()).diff()

        _exit = True
        _trade = []
        _profit = []
        _only_profit = []
        _time = []

        _decrease = 0
        _start_time = 0
        _std = 0
        _wallet = [0, 0, 0]

        for row in tqdm(self.total.itertuples()):
            row = row._asdict()
            

            _flag = True
            if self.start_time:
                _now = row["Index"]
                _start_time = datetime.datetime(_now.year, _now.month, _now.day, self.start_time.hour, self.start_time.minute, self.start_time.second)
                _settlement_time = datetime.datetime(_now.year, _now.month, _now.day, self.settlement_time.hour, self.settlement_time.minute, self.settlement_time.second)
                _flag = _exit and row["Index"] > _start_time and row["Index"] <= _settlement_time
            else:
                _flag = _exit

            if _flag:
                ## finding buy or sell entry.
                if row["deter_buy"] > 0:
                    ## do buy action
                    _trade.append("buy")
                    _exit = False
                    _decrease = 0
                    _std = row["std_for_past_{}_days".format(self.nday)]
                    _wallet = [1, row["front"], row["second"]]
                    _start_time = row["Index"]
                elif row["deter_sell"] > 0:
                    ## do sell action
                    _trade.append("sell")
                    _exit = False
                    _decrease = 0
                    _std = row["std_for_past_{}_days".format(self.nday)]
                    _wallet = [-1, row["front"], row["second"]]
                    _start_time = row["Index"]
                else:
                    _trade.append(np.nan)
                _profit.append(np.nan)
                _only_profit.append(np.nan)
                _time.append(np.nan)
            else:
                ## finding exit entry.
                if not _exit:
                    _decrease += row["amount_that_decreases_from_prior_value"]

                if not _exit and _decrease > x * _std or str(row["Index"]).split()[1] == str(self.settlement_time).split()[1]:
                    _trade.append("exit")
                    _exit = True
                    if _wallet[0] == 1:
                        ## If buy action was taken recently.
                        _ = _wallet[1] - row["front"] + row["second"] - _wallet[2]
                        _profit.append(_)
                        _only_profit.append(_ * self.single_point)
                    elif _wallet[0] == -1:
                        ## If sell action was taken recently.
                        _ = row["front"] - _wallet[1] + _wallet[2] - row["second"]
                        _profit.append(_)
                        _only_profit.append(_ * self.single_point)
                    _time.append((row["Index"] - _start_time).seconds / 60)
                else:
                    _trade.append(np.nan)
                    _profit.append(np.nan)
                    _only_profit.append(np.nan)
                    _time.append(np.nan)
        self.total["trade"] = _trade
        self.total["profit"] = _profit
        self.total["profit_by_singlepoint"] = _only_profit
        self.total["position_holding_time"] = _time
        
        ## remove useless columns in this stage.
        self.total = self.total.drop(["deter_buy", "deter_sell"], axis=1)
        self.total = self.total.round(6)
        self.total.to_csv("{}{} checkpoints.csv".format(self.output_source, self.product), index=False)

        ###### new output format#############
        profit = self.total["profit"]
        valid_data = profit.dropna()
        self.meet_day = 0
        for row in valid_data.resample('D').groups:
            _day = str(row).split()[0]
            df = valid_data.loc[_day]
            if len(df) != 0:
                self.meet_day += 1

        describe_point = self.total["profit_by_singlepoint"].describe().to_frame()
        describe = profit.describe().to_frame()
        self.result_list[0].append(self.product)
        self.result_list[1].append("std for {}days, x={}".format(self.nday, x))
        self.result_list[2].append("{}%".format(round(self.meet_day * 100 / self.total_day, 4)))
        self.result_list[3].append(self.meet_day)
        self.result_list[4].append(profit.sum() / self.total_day)
        self.result_list[5].append(profit.sum())
        self.result_list[6].append(describe.loc["std"].values[0])
        self.result_list[7].append(describe_point.loc["mean"].values[0])

        self.result_list[8].append(((profit.sum() / self.total_day) / describe.loc["std"].values[0]) * (252 ** 0.5))
        self.result_list[9].append(len(valid_data.loc[valid_data > 0]))
        self.result_list[10].append(self.total.loc[self.total["profit"] > 0]["profit"].mean())
        self.result_list[11].append(self.total.loc[self.total["profit"] < 0]["profit"].mean())
        self.result_list[12].append(describe.loc["max"].values[0])
        self.result_list[13].append(describe.loc["min"].values[0])
        self.result_list[14].append("{}%".format(round(len(valid_data.loc[valid_data > 0]) * 100 / len(valid_data), 4)))
        self.result_list[15].append(self.total.loc[self.total["trade"] == "exit"]["position_holding_time"].mean())
        self.result_list[16].append(self.total["front"].mean())
        _move = describe.loc["mean"].values[0] * 100 / self.total["front"].mean()
        self.result_list[17].append(_move)
        self.result_list[18].append(_move / self.tick_size)
        self.result_list[19].append(self.tick_size)

    def writeOutput(self):
        for index, column in enumerate(self.columns):
            try:
                self.result[column] = self.result_list[index]
            except:
                pass
        self.result = self.result.round(8)
        self.result.to_csv("{}result.csv".format(self.output_source), index=False)

    def prepareOutput(self):
        os.makedirs(self.output_source, exist_ok=True)
    ##

if __name__ == "__main__":
    ## specify initial data.
    init = {
        "input": "./Divergence Input/",
        "sub_input": "resampled/",
        "output": "./Divergence_Output/",
        "Ndays": 3,
        "X": [1, 2, 3],
        "starting trade": 100 # or False
    }

    ## specify products name and its settlement time.
    products = [
        "EB", 
        "QHO", 
        "QGC", 
        "QHG", 
        "QPL", 
        "QRB"
    ]

    products = [
        "@AD",
        "@BO",
        "@BP",
        "@C",
        "@CC",
        "@CD",
        "CRD",
        "@CT",
        "@DX",
        "EB",
        "@ES",
        "@EU",
        "GAS",
        "@GF",
        "@HE",
        "IHO",
        "IRB",
        "@JY",
        "@KC",
        "@KW",
        "@LE",
        "@MFS",
        "@MME",
        "@NQ",
        "@PX",
        "QBZ",
        "QCL",
        "QGC",
        "QHG",
        "QHO",
        "QNG",
        "QPA",
        "QPL",
        "QRB",
        "QSI",
        "@RTY",
        "@S",
        "@SB",
        "@SF",
        "@SM",
        "@UB",
        "@US",
        "@W",
        "@YM",
    ]

    ## create and initialize object.
    dbt = DBacktest(**init)

    ## prepare output directory.
    dbt.prepareOutput()

    ## start backtest per backtest.
    for product in tqdm(products, desc="total process"):
        ## read front and back month data about one product.
        if dbt.readFiles(product) == "exit":
            continue

        ## perform main process.
        dbt.process()
    
    dbt.writeOutput()