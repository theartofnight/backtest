import pandas as pd
import numpy as np
import datetime
from tqdm import tqdm
import os

class momentum:
    def __init__(self, *args):
        self.df = pd.read_csv(args[0])
        self.df['timestamp'] = pd.to_datetime(self.df['timestamp'])
        self.percentiles = args[1]
        self.df = self.df.set_index('timestamp')
        self.df = self.df.close
        self.percents = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]

    def testStrategy(self):
        ## make check points file and prepare daily survey.
        total_series = []
        for day in tqdm(self.df.resample('D').groups):
            _sub_result = []
            _day = str(day).split()[0]
            _dict, criteria = self._process_day(self.df[_day], _day)
            for key in _dict:
                [_sub_result.append(item) for item in _dict[key]]
            _index = []
            for _ in range(len(self.percentiles)):
                _index.append("total trade")
                _index.append("sell")
                _index.append("buy")
                _index.append("profit")

            series = pd.Series(_sub_result, index=_index)
            series = criteria.append(series)
            series = series.rename(_day)
            total_series.append(series)
        total_frame = pd.concat(total_series, axis=1)
        total_frame = total_frame.T
        total_list = []
        for i in range(len(self.percentiles) + 1):
            if i == 0:
                total_list.append(pd.concat({'stats': total_frame.iloc[:, :14]}, axis=1))
            else:
                total_list.append(pd.concat({self.percentiles[i-1]: total_frame.iloc[:, 14 + (i - 1) * 4: 14 + i * 4]}, axis=1))
        
        self.total_frame = pd.concat(total_list, axis=1)
        self.total_frame = self.total_frame.round(3)
        
        profit_frame = total_frame.iloc[:, 14:]
        final_stats = profit_frame.describe(percentiles=self.percents).loc[:, 'profit']

        profit_frame = profit_frame.rename(columns={'profit': 'total profit'})
        for _ in range(len(self.percentiles)):
            profit_frame.insert((len(self.percentiles) - _) * 4, 'win%', 1, True)
            profit_frame.insert((len(self.percentiles) - _) * 4 + 1, 'sharpe', 1, True)

        self.profit_frame = profit_frame.sum()
        self.profit_frame = self.profit_frame.to_frame().T

        for i in range(len(self.percentiles)):
            _list = [key for key in final_stats.iloc[:, 0].index]
            _list.reverse()
            for key in _list:
                self.profit_frame.insert((len(self.percentiles) - i) * 6, key, final_stats.iloc[:, 0][key], True)

        total_list = []
        for i in range(len(self.percentiles)):
            total_list.append(pd.concat({self.percentiles[i]: self.profit_frame.iloc[:, i * 20: (i + 1) * 20]}, axis=1))
        self.profit_frame = pd.concat(total_list, axis=1)

        for key in self.percentiles:
            self.profit_frame[key, 'win%'] = self.profit_frame[key, 'total trade'] / self.profit_frame[key, 'win%'] * 100
            self.profit_frame[key, 'sharpe'] = (self.profit_frame[key, 'mean'] / self.profit_frame[key, 'std']) * (252 ** 0.5)

        self.profit_frame = self.profit_frame.T.round(5)

    def writeOutput(self):
        output = './momentum/'
        os.makedirs(output, exist_ok=True)
        self.total_frame.to_csv(output + "momentum checkpoints.csv")
        self.profit_frame.to_csv(output + "momentum result.csv")

    def _process_day(self, frames, day):
        criteria = frames.describe(percentiles=self.percents)
        __ = {}
        sub_series = frames.between_time('14:25', '14:30')
        sub_list = sub_series.to_list()
        for percentile in self.percentiles:
            threshold = criteria.get('std') * percentile
            _ = []
            for item in sub_list[:3]:
                if sub_list[0] - item > threshold:
                    _.append(1) # total trade
                    _.append(1) # sell
                    _.append(0) # buy
                    _.append(item - sub_list[-1])
                    break
            else:
                for item in sub_list[:3]:
                    if item - sub_list[0] > threshold:
                        _.append(1) # total trade
                        _.append(0) # sell
                        _.append(1) # buy
                        _.append(sub_list[-1] - item)
                        break
                else:
                    _.append(0)
                    _.append(0)
                    _.append(0)
                    _.append(0)
            __.update({percentile: _})
        return __, criteria


if __name__ == "__main__":
    _input = [
        './input/AutoData/iqfeed_year_data_20200320-20210320.csv',
        [.01, .03, .05, .10, .15, .20, .30, .40, .50]
    ]
    momentum_strategy = momentum(*_input)
    momentum_strategy.testStrategy()
    momentum_strategy.writeOutput()