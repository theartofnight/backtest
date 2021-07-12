import pandas as pd
import re

if __name__ == "__main__":

    filename = "quick_study.xlsx"
    # df = pd.read_csv("2021_hedge_with_future_MON-488.csv")
    df = pd.read_csv("202_future_MON-488_expandeddata.csv")

    df['symbol'] = df.apply(lambda row: row.ticker[:-2] if bool(re.match('.+[A-Z][0-9]', row.ticker)) else row.ticker, axis=1)
    
    df['symbol'] = df.apply(lambda row: row.symbol.split(' ')[0], axis=1)

    df_grouped = df.groupby(['symbol', 'direction'])
    df_symbol = df_grouped.sum()
    df_symbol['average PnL'] = df_symbol['PnL'] / df_symbol['Size']
    df_symbol = df_symbol.round(0)

    df_s = df.groupby(['symbol']).sum()
    df_s['average PnL'] = df_s['PnL'] / df_s['Size']

    df_symbol['PnL for symbol'] = df_symbol['PnL']
    _tuples = df_symbol.index.tolist()

    _keys = []
    _list = [[], [], []]
    for symbol, direction in _tuples:
        _list[0].append(df_s.loc[symbol]['Size'])
        _list[1].append(df_s.loc[symbol]['PnL'])
        _list[2].append(df_s.loc[symbol]['average PnL'])
        if direction == 'B':
            _keys.append([symbol, df_s.loc[symbol]['average PnL']])
    
    _keys.sort(key=lambda item: item[1])

    df_symbol['Size for symbol'] = _list[0]
    df_symbol['PnL for symbol'] = _list[1]
    df_symbol['average PnL for symbol'] = _list[2]

    df_symbol = df_symbol.sort_values(by="average PnL for symbol")
    df_symbol = df_symbol.round(0)


    df_symbol.loc['total', 'Size'] = df_symbol['Size'].sum()
    df_symbol.loc['total', 'PnL'] = df_symbol['PnL'].sum()
    df_symbol.loc['total', 'average PnL'] = round(df_symbol['PnL'].sum() / df_symbol['Size'].sum(), 0)
    print(df_symbol)
    df_symbol.to_excel(filename)
    # df_symbol.to_csv("per symbol and direction.csv")




    

    # df_tag = df.groupby(['tag']).sum()
    # df_tag['average PnL'] = df_tag['PnL'] / df_tag['Size']
    # df_tag = df_tag.round(3)

    # df_tag.to_csv("per tag.csv")

    