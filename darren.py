import pandas as pd
import numpy as np

from tqdm import tqdm

def commonProcess(df_temp, name):
    # df_temp = df_temp[['ticker_alias', 'theoretical_pnl_contract']]
    df_temp = df_temp.rename(columns={'theoretical_pnl_contract': name})
    group_dict = df_temp.groupby(['ticker_alias']).groups

    _temp = []
    for key in tqdm(group_dict, desc="sub process"):
        df_basic = df_temp.iloc[group_dict[key]]
        sr_front = df_basic[[name]].describe()

        # sr_transposed = sr_front.transpose()
        # sr_transposed.index = [key]

        sr_front = sr_front.rename(columns={name : key})

        ## add other statistics standard.
        df_basic.loc[:, 'win'] = df.apply(lambda row: 1 if row.theoretical_pnl_contract > 0 else 0, axis=1)
        df_basic.loc[:, 'unit_pnl'] = df_basic[name]

        extra_dict = {}
        new_columns = [
            'win%', 'mean_unit_pnl', 'std_unit_pnl', 'total_dates', 'total_pnl', 'total_volume', 'avg_daily_pnl', 'mn sharpe'
        ]

        extra_dict.update({new_columns[0]: np.mean(df_basic['win']) * 100})
        extra_dict.update({new_columns[1]: np.mean(df_basic['unit_pnl'])})
        extra_dict.update({new_columns[2]: np.std(df_basic['unit_pnl'])})

        extra_dict.update({new_columns[3]: len(set(df_basic['date']))})
        extra_dict.update({new_columns[4]: df_basic[name].sum()})
        extra_dict.update({new_columns[5]: len(df_basic.iloc[:, 0])})

        df_daily_pnl = df_basic.groupby('date')[name].sum().reset_index()
        avg_daily_pnl = np.mean(df_daily_pnl[name])
        avg_daily_std = np.std(df_daily_pnl[name])

        extra_dict.update({new_columns[6]: avg_daily_pnl})
        
        try:
            sharpe = (avg_daily_pnl / avg_daily_std) * (252 ** 0.5)
        except:
            sharpe = np.nan

        extra_dict.update({new_columns[7]: sharpe})
        df_extra = pd.DataFrame.from_dict(extra_dict, orient="index", columns=[key])
        df_complete = sr_front.append(df_extra)
        df_complete = df_complete.round(3)
        

        _temp.append(df_complete)

    try:
        df_front = pd.concat(_temp, axis=1)
    except:
        df_front = pd.DataFrame([])
    df_whole = pd.concat({name: df_front}, axis=1)
    df_whole.index.name = 'stats'
    
    return df_whole

if __name__ == "__main__":
    df = pd.read_csv("MON-177_to_moises_rerun.csv")
    threshes = [[0.125, -0.125], [0.25, -0.25], [0.5, -0.5]]

    for thresh in tqdm(threshes, desc="total process"):
        frames = []
        df_temp = df[(df['global_mn_signal'] >= thresh[0]) & (df['global_pm_energy_signal'] >= thresh[0])].reset_index(drop=True)
        df_sum = commonProcess(df_temp.copy(), "aligned bulish")
        frames.append(df_sum)

        df_temp = df[(df['global_mn_signal'] <= thresh[1]) & (df['global_pm_energy_signal'] <= thresh[1])].reset_index(drop=True)
        df_sum = commonProcess(df_temp.copy(), "aligned bearish")
        frames.append(df_sum)

        df_temp = df[(df['global_mn_signal'] >= thresh[0]) & (df['global_pm_energy_signal'] <= thresh[1])].reset_index(drop=True)
        df_sum = commonProcess(df_temp.copy(), "divergingA")
        frames.append(df_sum)

        df_temp = df[(df['global_mn_signal'] <= thresh[1]) & (df['global_pm_energy_signal'] >= thresh[0])].reset_index(drop=True)
        df_sum = commonProcess(df_temp.copy(), "divergingB")
        frames.append(df_sum)

        df_temp = df[
            ((df['global_mn_signal'] >= thresh[1]) & (df['global_mn_signal'] <= thresh[0]))
            & 
            (df['global_pm_energy_signal'] >= thresh[0])
        ].reset_index(drop=True)
        df_sum = commonProcess(df_temp.copy(), "neutral bullish")
        frames.append(df_sum)

        df_temp = df[
            ((df['global_mn_signal'] >= thresh[1]) & (df['global_mn_signal'] <= thresh[0]))
            & 
            (df['global_pm_energy_signal'] <= thresh[1])
        ].reset_index(drop=True)
        df_sum = commonProcess(df_temp.copy(), "neutral bearish")
        frames.append(df_sum)
    
        df_real = frames[0].copy()
        

        try:
            result = pd.merge(frames[0], frames[1], how="outer", on="stats")
        except:
            pass

        try:
            result = pd.merge(result, frames[2], how="outer", on="stats")
        except:
            pass

        try:
            result = pd.merge(result, frames[3], how="outer", on="stats")
        except:
            pass

        try:
            result = pd.merge(result, frames[4], how="outer", on="stats")
        except:
            pass

        try:
            result = pd.merge(result, frames[5], how="outer", on="stats")
        except:
            pass

        result.to_csv("./out/thresh is {}.csv".format(str(thresh)))

    
