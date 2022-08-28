import numpy as np
import pandas as pd


def convert_to_executable_time(df, execution_delay):
    df = df.reset_index()
    df['timestamp'] = np.maximum(
        df['timestamp'] + pd.to_timedelta(df['delay'] + execution_delay, unit='s'),
        df['timestamp']
    )
    return df.set_index(['model_id', 'timestamp']).sort_index()


def asfreq_positions(df, freq):
    dfs = []
    for model_id, df_model in df.groupby('model_id'):
        df_model = df_model.reset_index()
        df_model = df_model.drop_duplicates('timestamp', keep='last')
        df_model = df_model.set_index('timestamp').sort_index()
        dfs.append(df_model.asfreq(freq, method='ffill'))
    return pd.concat(dfs).reset_index().set_index(['model_id', 'timestamp'])
