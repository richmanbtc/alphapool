import pandas as pd
from pandas.testing import assert_frame_equal


def test_submit(test, client):
    client.submit(
        tournament='crypto_v1',
        timestamp=int(pd.to_datetime('2020/01/01 00:00:00', utc=True).timestamp()),
        model_id='model1',
        positions={
            'btc': 0.5,
            'eth': -0.5,
        }
    )
    client.submit(
        tournament='crypto_v1',
        timestamp=int(pd.to_datetime('2020/01/01 00:00:00', utc=True).timestamp()),
        model_id='model2',
        positions={
            'btc': 0.5,
            'xrp': -0.5,
        }
    )
    df = client.get_positions('crypto_v1')
    expected = pd.DataFrame([
        {
            'timestamp': pd.to_datetime('2020/01/01 00:00:00', utc=True),
            'model_id': 'model1',
            'p.btc': 0.5,
            'p.eth': -0.5,
            'p.xrp': 0.0,
        },
        {
            'timestamp': pd.to_datetime('2020/01/01 00:00:00', utc=True),
            'model_id': 'model2',
            'p.btc': 0.5,
            'p.eth': 0.0,
            'p.xrp': -0.5,
        },
    ]).set_index(['model_id', 'timestamp'])
    assert_frame_equal(df.drop('delay', axis=1), expected)

