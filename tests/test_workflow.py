import pandas as pd

from workflowlib.workflow import run


def test_run_step(data_path):
    process = {
        'run': 'channel.tclogger@v1',
        'params': {
            'source': data_path / 'ChannelV2TCLog/2024-01-16T11-26-54.csv',
        },
    }
    df = run(process)
    assert isinstance(df, pd.DataFrame)


def test_run_pipe(data_path):
    process = [
        {
            'run': 'channel.tclogger@v1',
            'params': {
                'source': data_path / 'ChannelV2TCLog/2024-01-16T11-26-54.csv',
            },
        },
        {
            'run': 'select.columns@v1',
            'params': {
                'select': {
                    "timestamp": "timestamp",
                    "sample-downstream": "temperature",
                },
            },
        },
    ]
    df = run(process)
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ['timestamp', 'temperature']
