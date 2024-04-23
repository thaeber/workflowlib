import pandas as pd

from workflowlib import Workflow, run
from workflowlib.process import ProcessBase
from workflowlib.registry import register


def test_run_step(data_path):
    process = {
        'run': 'channel.tclogger@v1',
        'params': {
            'source': data_path / 'ChannelV2TCLog/2024-01-16T11-26-54.csv',
        },
    }
    df = run(process)
    assert isinstance(df, pd.DataFrame)


def test_run_sequence(data_path):
    process = [
        {
            'run': 'channel.tclogger@v1',
            'params': {
                'source': data_path / 'ChannelV2TCLog/2024-01-16T11-26-54.csv',
            },
        },
        {
            'run': 'dataframe.select.columns@v1',
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


class TestWorkflow:

    def test_workflow_with_single_process(self, data_path):
        descriptor = {
            'run': 'mks.ftir@v1',
            'params': {
                'source': str(data_path / 'mks_ftir/2024-01-16-conc.prn'),
            },
        }

        # create workflow
        workflow = Workflow.create(descriptor)
        assert isinstance(workflow, Workflow)

        # run workflow
        df = workflow.run()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 16

    def test_workflow_with_process_sequence(self, data_path):
        descriptor = [
            {
                'run': 'mks.ftir@v1',
                'params': {
                    'source': str(data_path / 'mks_ftir/2024-01-16-conc.prn'),
                },
            },
            {
                'run': 'dataframe.select.timespan@v1',
                'params': {
                    'column': 'timestamp',
                    'start': '2024-01-16T10:05:22.5',
                    'stop': '2024-01-16T10:05:27.5',
                },
            },
            {
                'run': 'dataframe.select.columns@v1',
                'params': {
                    'select': {
                        "timestamp": "timestamp",
                        "NH3 (3000) 191C (2of2)": "NH3",
                        "N2O (100,200,300) 191C (1of2)": "N2O",
                        "NO (350,3000) 191C": "NO",
                        "NO2 (2000) 191C (2of2)": "NO2",
                        "NO2 (150) 191C (1of2)": "NO2b",
                        "H2O% (25) 191C": "H2O",
                    }
                },
            },
        ]

        # create workflow
        workflow = Workflow.create(descriptor)
        assert isinstance(workflow, Workflow)

        # run workflow
        df = workflow.run()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 5
        assert len(df.columns) == 7

    def test_workflow_with_process_params(self, data_path):
        descriptor = [
            {
                'run': 'mks.ftir@v1',
                'params': {
                    'source': str(data_path / 'mks_ftir/2024-01-16-conc.prn'),
                },
            },
            {
                'run': 'dataframe.select.columns@v1',
                'params': {
                    'select': {
                        "timestamp": "timestamp",
                        "NH3 (3000) 191C (2of2)": "NH3",
                        "N2O (100,200,300) 191C (1of2)": "N2O",
                        "NO (350,3000) 191C": "NO",
                        "NO2 (2000) 191C (2of2)": "NO2",
                        "NO2 (150) 191C (1of2)": "NO2b",
                        "H2O% (25) 191C": "H2O",
                    }
                },
            },
            {
                'run': 'dataframe.setindex@v1',
                'params': {
                    'index_var': 'timestamp',
                },
            },
            {
                'run': 'dataframe.join@v1',
                'params': {
                    'interpolate': True,
                    'how': 'left',
                    '$right': [
                        {
                            'run': 'channel.tclogger@v1',
                            'params': {
                                'source': str(
                                    data_path / 'ChannelV2TCLog/2024-01-16T10-05-21.csv'
                                ),
                            },
                        },
                        {
                            'run': 'dataframe.select.columns@v1',
                            'params': {
                                'select': {
                                    "timestamp": "timestamp",
                                    "sample-downstream": "temperature2",
                                }
                            },
                        },
                        {
                            'run': 'dataframe.setindex@v1',
                            'params': {
                                'index_var': 'timestamp',
                            },
                        },
                    ],
                },
            },
        ]

        # create workflow
        workflow = Workflow.create(descriptor)
        assert isinstance(workflow, Workflow)

        # run workflow
        df = workflow.run()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 16
        assert len(df.columns) == 7
