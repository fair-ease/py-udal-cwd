from pathlib import Path
import copernicusmarine as cm
import numpy as np
import os
import pandas
import tempfile
import typing
from typing import Any, Literal

import udal.specification as udal

from .copernicus_functions_poly import getIndexFilesInfo, poligonOverlap, rangeBounds, timeOverlap



#
# Description of the queries
#


QueryName = Literal[
    'urn:fairease.eu:udal:cwd:NAME'
]
"""Type to help development restricting query names to existing ones."""


QUERY_NAMES: typing.Tuple[QueryName, ...] = typing.get_args(QueryName)
"""List of the supported query names."""


QUERY_REGISTRY : dict[QueryName, udal.NamedQueryInfo] = {
    'urn:fairease.eu:udal:cwd:NAME': udal.NamedQueryInfo(
            'urn:fairease.eu:udal:cwd:NAME',
            {},
        ),
}
"""Catalogue of query names supported by this implementation."""



#
# UDAL objects
#


class Result(udal.Result):
    """Result from executing an UDAL query."""

    Type = pandas.DataFrame

    def __init__(self, query: udal.NamedQueryInfo, data: Any, metadata: dict = {}):
        self._query = query
        self._data = data
        self._metadata = metadata

    @property
    def query(self):
        """Information about the query that generated the data in this
        result."""
        return self._query

    @property
    def metadata(self):
        """Metadata associated with the result data."""
        return self._metadata

    def data(self, type: type[Type] | None = None) -> Type:
        """The data of the result."""
        if type is None or type is pandas.DataFrame:
            return self._data
        raise Exception(f'type "{type}" not supported')


class UDAL(udal.UDAL):
    """Uniform Data Access Layer"""

    def __init__(self, config: udal.Config = udal.Config()):
        self._config = config

    @staticmethod
    def __query_NAME(cache_dir: str | Path, params: dict) -> tuple:

        if params.get('latest') is None or params.get('latest'):
            period = 'latest'
        else:
            period = 'history'
        poly = params.get('polygon') or None
        time = params.get('time_range') or None
        data_type = params.get('data_type') or 'MO'
        file_type = params.get('file_type') or 'TS'

        dataset = 'cmems_obs-ins_glo_phybgcwav_mynrt_na_irr'

        dataset_type = 'OBSERVATION'

        dataset_details = {
            'product': 'INSITU_GLO_PHYBGCWAV_DISCRETE_MYNRT_013_030',
            'name': 'cmems_obs-ins_med_phybgcwav_mynrt_na_irr',
            'index_files': [f'index_{period}.txt'],
            'index_platform': 'index_platform.txt',
        }

        directory = os.path.join(Path(cache_dir), 'input', dataset_type, period)

        cm.get(
            dataset_id=dataset,
            output_directory=directory,
            no_directories=True,
            index_parts=True,
            overwrite=True,
            disable_progress_bar=True,
        )

        info = getIndexFilesInfo(dataset_details, directory, poly)

        # bounding box / polygon subsetting
        if poly is not None:
            info['poligonOverlap'] = info.apply(poligonOverlap, polygon=poly, axis=1)
            info = info[info['poligonOverlap'] == True]
        # time subsetting
        if time is not None:
            start, end = rangeBounds(time)
            info['timeOverlap'] = info.apply(timeOverlap, start=start, end=end, axis=1)
            info = info[info['timeOverlap'] == True]
        # data type subsetting
        if data_type is not None:
            info = info[info['data_type'] == data_type]
        # file type subsetting
        if file_type is not None:
            info = info[info['file_type'] == file_type]

        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete_on_close=False) as file:
            for i in np.arange(0, np.size(info.file_name)):
                filename = os.path.join(
                    period,
                    os.path.split(info.file_name.iloc[i])[0].split('/')[-1::][0],
                    os.path.split(info.file_name.iloc[i])[1],
                )
                file.write(filename + '\n')
            file.close()
            cm.get(
                dataset_id=dataset,
                output_directory=os.path.join(directory, data_type),
                no_directories=True,
                overwrite=True,
                disable_progress_bar=True,
                file_list=file.name,
            )

        dataframes = []
        metadata = { 'files': [] }
        for platform, files in info.groupby(['platform_code', 'data_type']):
            i = len(files) - 1
            dataframes.append(files)
            metadata['files'].append(files.file_name)
        data = pandas.DataFrame(dataframes)

        return data, metadata

    def execute(self, name: str, params: dict | None = None) -> Result:
        if name == 'urn:fairease.eu:udal:cwd:NAME':
            data, metadata = UDAL.__query_NAME(self._config.cache_dir or 'py_udal_cwd_cache', params or {})
            return Result(QUERY_REGISTRY[name], data, metadata)
        else:
            raise Exception(f'query {name} not supported')

    @property
    def queryNames(self) -> list[str]:
        return list(QUERY_NAMES)

    @property
    def queries(self) -> dict[str, udal.NamedQueryInfo]:
        return { k: v for k, v in QUERY_REGISTRY.items() }
