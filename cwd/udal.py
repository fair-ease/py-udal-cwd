import pandas
import typing
from typing import Any, Literal

import udal.specification as udal



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

    def __query_NAME(self, name: str, params: dict) -> Result:
        ... # TODO

    def execute(self, name: str, params: dict|None = None) -> Result:
        if name == 'urn:fairease.eu:udal:cwd:NAME':
            return self.__query_NAME(name, params or {})
        else:
            raise Exception(f'query {name} not supported')

    @property
    def queryNames(self) -> list[str]:
        return list(QUERY_NAMES)

    @property
    def queries(self) -> dict[str, udal.NamedQueryInfo]:
        return { k: v for k, v in QUERY_REGISTRY.items() }
