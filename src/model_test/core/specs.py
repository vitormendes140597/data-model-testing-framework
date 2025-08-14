from typing import Dict, Tuple
from model_test.core.files import LocalFile, SQLFile
from model_test.adapters import BackendAdapter

from abc import ABC, abstractmethod
import pandas as pd
from pandas.testing import assert_frame_equal

class Loader:

    def __init__(self, adapter: BackendAdapter) -> None:
        self.adapter = adapter

    def _read(self, file: LocalFile, **kwargs):

        if file.format == "csv":
            return self.adapter.read_csv(str(file.path), **kwargs)
        elif file.format == "json":
            return self.adapter.read_json(str(file.path), **kwargs)

        raise ValueError(f"Formato não suportado: {file.format}")

    def load(self, file: LocalFile, **kwargs):
        return self._read(file, **kwargs)

class TestModel(ABC):

    def __init__(
        self,
        inputs: Dict[str, LocalFile],
        expected: LocalFile,
        backend: BackendAdapter,
        run_params: Dict[str,str],
        load_params: Dict[str,str],
    ) -> None:
        self.inputs = inputs
        self.expected = expected
        self.backend = backend
        self.loader = Loader(self.backend)
        self.run_params = run_params
        self.load_params = load_params

    @abstractmethod
    def run(self, **kwargs):
        pass

class TestModelSQL(TestModel):

    def __init__(
        self,
        inputs: Dict[str, LocalFile],
        expected: LocalFile,
        backend: BackendAdapter,
        run_params: Dict[str,str],
        load_params:  Dict[str,str],
        sql: SQLFile
    ) -> None:
        super().__init__(inputs, expected, backend, run_params, load_params)
        self.sql = sql

    def run(self) -> Tuple[pd.DataFrame, pd.DataFrame]:

        _ = {k: self.loader.load(v, **{**self.load_params, **{"temp_view":k}}) for k,v in self.inputs.items()}
        expected_model = self.loader.load(self.expected, **self.load_params)

        transformation = self.sql.render(self.run_params)
        result = self.backend.execute_sql(transformation)

        return self.backend.to_pandas(expected_model) , self.backend.to_pandas(result)

class TestExecution:

    @classmethod
    def test_model(cls, test: TestModel):
        expected_df , result_df = test.run()

        assert_frame_equal(expected_df, result_df)
