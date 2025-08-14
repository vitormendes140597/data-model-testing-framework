from abc import ABC, abstractmethod

class BackendAdapter(ABC):

    @abstractmethod
    def read_csv(self, **kwargs):
        pass

    @abstractmethod
    def read_json(self, **kwargs):
        pass

    @abstractmethod
    def execute_sql(self, **kwargs):
        pass

    @classmethod
    @abstractmethod
    def to_pandas(self, **kwargs):
        pass
