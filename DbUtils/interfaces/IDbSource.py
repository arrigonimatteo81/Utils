from abc import ABC, abstractmethod


class IDbSource(ABC):

    @abstractmethod
    def returnCount(self, query_count) -> int:
        pass

    @abstractmethod
    def returnQueryContent(self, query_content):
        pass

    @abstractmethod
    def closeConnection(self):
        pass
