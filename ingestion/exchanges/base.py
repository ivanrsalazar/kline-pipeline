# ingestion/exchanges/base.py

from abc import ABC, abstractmethod

class ExchangeClient(ABC):

    @abstractmethod
    async def connect(self):
        ...

    @abstractmethod
    async def subscribe(self):
        ...

    @abstractmethod
    async def messages(self):
        ...
