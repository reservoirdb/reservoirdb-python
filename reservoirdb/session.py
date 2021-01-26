from typing import TypeVar, Type, Literal, Optional, Protocol, Dict, Any, Sequence
from dataclasses import dataclass, asdict
import json

import aiohttp
from dacite.core import from_dict
from pyarrow import Table, ipc
from pandas import DataFrame

from .protocol import *
from .commands import Command

class Request(Protocol):
	endpoint: str
	method: str

_RequestType = TypeVar('_RequestType', bound = Request)
_ResponseType = TypeVar('_ResponseType')

class ReservoirException(Exception):
	pass

class UnauthenticatedException(ReservoirException):
	pass

_TokenType = Literal['Bearer', 'AdminToken']

class ReservoirSession:
	def __init__(self, region: str, provider: str, token_type: _TokenType, token: Optional[str] = None) -> None:
		self._base_url = f'https://{region}.{provider}.reservoirdb.com'
		self._token = token
		self._token_type = token_type

	@classmethod
	async def connect(cls, *, region: str, provider: str, account: str, user: str, password: str) -> 'ReservoirSession':
		session = cls(region, provider, 'Bearer')
		auth_res = await session._request(AuthRequest(account, user, password), AuthResponse, requires_auth = False)
		session._token = auth_res.token
		return session

	async def _request(
		self,
		request: _RequestType,
		response_type: Type[_ResponseType],
		multipart_data: Optional[aiohttp.FormData] = None,
		requires_auth: bool = True,
		override_base_url: Optional[str] = None,
	) -> _ResponseType:
		headers = {}
		if requires_auth:
			if not self._token:
				raise UnauthenticatedException()
			headers['Authorization'] = f'{self._token_type} {self._token}'

		request_args: Dict[str, Any] = {}
		if multipart_data:
			multipart_data.add_field('request', json.dumps(asdict(request)), content_type = 'application/json')
			request_args['data'] = multipart_data
		else:
			request_args['json'] = asdict(request)

		async with aiohttp.request(
			request.method,
			(override_base_url or self._base_url) + request.endpoint,
			headers = headers,
			**request_args,
		) as res:
			if res.status != 200:
				raise ReservoirException(f'{request.endpoint} failed, status {res.status}: {await res.text()}')

			if response_type == Table:
				reader = ipc.open_stream(await res.read())
				return Table.from_batches([b for b in reader], reader.schema) # type: ignore
			else:
				return from_dict(response_type, await res.json())

	async def txn(self, commands: Sequence[Command]) -> TxnResponse:
		return await self._request(
			TxnRequest(list(commands)),
			TxnResponse,
			multipart_data = aiohttp.FormData(),
		)

	async def query(self, query: str) -> Table:
		return await self._request(
			QueryRequest(query),
			Table,
		)

	async def query_pandas(self, query: str) -> DataFrame:
		table = await self._request(
			QueryRequest(query),
			Table,
		)
		return table.to_pandas()
