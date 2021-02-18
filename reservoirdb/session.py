from typing import TypeVar, Type, Optional, Dict, Any, Sequence, Callable, List
from typing_extensions import Protocol
from dataclasses import dataclass, asdict
import json
from io import BytesIO
from enum import Enum

import aiohttp
from dacite.core import from_dict
from dacite.config import Config
from pyarrow import Table as ArrowTable, ipc, BufferOutputStream
from pandas import DataFrame

from reservoirdb_protocol import *
import reservoirdb_protocol.types
from reservoirdb_protocol.types import *

_RequestType = TypeVar('_RequestType')
_ResponseType = TypeVar('_ResponseType')

class ReservoirException(Exception):
	pass

class UnauthenticatedException(ReservoirException):
	pass

_dacite_config = Config(cast = [Enum])

class ReservoirSession:
	def __init__(
		self,
		region: str,
		provider: str,
		token_type: str,
		token: Optional[str] = None,
	) -> None:
		self._base_url = f'https://{region}.{provider}.reservoirdb.com'
		self._token = token
		self._token_type = token_type

	@classmethod
	async def connect(
		cls,
		*,
		region: str,
		provider: str,
		account: str,
		user: str,
		password: str,
	) -> 'ReservoirSession':
		session = cls(region, provider, 'Bearer')
		auth_res = await session._request(
			'POST',
			'/auth/login',
			AuthLoginRequest(account, UserRef(user), password),
			AuthLoginResponse,
			requires_auth = False,
		)
		session._token = auth_res.token
		return session

	async def _request(
		self,
		method: str,
		endpoint: str,
		request: _RequestType,
		response_type: Type[_ResponseType],
		multipart_data: Optional[aiohttp.FormData] = None,
		requires_auth: bool = True,
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
			method,
			self._base_url + endpoint,
			headers = headers,
			**request_args,
		) as res:
			if res.status != 200:
				raise ReservoirException(f'{endpoint} failed, status {res.status}: {await res.text()}')

			if response_type == ArrowTable:
				reader = ipc.open_stream(await res.read())
				return ArrowTable.from_batches([b for b in reader], reader.schema) # type: ignore
			else:
				json_data = await res.json()
				if response_type == dict:
					return json_data # type: ignore

				return from_dict(response_type, json_data, _dacite_config)

	async def txn(
		self,
		commands: Sequence[Command],
		arrow_data: Dict[str, ArrowTable] = {},
	) -> List[Optional[TxnResult]]:
		multipart_data = aiohttp.FormData()
		for name, table in arrow_data.items():
			sink = BufferOutputStream()
			stream_writer = ipc.RecordBatchStreamWriter(sink, table.schema)
			stream_writer.write_table(table)
			stream_writer.close()

			multipart_data.add_field(name, sink.getvalue().to_pybytes())

		res = await self._request(
			'POST',
			'/db/txn',
			TxnRequest([TaggedCommand(type(c).__name__, c) for c in commands]),
			dict,
			multipart_data = multipart_data,
		)

		return [
			from_dict(getattr(reservoirdb_protocol.types, r['type']), r['data'], _dacite_config) if r else None
			for r in res['results']
		]

	async def query(self, query: str) -> ArrowTable:
		return await self._request(
			'POST',
			'/db/query',
			QueryRequest(query),
			ArrowTable,
		)

	async def query_pandas(self, query: str) -> DataFrame:
		table = await self.query(query)
		return table.to_pandas()
