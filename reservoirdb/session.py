from typing import TypeVar, Type, Optional, Dict, Any, Sequence, Callable, List, Awaitable
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

import reservoirdb_protocol
from reservoirdb_protocol import *

_RequestType = TypeVar('_RequestType')
_ResponseType = TypeVar('_ResponseType')

class ReservoirException(Exception):
	pass

class UnauthenticatedException(ReservoirException):
	pass

_dacite_config = Config(cast = [Enum], strict_unions_match = True)

class ReservoirSession:
	def __init__(
		self,
		region: str,
		provider: str,
		token_type: str,
		default_compute_cluster: Optional[ComputeClusterRef] = None,
		default_schema: Optional[SchemaRef] = None,
		token: Optional[str] = None,
	) -> None:
		self._base_url = f'https://{region}.{provider}.reservoirdb.com'
		self._token = token
		self._token_type = token_type
		self._default_compute_cluster = default_compute_cluster
		self._catalog_context = CatalogContext(default_catalog = None, default_schema = default_schema)

	@classmethod
	async def connect(
		cls,
		*,
		region: str,
		provider: str,
		account: str,
		user: str,
		password: str,
		default_compute_cluster: Optional[ComputeClusterRef] = None,
		default_schema: Optional[SchemaRef] = None,
	) -> 'ReservoirSession':
		session = cls(
			region,
			provider,
			'Bearer',
			default_compute_cluster = default_compute_cluster,
			default_schema = default_schema,
		)

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
		response_parser: Optional[Callable[[aiohttp.ClientResponse], Awaitable[_ResponseType]]] = None,
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

			if response_parser:
				return await response_parser(res)
			else:
				json_data = await res.json()
				return from_dict(response_type, json_data, _dacite_config)

	async def txn(
		self,
		commands: Sequence[Command],
		arrow_data: Dict[str, ArrowTable] = {},
		run_on: Optional[ComputeClusterRef] = None,
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
			TxnRequest(list(commands), run_on or self._default_compute_cluster),
			TxnResponse,
			multipart_data = multipart_data,
		)

		return res.results

	@staticmethod
	async def _query_response_parser(res: aiohttp.ClientResponse) -> ArrowTable:
		reader = ipc.open_stream(await res.read())
		return reader.read_all()

	async def query(
		self,
		query: str,
		run_on: Optional[ComputeClusterRef] = None,
		catalog_context: Optional[CatalogContext] = None,
	) -> ArrowTable:
		return await self._request(
			'POST',
			'/db/query',
			QueryRequest(query, run_on or self._default_compute_cluster, catalog_context or self._catalog_context),
			ArrowTable,
			response_parser = self._query_response_parser,
		)

	async def query_pandas(
		self,
		query: str,
		run_on: Optional[ComputeClusterRef] = None,
	) -> DataFrame:
		table = await self.query(query, run_on)
		return table.to_pandas()
