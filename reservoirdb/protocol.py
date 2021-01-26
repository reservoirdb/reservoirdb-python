from dataclasses import dataclass, asdict
from typing import List, Any

from .commands import Command

@dataclass
class AuthRequest:
	endpoint = '/auth/login'
	method = 'POST'

	account: str
	username: str
	password: str

@dataclass
class AuthResponse:
	token: str

@dataclass
class TxnRequest:
	endpoint = '/db/txn'
	method = 'POST'

	commands: list[Command]

@dataclass
class TxnResponse:
	results: List[Any]

@dataclass
class QueryRequest:
	endpoint = '/db/query'
	method = 'POST'

	query: str
