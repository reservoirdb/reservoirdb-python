from dataclasses import dataclass
from typing import List
from enum import Enum

@dataclass
class TableRef:
	schema: str
	name: str

class ColumnType(str, Enum):
	INT64 = 'Int64'

@dataclass
class Column:
	name: str
	ty: ColumnType
	nullable: bool

@dataclass
class Table:
	columns: List[Column]
