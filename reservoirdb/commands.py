from dataclasses import dataclass, field
from typing import List
from enum import Enum

@dataclass
class Command:
	type: str = field(init = False)

	def __post_init__(self) -> None:
		self.type = type(self).__name__

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

@dataclass
class GetTable(Command):
	table: TableRef

@dataclass
class CreateTable(Command):
	table: TableRef
	table_def: Table

@dataclass
class CreateSchema(Command):
	name: str

@dataclass
class InsertData(Command):
	table: TableRef
	data_ref: str
