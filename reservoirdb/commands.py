from dataclasses import dataclass, field
from typing import List, Literal

@dataclass
class Command:
	type: str = field(init = False)

	def __post_init__(self) -> None:
		self.type = type(self).__name__

@dataclass
class TableRef:
	schema: str
	name: str

ColumnType = Literal['Int64']

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
