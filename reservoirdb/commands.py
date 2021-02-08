from dataclasses import dataclass, field

from .state import *

@dataclass
class Command:
	type: str = field(init = False)

	def __post_init__(self) -> None:
		self.type = type(self).__name__

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
