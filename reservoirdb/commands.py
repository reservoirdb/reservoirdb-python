from dataclasses import dataclass, field

from .state import *

@dataclass
class Command:
	pass

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
