from uuid import uuid4
from dataclasses import asdict
import os

import pytest
from dotenv import load_dotenv
from pyarrow import Table as ArrowTable

from reservoirdb.session import ReservoirSession
from reservoirdb.commands import CreateSchema, CreateTable, GetTable, TableRef, Table, Column, ColumnType, InsertData

load_dotenv()

@pytest.fixture
async def session() -> ReservoirSession:
	return await ReservoirSession.connect(
		provider = os.environ['RESERVOIR_PROVIDER'],
		region = os.environ['RESERVOIR_REGION'],
		account = os.environ['RESERVOIR_ACCOUNT'],
		user = os.environ['RESERVOIR_USER'],
		password = os.environ['RESERVOIR_PASSWORD'],
	)

@pytest.fixture
async def random_schema(session: ReservoirSession) -> str:
	name = 'schema_' + str(uuid4()).replace('-', '')

	await session.txn([
		CreateSchema(name),
	])

	return name

@pytest.mark.asyncio
async def test_create_table(session: ReservoirSession, random_schema: str) -> None:
	table = TableRef(random_schema, 'my_table')
	table_structure = Table([
		Column('test', ColumnType.INT64, True),
	])

	await session.txn([
		CreateTable(table, table_structure),
	])

	response = await session.txn([
		GetTable(table),
	])

	assert response.results[0]['data']['columns'] == asdict(table_structure)['columns']

@pytest.mark.asyncio
async def test_insert_data(session: ReservoirSession, random_schema: str) -> None:
	table = TableRef(random_schema, 'my_table')
	table_structure = Table([
		Column('test', ColumnType.INT64, True),
	])

	await session.txn([
		CreateTable(table, table_structure),
	])

	df = await session.query_pandas(f'select count(*) as n from {random_schema}__my_table')
	assert df['n'].values[0] == 0

	await session.txn(
		[
			InsertData(table, 'attachment_name'),
		],
		arrow_data = {
			'attachment_name': ArrowTable.from_pydict({
				'test': [1, 2, 3],
			}),
		},
	)

	df = await session.query_pandas(f'select count(*) as n, sum(test) as sum from {random_schema}__my_table')
	assert df['n'].values[0] == 3
	assert df['sum'].values[0] == 6
