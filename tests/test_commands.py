from uuid import uuid4
from dataclasses import asdict
import os

import pytest
from dotenv import load_dotenv

from reservoirdb.session import ReservoirSession
from reservoirdb.commands import CreateSchema, CreateTable, GetTable, TableRef, Table, Column

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

@pytest.mark.asyncio
async def test_create_table(session: ReservoirSession) -> None:
	schema = 'schema_' + str(uuid4()).replace('-', '')
	table = TableRef(schema, 'my_table')
	table_structure = Table([
		Column('test', 'Int64', True),
	])

	await session.txn([
		CreateSchema(schema),
		CreateTable(table, table_structure),
	])

	response = await session.txn([
		GetTable(table),
	])

	assert response.results[0]['data']['columns'] == asdict(table_structure)['columns']

	df = await session.query_pandas(f'select count(*) as n from {schema}__my_table')
	assert df['n'].values[0] == 0
