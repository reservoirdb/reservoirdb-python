from uuid import uuid4
from dataclasses import asdict
import os

import pytest
from dotenv import load_dotenv
from pyarrow import Table as ArrowTable

from reservoirdb.session import ReservoirSession, ReservoirException
from reservoirdb_protocol import *

load_dotenv()

async def new_session(
	provider: str = os.environ['RESERVOIR_PROVIDER'],
	region: str = os.environ['RESERVOIR_REGION'],
	account: str = os.environ['RESERVOIR_ACCOUNT'],
	user: str = os.environ['RESERVOIR_USER'],
	password: str = os.environ['RESERVOIR_PASSWORD'],
) -> ReservoirSession:
	return await ReservoirSession.connect(
		provider = provider,
		region = region,
		account = account,
		user = user,
		password = password,
	)

@pytest.fixture
async def session() -> ReservoirSession:
	return await new_session()

@pytest.fixture
async def random_schema(session: ReservoirSession) -> SchemaRef:
	name = SchemaRef('schema_' + str(uuid4()).replace('-', ''))

	await session.txn([
		CreateSchema(name),
	])

	return name

@pytest.fixture
async def random_user(session: ReservoirSession) -> UserRef:
	name = UserRef('testuser_' + str(uuid4()).replace('-', ''))

	await session.txn([
		CreateUser(name, 'password'),
	])

	return name

@pytest.fixture
async def random_role(session: ReservoirSession) -> RoleRef:
	name = RoleRef('testrole_' + str(uuid4()).replace('-', ''))

	await session.txn([
		CreateRole(name),
	])

	return name

@pytest.mark.asyncio
async def test_create_table(session: ReservoirSession, random_schema: SchemaRef) -> None:
	table = TableRef(random_schema, 'my_table')
	table_structure = Table([
		Column('test', ColumnType.INT64, True),
	], None)

	await session.txn([
		CreateTable(table, table_structure),
	])

	results = await session.txn([
		GetTable(table),
	])

	assert results[0] == table_structure

@pytest.mark.asyncio
async def test_insert_data(session: ReservoirSession, random_schema: SchemaRef) -> None:
	table = TableRef(random_schema, 'my_table')
	table_structure = Table([
		Column('test', ColumnType.INT64, True),
	], None)

	await session.txn([
		CreateTable(table, table_structure),
	])

	df = await session.query_pandas(f'select count(*) as n from {random_schema}.my_table')
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

	df = await session.query_pandas(f'select count(*) as n, sum(test) as sum from {random_schema}.my_table')
	assert df['n'].values[0] == 3
	assert df['sum'].values[0] == 6

@pytest.mark.asyncio
async def test_user_role_setup(
	session: ReservoirSession,
	random_schema: SchemaRef,
	random_user: UserRef,
	random_role: RoleRef,
) -> None:
	table = TableRef(random_schema, 'limited_access')
	table_structure = Table([
		Column('test', ColumnType.INT64, True),
	], None)

	await session.txn([
		CreateTable(table, table_structure),
	])

	user_session = await new_session(user = random_user, password = 'password')

	with pytest.raises(ReservoirException):
		await user_session.query_pandas(f'select * from {random_schema}.limited_access')

	await session.txn([
		AssignUserRoles(random_user, [random_role]),
		GrantGlobalSchemaPermissions(random_role, SchemaPermissions.READ_TABLE)
	])

	await user_session.query_pandas(f'select * from {random_schema}.limited_access')
