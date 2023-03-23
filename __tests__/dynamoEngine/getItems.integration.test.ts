import { v4 as getUuid } from 'uuid';

import env from './env';
import DynamoEngine from '../../src/dynamoEngine';
import MissingTableError from '../../src/errors/missingTableError';
import { addItem, createTable, deleteAllTables } from './utils';

const ENVIRONMENT = 'integrationTest-getItems';

jest.setTimeout(10000);

afterAll(async () => {
  await deleteAllTables({
    prefix: ENVIRONMENT,
    region: env.storageRegion,
    storageId: env.storageId,
    storageSecret: env.storageSecret,
  });
});

test('gets items', async () => {
  const tableName = `${ENVIRONMENT}-${getUuid()}`;
  await createTable({
    hashKey: 'testHashKey',
    region: env.storageRegion,
    sortKey: 'testSortKey',
    storageId: env.storageId,
    storageSecret: env.storageSecret,
    tableName,
  });
  await addItem({
    item: {
      testHashKey: 'test-hash-key',
      testSortKey: 'test-sort-key-1',
      otherProperty: 'test-value-1',
    },
    region: env.storageRegion,
    storageId: env.storageId,
    storageSecret: env.storageSecret,
    tableName,
  });
  await addItem({
    item: {
      testHashKey: 'test-hash-key',
      testSortKey: 'test-sort-key-2',
      otherProperty: 'test-value-2',
    },
    region: env.storageRegion,
    storageId: env.storageId,
    storageSecret: env.storageSecret,
    tableName,
  });
  const engine = new DynamoEngine({
    id: env.storageId,
    region: env.storageRegion,
    secret: env.storageSecret,
  });
  const response = await engine.getItems({
    hashKeyName: 'testHashKey',
    hashKeyValue: 'test-hash-key',
    tableName,
  });
  expect(response).toMatchObject([
    {
      testHashKey: 'test-hash-key',
      testSortKey: 'test-sort-key-1',
      otherProperty: 'test-value-1',
    },
    {
      testHashKey: 'test-hash-key',
      testSortKey: 'test-sort-key-2',
      otherProperty: 'test-value-2',
    },
  ]);
});

test('returns empty list when no items exist', async () => {
  const tableName = `${ENVIRONMENT}-${getUuid()}`;
  await createTable({
    hashKey: 'testHashKey',
    region: env.storageRegion,
    sortKey: 'testSortKey',
    storageId: env.storageId,
    storageSecret: env.storageSecret,
    tableName,
  });
  const engine = new DynamoEngine({
    id: env.storageId,
    region: env.storageRegion,
    secret: env.storageSecret,
  });
  const response = await engine.getItems({
    hashKeyName: 'testHashKey',
    hashKeyValue: 'test-hash-key',
    tableName,
  });
  expect(response).toHaveLength(0);
});

test('returns single item when table has no sort key', async () => {
  const tableName = `${ENVIRONMENT}-${getUuid()}`;
  await createTable({
    hashKey: 'testHashKey',
    region: env.storageRegion,
    storageId: env.storageId,
    storageSecret: env.storageSecret,
    tableName,
  });
  await addItem({
    item: {
      testHashKey: 'test-hash-key',
      otherProperty: 'test-value',
    },
    region: env.storageRegion,
    storageId: env.storageId,
    storageSecret: env.storageSecret,
    tableName,
  });
  const engine = new DynamoEngine({
    id: env.storageId,
    region: env.storageRegion,
    secret: env.storageSecret,
  });
  const response = await engine.getItems({
    hashKeyName: 'testHashKey',
    hashKeyValue: 'test-hash-key',
    tableName,
  });
  expect(response).toMatchObject([
    {
      testHashKey: 'test-hash-key',
      otherProperty: 'test-value',
    },
  ]);
});

test('throws error if table does not exist', async () => {
  const tableName = `${ENVIRONMENT}-${getUuid()}`;
  const engine = new DynamoEngine({
    id: env.storageId,
    region: env.storageRegion,
    secret: env.storageSecret,
  });
  await expect(
    engine.getItems({
      hashKeyName: 'testHashKey',
      hashKeyValue: 'test-hash-key',
      tableName,
    })
  ).rejects.toThrow(MissingTableError);
});
