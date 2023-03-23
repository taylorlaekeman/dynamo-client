import { v4 as uuid } from 'uuid';

import env from './env';
import DynamoEngine from '../../src/dynamoEngine';
import ExistingTableError from '../../src/errors/existingTableError';
import { createTable, deleteAllTables, describeTable } from './utils';

const ENVIRONMENT = 'integrationTest-createTable';

jest.setTimeout(10000);

afterAll(async () => {
  await deleteAllTables({
    prefix: ENVIRONMENT,
    region: env.storageRegion,
    storageId: env.storageId,
    storageSecret: env.storageSecret,
  });
});

test('creates table', async () => {
  const tableName = `${ENVIRONMENT}-${uuid()}`;
  const engine = new DynamoEngine({
    id: env.storageId,
    region: env.storageRegion,
    secret: env.storageSecret,
  });
  await engine.createTable({
    hashKey: 'testHashKey',
    sortKey: 'testSortKey',
    tableName,
  });
  const table = await describeTable({
    region: env.storageRegion,
    storageId: env.storageId,
    storageSecret: env.storageSecret,
    tableName,
  });
  expect(table).toMatchObject({
    Table: {
      AttributeDefinitions: [
        { AttributeName: 'testHashKey', AttributeType: 'S' },
        { AttributeName: 'testSortKey', AttributeType: 'S' },
      ],
      BillingModeSummary: { BillingMode: 'PAY_PER_REQUEST' },
      KeySchema: [
        { AttributeName: 'testHashKey', KeyType: 'HASH' },
        { AttributeName: 'testSortKey', KeyType: 'RANGE' },
      ],
    },
  });
});

test('creates table without sort key', async () => {
  const tableName = `${ENVIRONMENT}-${uuid()}`;
  const engine = new DynamoEngine({
    id: env.storageId,
    region: env.storageRegion,
    secret: env.storageSecret,
  });
  await engine.createTable({
    hashKey: 'testHashKey',
    tableName,
  });
  const table = await describeTable({
    region: env.storageRegion,
    storageId: env.storageId,
    storageSecret: env.storageSecret,
    tableName,
  });
  expect(table).toMatchObject({
    Table: {
      AttributeDefinitions: [
        { AttributeName: 'testHashKey', AttributeType: 'S' },
      ],
      BillingModeSummary: { BillingMode: 'PAY_PER_REQUEST' },
      KeySchema: [{ AttributeName: 'testHashKey', KeyType: 'HASH' }],
    },
  });
});

test('waits for table', async () => {
  const tableName = `${ENVIRONMENT}-${uuid()}`;
  const engine = new DynamoEngine({
    id: env.storageId,
    region: env.storageRegion,
    secret: env.storageSecret,
  });
  await engine.createTable({
    hashKey: 'testHashKey',
    tableName,
  });
  const { Table: { TableStatus: status } = {} } = await describeTable({
    region: env.storageRegion,
    storageId: env.storageId,
    storageSecret: env.storageSecret,
    tableName,
  });
  expect(status).toBe('ACTIVE');
});

test('throws error if table exists', async () => {
  const tableName = `${ENVIRONMENT}-${uuid()}`;
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
  await expect(async () => {
    await engine.createTable({
      hashKey: 'differentHashKey',
      sortKey: 'differentSortKey',
      tableName,
    });
  }).rejects.toThrow(ExistingTableError);
});
