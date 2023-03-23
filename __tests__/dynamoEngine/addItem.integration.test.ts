import { v4 as uuid } from 'uuid';

import env from './env';
import DynamoEngine from '../../src/dynamoEngine';
import MissingKeyError from '../../src/errors/missingKeyError';
import MissingTableError from '../../src/errors/missingTableError';
import { addItem, createTable, deleteAllTables, getItem } from './utils';

const ENVIRONMENT = 'integrationTest-addItem';

jest.setTimeout(10000);

afterAll(async () => {
  await deleteAllTables({
    prefix: ENVIRONMENT,
    region: env.storageRegion,
    storageId: env.storageId,
    storageSecret: env.storageSecret,
  });
});

test('adds item', async () => {
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
  const item = {
    booleanProperty: true,
    numericProperty: 9,
    stringProperty: 'test-value',
    testHashKey: 'test-hash-key',
    testSortKey: 'test-sort-key',
  };
  const returnedItem = await engine.addItem({
    item,
    tableName,
  });
  const response = await getItem({
    hashKeyName: 'testHashKey',
    hashKeyValue: 'test-hash-key',
    region: env.storageRegion,
    sortKeyName: 'testSortKey',
    sortKeyValue: 'test-sort-key',
    storageId: env.storageId,
    storageSecret: env.storageSecret,
    tableName,
  });
  const { Item: savedItem } = response;
  expect(returnedItem).toMatchObject(item);
  expect(savedItem).toMatchObject(item);
});

test('adds item without sort key', async () => {
  const tableName = `${ENVIRONMENT}-${uuid()}`;
  await createTable({
    hashKey: 'testHashKey',
    region: env.storageRegion,
    storageId: env.storageId,
    storageSecret: env.storageSecret,
    tableName,
  });
  const item = {
    otherProperty: 'test-value',
    testHashKey: 'test-hash-key',
  };
  const engine = new DynamoEngine({
    id: env.storageId,
    region: env.storageRegion,
    secret: env.storageSecret,
  });
  const returnedItem = await engine.addItem({
    item,
    tableName,
  });
  const response = await getItem({
    hashKeyName: 'testHashKey',
    hashKeyValue: 'test-hash-key',
    region: env.storageRegion,
    storageId: env.storageId,
    storageSecret: env.storageSecret,
    tableName,
  });
  const { Item: savedItem } = response;
  expect(returnedItem).toMatchObject(item);
  expect(savedItem).toMatchObject(item);
});

test('throws error if table does not exist', async () => {
  const tableName = `${ENVIRONMENT}-${uuid()}`;
  const engine = new DynamoEngine({
    id: env.storageId,
    region: env.storageRegion,
    secret: env.storageSecret,
  });
  await expect(
    engine.addItem({
      item: {
        otherProperty: 'test-value',
        testHashKey: 'test-hash-key',
      },
      tableName,
    })
  ).rejects.toThrow(MissingTableError);
});

test('throws error if item is missing hash key', async () => {
  const tableName = `${ENVIRONMENT}-${uuid()}`;
  await createTable({
    hashKey: 'testHashKey',
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
  await expect(
    engine.addItem({
      item: {
        otherProperty: 'test-value',
      },
      tableName,
    })
  ).rejects.toThrow(MissingKeyError);
});

test('throws error if item is missing sort key', async () => {
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
  await expect(
    engine.addItem({
      item: {
        testHashKey: 'test-hash-key',
        otherProperty: 'test-value',
      },
      tableName,
    })
  ).rejects.toThrow(MissingKeyError);
});

test('overwrites item with matching key', async () => {
  const tableName = `${ENVIRONMENT}-${uuid()}`;
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
  await engine.addItem({
    item: {
      testHashKey: 'test-hash-key',
      otherProperty: 'test-other-value',
    },
    tableName,
  });
  const { Item: item } = await getItem({
    hashKeyName: 'testHashKey',
    hashKeyValue: 'test-hash-key',
    region: env.storageRegion,
    storageId: env.storageId,
    storageSecret: env.storageSecret,
    tableName,
  });
  expect(item).toMatchObject({
    testHashKey: 'test-hash-key',
    otherProperty: 'test-other-value',
  });
});
