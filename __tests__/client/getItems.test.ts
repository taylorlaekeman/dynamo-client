import StorageClient from '../../src/client';
import MissingTableError from '../../src/errors/missingTableError';
import MemoryEngine from '../../src/memoryEngine';

test('gets items', async () => {
  const engine = new MemoryEngine({
    items: {
      'test-getItems-test-table': {
        'test-hash-key+test-sort-key-1': {
          testHashKey: 'test-hash-key',
          testSortKey: 'test-sort-key-1',
          testProperty: 'test-value-1',
        },
        'test-hash-key+test-sort-key-2': {
          testHashKey: 'test-hash-key',
          testSortKey: 'test-sort-key-2',
          testProperty: 'test-value-2',
        },
      },
    },
    tables: {
      'test-getItems-test-table': {
        hashKey: 'testHashKey',
        sortKey: 'testSortKey',
      },
    },
  });
  const client = new StorageClient({
    engine,
    environment: 'test-getItems',
    getId: () => 'test-id',
    getNow: () => '2020-01-01T00:00:00',
  });
  const items = await client.getItems({
    hashKeyName: 'testHashKey',
    hashKeyValue: 'test-hash-key',
    tableName: 'test-table',
  });
  expect(items).toMatchObject([
    {
      testHashKey: 'test-hash-key',
      testSortKey: 'test-sort-key-1',
      testProperty: 'test-value-1',
    },
    {
      testHashKey: 'test-hash-key',
      testSortKey: 'test-sort-key-2',
      testProperty: 'test-value-2',
    },
  ]);
});

test('gets item without sort key', async () => {
  const engine = new MemoryEngine({
    items: {
      'test-getItems-test-table': {
        'test-hash-key': {
          testHashKey: 'test-hash-key',
          testProperty: 'test-value',
        },
      },
    },
    tables: {
      'test-getItems-test-table': { hashKey: 'testHashKey' },
    },
  });
  const client = new StorageClient({
    engine,
    environment: 'test-getItems',
    getId: () => 'test-id',
    getNow: () => '2020-01-01T00:00:00',
  });
  const items = await client.getItems({
    hashKeyName: 'testHashKey',
    hashKeyValue: 'test-hash-key',
    tableName: 'test-table',
  });
  expect(items).toMatchObject([
    {
      testHashKey: 'test-hash-key',
      testProperty: 'test-value',
    },
  ]);
});

test('returns empty list when no items exist', async () => {
  const engine = new MemoryEngine({
    items: {},
    tables: {
      'test-getItems-test-table': { hashKey: 'testHashKey' },
    },
  });
  const client = new StorageClient({
    engine,
    environment: 'test-getItems',
    getId: () => 'test-id',
    getNow: () => '2020-01-01T00:00:00',
  });
  const items = await client.getItems({
    hashKeyName: 'testHashKey',
    hashKeyValue: 'test-hash-key',
    tableName: 'test-table',
  });
  expect(items).toMatchObject([]);
});

test('throws error when table does not exist', async () => {
  const engine = new MemoryEngine();
  const client = new StorageClient({
    engine,
    environment: 'test-getItems',
    getId: () => 'test-id',
    getNow: () => '2020-01-01T00:00:00',
  });
  expect(
    client.getItems({
      hashKeyName: 'testHashKey',
      hashKeyValue: 'test-hash-key',
      tableName: 'test-table',
    })
  ).rejects.toThrow(MissingTableError);
});
