import MissingTableError from '../../src/errors/missingTableError';
import MemoryEngine from '../../src/memoryEngine';

test('gets items', async () => {
  const engine = new MemoryEngine({
    items: {
      'test-table': {
        'test-hash-key+test-sort-key-1': {
          testHashKey: 'test-hash-key',
          testSortKey: 'test-sort-key-1',
          nestedObject: {
            testValue: 'test',
          },
          otherProperty: 'test-value-1',
        },
        'test-hash-key+test-sort-key-2': {
          testHashKey: 'test-hash-key',
          testSortKey: 'test-sort-key-2',
          otherProperty: 'test-value-2',
        },
      },
    },
    tables: {
      'test-table': { hashKey: 'testHashKey', sortKey: 'testSortKey' },
    },
  });
  const result = await engine.getItems({
    hashKeyName: 'testHashKey',
    hashKeyValue: 'test-hash-key',
    tableName: 'test-table',
  });
  expect(result).toMatchObject([
    {
      testHashKey: 'test-hash-key',
      testSortKey: 'test-sort-key-1',
      nestedObject: {
        testValue: 'test',
      },
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
  const engine = new MemoryEngine({
    tables: {
      'test-table': { hashKey: 'testHashKey', sortKey: 'testSortKey' },
    },
  });
  const result = await engine.getItems({
    hashKeyName: 'testHashKey',
    hashKeyValue: 'test-hash-key',
    tableName: 'test-table',
  });
  expect(result).toHaveLength(0);
});

test('returns single items when table has no sort key', async () => {
  const engine = new MemoryEngine({
    items: {
      'test-table': {
        'test-hash-key': {
          testHashKey: 'test-hash-key',
          otherProperty: 'test-value',
        },
      },
    },
    tables: {
      'test-table': { hashKey: 'testHashKey' },
    },
  });
  const result = await engine.getItems({
    hashKeyName: 'testHashKey',
    hashKeyValue: 'test-hash-key',
    tableName: 'test-table',
  });
  expect(result).toMatchObject([
    {
      testHashKey: 'test-hash-key',
      otherProperty: 'test-value',
    },
  ]);
});

test('throws error if table does not exist', async () => {
  const engine = new MemoryEngine();
  await expect(
    engine.getItems({
      hashKeyName: 'testHashKey',
      hashKeyValue: 'test-hash-key',
      tableName: 'test-table',
    })
  ).rejects.toThrow(MissingTableError);
});
