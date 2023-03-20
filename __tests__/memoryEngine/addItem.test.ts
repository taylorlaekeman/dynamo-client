import MissingKeyError from '../../src/errors/missingKeyError';
import MissingTableError from '../../src/errors/missingTableError';
import MemoryEngine from '../../src/memoryEngine';

test('adds item', async () => {
  const engine = new MemoryEngine({
    tables: {
      'test-table': { hashKey: 'testHashKey', sortKey: 'testSortKey' },
    },
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
    tableName: 'test-table',
  });
  expect(returnedItem).toMatchObject(item);
  expect(
    engine.items['test-table']['test-hash-key+test-sort-key']
  ).toMatchObject(item);
});

test('adds item without sort key', async () => {
  const engine = new MemoryEngine({
    tables: {
      'test-table': { hashKey: 'testHashKey' },
    },
  });
  const returnedItem = await engine.addItem({
    item: {
      otherProperty: 'test-value',
      testHashKey: 'test-hash-key',
    },
    tableName: 'test-table',
  });
  expect(returnedItem).toMatchObject({
    otherProperty: 'test-value',
    testHashKey: 'test-hash-key',
  });
  expect(engine.items['test-table']['test-hash-key']).toMatchObject({
    otherProperty: 'test-value',
    testHashKey: 'test-hash-key',
  });
});

test('throws error if table does not exist', async () => {
  const engine = new MemoryEngine();
  await expect(
    engine.addItem({
      item: {
        otherProperty: 'test-value',
        testHashKey: 'test-hash-key',
      },
      tableName: 'test-table',
    })
  ).rejects.toThrow(MissingTableError);
});

test('throws error if item is missing hash key', async () => {
  const engine = new MemoryEngine({
    tables: { 'test-table': { hashKey: 'testHashKey' } },
  });
  await expect(
    engine.addItem({
      item: {
        otherProperty: 'test-value',
      },
      tableName: 'test-table',
    })
  ).rejects.toThrow(MissingKeyError);
});

test('throws error if item is missing sort key', async () => {
  const engine = new MemoryEngine({
    tables: {
      'test-table': { hashKey: 'testHashKey', sortKey: 'testSortKey' },
    },
  });
  await expect(
    engine.addItem({
      item: {
        otherProperty: 'test-value',
        testHashKey: 'test-hash-key',
      },
      tableName: 'test-table',
    })
  ).rejects.toThrow(MissingKeyError);
});

test('overwrites item with matching key', async () => {
  const engine = new MemoryEngine({
    items: {
      'test-table': {
        'test-hash-key': {
          testHashKey: 'test-hash-key',
          otherProperty: 'test-value',
        },
      },
    },
    tables: { 'test-table': { hashKey: 'testHashKey' } },
  });
  await engine.addItem({
    item: {
      testHashKey: 'test-hash-key',
      otherProperty: 'test-other-value',
    },
    tableName: 'test-table',
  });
  expect(engine.items['test-table']['test-hash-key']).toMatchObject({
    testHashKey: 'test-hash-key',
    otherProperty: 'test-other-value',
  });
});
