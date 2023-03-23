import ExistingTableError from '../../src/errors/existingTableError';
import MemoryEngine from '../../src/memoryEngine';

test('creates table', async () => {
  const engine = new MemoryEngine();
  await engine.createTable({
    hashKey: 'testHashKey',
    sortKey: 'testSortKey',
    tableName: 'test-table',
  });
  expect(engine.tables['test-table']).toMatchObject({
    hashKey: 'testHashKey',
    sortKey: 'testSortKey',
  });
});

test('creates table without sort key', async () => {
  const engine = new MemoryEngine();
  await engine.createTable({
    hashKey: 'testHashKey',
    tableName: 'test-table',
  });
  expect(engine.tables['test-table']).toMatchObject({
    hashKey: 'testHashKey',
  });
});

test('throws error if table exists', async () => {
  const engine = new MemoryEngine({
    tables: { 'test-table': { hashKey: 'testHashKey' } },
  });
  await expect(
    engine.createTable({
      hashKey: 'testHashKey',
      tableName: 'test-table',
    })
  ).rejects.toThrow(ExistingTableError);
});
