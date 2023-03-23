import { promises as fs } from 'fs';
import { v4 as getUuid } from 'uuid';

import FileStorageEngine from '../../src/fileEngine';
import MissingLocationError from '../../src/errors/missingLocationError';
import MissingTableError from '../../src/errors/missingTableError';
import { createDirectoryIfMissing, removeAll, writeFile } from './utils';

const PREFIX = 'integrationTest-getItems';

afterAll(async () => {
  await fs.rm('./test-filedb-root-getItems', { recursive: true });
  await removeAll({ prefix: PREFIX });
});

beforeAll(async () => {
  try {
    await fs.rm('./test-filedb-root-getItems', { recursive: true });
  } catch {
    /* an error will be thrown if the directory isn't found, do nothing */
  }
});

test('throws error if location does not exist', async () => {
  const dbName = `${PREFIX}-${getUuid()}`;
  const engine = new FileStorageEngine({
    dbName,
    location: './bad/bad/bad',
  });
  await expect(
    engine.getItems({
      hashKeyName: 'testHashKey',
      hashKeyValue: 'test-hash-key',
      tableName: 'test-table',
    })
  ).rejects.toThrow(MissingLocationError);
});

test('throws error if filedb folder does not exist', async () => {
  const dbName = `${PREFIX}-${getUuid()}`;
  await fs.mkdir('./test-filedb-root-getItems');
  const engine = new FileStorageEngine({
    dbName,
    location: './test-filedb-root-getItems',
  });
  await expect(
    engine.getItems({
      hashKeyName: 'testHashKey',
      hashKeyValue: 'test-hash-key',
      tableName: 'test-table',
    })
  ).rejects.toThrow(MissingLocationError);
});

test('throws error if db folder does not exist', async () => {
  const dbName = `${PREFIX}-${getUuid()}`;
  await createDirectoryIfMissing({ directory: '.filedb' });
  const engine = new FileStorageEngine({
    dbName,
    location: '.',
  });
  await expect(
    engine.getItems({
      hashKeyName: 'testHashKey',
      hashKeyValue: 'test-hash-key',
      tableName: 'test-table',
    })
  ).rejects.toThrow(MissingLocationError);
});

test('throws error if tables file does not exist', async () => {
  const dbName = `${PREFIX}-${getUuid()}`;
  await createDirectoryIfMissing({ directory: '.filedb' });
  await fs.mkdir(`./.filedb/${dbName}`);
  const engine = new FileStorageEngine({
    dbName,
    location: '.',
  });
  await expect(
    engine.getItems({
      hashKeyName: 'testHashKey',
      hashKeyValue: 'test-hash-key',
      tableName: 'test-table',
    })
  ).rejects.toThrow(MissingTableError);
});

test('throws error if table is not in tables file', async () => {
  const dbName = `${PREFIX}-${getUuid()}`;
  await createDirectoryIfMissing({ directory: '.filedb' });
  await fs.mkdir(`./.filedb/${dbName}`);
  await writeFile({
    contents: {
      'other-table': { hashKey: 'testHashKey', sortKey: 'testSortKey' },
    },
    name: `./.filedb/${dbName}/tables.json`,
  });
  const engine = new FileStorageEngine({
    dbName,
    location: '.',
  });
  await expect(
    engine.getItems({
      hashKeyName: 'testHashKey',
      hashKeyValue: 'test-hash-key',
      tableName: 'test-table',
    })
  ).rejects.toThrow(MissingTableError);
});

test('gets items', async () => {
  const dbName = `${PREFIX}-${getUuid()}`;
  await fs.mkdir(`./.filedb/${dbName}`);
  await writeFile({
    contents: {
      'test-table': { hashKey: 'testHashKey', sortKey: 'testSortKey' },
    },
    name: `./.filedb/${dbName}/tables.json`,
  });
  await writeFile({
    contents: {
      'test-hash-key+test-sort-key-1': {
        testHashKey: 'test-hash-key',
        testSortKey: 'test-sort-key-1',
        otherProperty: 'test-value-1',
      },
      'test-hash-key+test-sort-key-2': {
        testHashKey: 'test-hash-key',
        testSortKey: 'test-sort-key-2',
        otherProperty: 'test-value-2',
      },
    },
    name: `./.filedb/${dbName}/test-table.json`,
  });
  const engine = new FileStorageEngine({
    dbName,
    location: '.',
  });
  const response = await engine.getItems({
    hashKeyName: 'testHashKey',
    hashKeyValue: 'test-hash-key',
    tableName: 'test-table',
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

test('returns empty list when items file does not exist', async () => {
  const dbName = `${PREFIX}-${getUuid()}`;
  await fs.mkdir(`./.filedb/${dbName}`);
  await writeFile({
    contents: {
      'test-table': {
        hashKey: 'testHashKey',
        sortKey: 'testSortKey',
      },
    },
    name: `./.filedb/${dbName}/tables.json`,
  });
  const engine = new FileStorageEngine({
    dbName,
    location: '.',
  });
  const response = await engine.getItems({
    hashKeyName: 'testHashKey',
    hashKeyValue: 'test-hash-key',
    tableName: 'test-table',
  });
  expect(response).toHaveLength(0);
});

test('returns empty list when items file is empty', async () => {
  const dbName = `${PREFIX}-${getUuid()}`;
  await fs.mkdir(`./.filedb/${dbName}`);
  await writeFile({
    contents: {
      'test-table': {
        hashKey: 'testHashKey',
        sortKey: 'testSortKey',
      },
    },
    name: `./.filedb/${dbName}/tables.json`,
  });
  await writeFile({
    contents: {},
    name: `./.filedb/${dbName}/test-table.json`,
  });
  const engine = new FileStorageEngine({
    dbName,
    location: '.',
  });
  const response = await engine.getItems({
    hashKeyName: 'testHashKey',
    hashKeyValue: 'test-hash-key',
    tableName: 'test-table',
  });
  expect(response).toHaveLength(0);
});

test('returns single item when table has no sort key', async () => {
  const dbName = `${PREFIX}-${getUuid()}`;
  await fs.mkdir(`./.filedb/${dbName}`);
  await writeFile({
    contents: {
      'test-table': { hashKey: 'testHashKey', sortKey: 'testSortKey' },
    },
    name: `./.filedb/${dbName}/tables.json`,
  });
  await writeFile({
    contents: {
      'test-hash-key': {
        testHashKey: 'test-hash-key',
        testSortKey: 'test-sort-key',
        otherProperty: 'test-value',
      },
    },
    name: `./.filedb/${dbName}/test-table.json`,
  });
  const engine = new FileStorageEngine({
    dbName,
    location: '.',
  });
  const response = await engine.getItems({
    hashKeyName: 'testHashKey',
    hashKeyValue: 'test-hash-key',
    tableName: 'test-table',
  });
  expect(response).toMatchObject([
    {
      testHashKey: 'test-hash-key',
      testSortKey: 'test-sort-key',
      otherProperty: 'test-value',
    },
  ]);
});
