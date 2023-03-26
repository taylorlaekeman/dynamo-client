import { DateTime } from 'luxon';
import { v4 as getUuid } from 'uuid';

import StorageEngine, { Item } from './engine';
import MissingKeyError from './errors/missingKeyError';
import MissingTableError from './errors/missingTableError';

class StorageClient {
  readonly engine: StorageEngine;

  readonly environment?: string;

  readonly getId: () => string;

  readonly getNow: () => string;

  constructor({
    engine,
    environment,
    getId = getUuid,
    getNow = () => DateTime.now().toUTC().toISO(),
  }: {
    engine: StorageEngine;
    environment?: string;
    getId?: () => string;
    getNow?: () => string;
  }) {
    this.engine = engine;
    this.environment = environment;
    this.getId = getId;
    this.getNow = getNow;
  }

  addItem = async ({
    item: itemWithoutMetadata,
    table: { hashKeys, name, sortKeys },
  }: AddItemInput): Promise<Item> => {
    const tableName = getTableName({
      environment: this.environment,
      tableName: name,
    });
    const hashKey = createCompositeValue({ values: hashKeys }) as string;
    const sortKey = createCompositeValue({ values: sortKeys }) as string;
    const item = formatItem({
      getId: this.getId,
      getNow: this.getNow,
      hashKeys,
      item: itemWithoutMetadata,
      sortKeys,
    });
    try {
      await this.engine.addItem({ item, tableName });
      return item;
    } catch (error) {
      if (error instanceof MissingTableError) {
        await this.engine.createTable({
          hashKey,
          tableName,
          sortKey,
        });
        await this.engine.addItem({ item, tableName });
        return item;
      }
      throw error;
    }
  };

  getItems = async ({
    hashKeyName,
    hashKeyValue,
    tableName: name,
  }: GetItemsInput): Promise<Item[]> => {
    const tableName = getTableName({
      environment: this.environment,
      tableName: name,
    });
    return this.engine.getItems({ hashKeyName, hashKeyValue, tableName });
  };
}

export interface AddItemInput {
  item: Item;
  table: {
    hashKeys: string[];
    name: string;
    sortKeys?: string[];
  };
}

export interface GetItemsInput {
  hashKeyName: string;
  hashKeyValue: string;
  tableName: string;
}

const getTableName = ({
  environment,
  tableName,
}: {
  environment?: string;
  tableName: string;
}): string => {
  if (environment) return `${environment}-${tableName}`;
  return tableName;
};

const formatItem = ({
  getId,
  getNow,
  hashKeys,
  item,
  sortKeys,
}: {
  getId: () => string;
  getNow: () => string;
  hashKeys: string[];
  item: Item;
  sortKeys?: string[];
}): Item => {
  const result = {
    ...item,
  } as Item;
  if (!('createdDate' in result)) result.createdDate = getNow();
  if (!('id' in result)) result.id = getId();
  const { key: hashKey, value: hashKeyValue } = formatKey({
    item: result,
    keys: hashKeys,
  });
  result[hashKey] = hashKeyValue;
  if (sortKeys) {
    const { key: sortKey, value: sortKeyValue } = formatKey({
      item: result,
      keys: sortKeys,
    });
    result[sortKey] = sortKeyValue;
  }
  return result;
};

const formatKey = ({
  item,
  keys,
}: {
  item: Item;
  keys: string[];
}): { key: string; value: unknown } => {
  const values = keys.map((key) => {
    if (!(key in item)) throw new MissingKeyError();
    return item[key];
  });
  return {
    key: createCompositeValue({ values: keys }) as string,
    value: createCompositeValue({ values }),
  };
};

const createCompositeValue = ({
  values,
}: {
  values?: unknown[];
}): unknown | undefined => {
  if (!values) return undefined;
  if (values.length === 1) return values[0];
  return values.join('|');
};

export default StorageClient;
