interface Engine {
  addItem: (input: AddItemInput) => Promise<Item>;

  createTable: (input: CreateTableInput) => Promise<void>;

  getItems: (input: GetItemsInput) => Promise<Item[]>;
}

export interface AddItemInput {
  item: Item;
  tableName: string;
}

export type Item = Record<string, unknown>;

export interface CreateTableInput {
  hashKey: string;
  sortKey?: string;
  tableName: string;
}

export interface GetItemsInput {
  hashKeyName: string;
  hashKeyValue: boolean | number | string;
  tableName: string;
}

export default Engine;
