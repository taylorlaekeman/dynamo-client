import {
  CreateTableCommand,
  DescribeTableCommand,
  DynamoDBClient,
} from '@aws-sdk/client-dynamodb';
import {
  DynamoDBDocumentClient,
  PutCommand,
  QueryCommand,
} from '@aws-sdk/lib-dynamodb';

import Engine, {
  AddItemInput,
  CreateTableInput,
  GetItemsInput,
  Item,
} from './engine';
import ExistingTableError from './errors/existingTableError';
import MissingKeyError from './errors/missingKeyError';
import MissingTableError from './errors/missingTableError';
import wait from './utils/wait';

class DynamoEngine implements Engine {
  readonly client: DynamoDBClient;

  readonly documentClient: DynamoDBDocumentClient;

  constructor({
    id,
    region = 'us-east-2',
    secret,
  }: {
    id: string;
    region?: string;
    secret: string;
  }) {
    this.client = new DynamoDBClient({
      credentials: {
        accessKeyId: id,
        secretAccessKey: secret,
      },
      region,
    });
    this.documentClient = DynamoDBDocumentClient.from(this.client);
  }

  addItem = async ({ item, tableName }: AddItemInput) => {
    const command = new PutCommand({
      Item: item,
      TableName: tableName,
    });
    try {
      await this.client.send(command);
    } catch (e) {
      const error = e as { name?: string; message?: string };
      if (error.name === 'ResourceNotFoundException')
        throw new MissingTableError({ tableName });
      if (
        error.name === 'ValidationException' &&
        error.message?.includes('Missing the key')
      ) {
        throw new MissingKeyError();
      }
      throw error;
    }
    return item;
  };

  createTable = async ({ hashKey, sortKey, tableName }: CreateTableInput) => {
    const attributes = [{ AttributeName: hashKey, AttributeType: 'S' }];
    const keys = [{ AttributeName: hashKey, KeyType: 'HASH' }];
    if (sortKey) {
      attributes.push({ AttributeName: sortKey, AttributeType: 'S' });
      keys.push({ AttributeName: sortKey, KeyType: 'RANGE' });
    }
    const createTableCommand = new CreateTableCommand({
      AttributeDefinitions: attributes,
      BillingMode: 'PAY_PER_REQUEST',
      KeySchema: keys,
      TableName: tableName,
    });
    try {
      await this.client.send(createTableCommand);
    } catch (e) {
      const error = e as { name?: string };
      if (error.name === 'ResourceInUseException')
        throw new ExistingTableError({ tableName });
      throw error;
    }
    await this.waitForTable({ tableName });
  };

  private describeTable = async ({
    tableName,
  }: {
    tableName: string;
  }): Promise<{ hashKey?: string; sortKey?: string; status?: string }> => {
    const command = new DescribeTableCommand({
      TableName: tableName,
    });
    try {
      const { Table: response } = await this.client.send(command);
      if (!response) throw new MissingTableError({ tableName });
      const { KeySchema: keys, TableStatus: status } = response;
      const hashKeyAttribute = keys?.find(({ KeyType }) => KeyType === 'HASH');
      if (!hashKeyAttribute) throw new MissingKeyError();
      const { AttributeName: hashKey } = hashKeyAttribute;
      const { AttributeName: sortKey } =
        keys?.find(({ KeyType }) => KeyType === 'RANGE') || {};
      return { hashKey, sortKey, status };
    } catch (e) {
      const error = e as { name?: string };
      if (error.name === 'ResourceNotFoundException')
        throw new MissingTableError({ tableName });
      throw error;
    }
  };

  getItems = async ({
    hashKeyName,
    hashKeyValue,
    tableName,
  }: GetItemsInput): Promise<Item[]> => {
    const expression = `${hashKeyName} = :hashKeyValue`;
    const command = new QueryCommand({
      ExpressionAttributeValues: {
        ':hashKeyValue': hashKeyValue,
      },
      KeyConditionExpression: expression,
      TableName: tableName,
    });
    try {
      const response = await this.documentClient.send(command);
      const { Items: items } = response;
      return items || [];
    } catch (e) {
      const error = e as { name?: string };
      if (error.name === 'ResourceNotFoundException')
        throw new MissingTableError({ tableName });
      throw error;
    }
  };

  private waitForTable = async ({ tableName }: { tableName: string }) => {
    let hasFinishedCreating = false;
    do {
      const { status } = await this.describeTable({ tableName }); // eslint-disable-line no-await-in-loop
      hasFinishedCreating = status !== 'CREATING';
      await wait({ seconds: 0.1 }); // eslint-disable-line no-await-in-loop
    } while (!hasFinishedCreating);
  };
}

export default DynamoEngine;
