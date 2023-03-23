import {
  CreateTableCommand,
  DeleteTableCommand,
  DescribeTableCommand,
  DynamoDBClient,
  ListTablesCommand,
} from '@aws-sdk/client-dynamodb';
import {
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
} from '@aws-sdk/lib-dynamodb';

const getClient = ({
  region = 'us-east-2',
  storageId,
  storageSecret,
}: {
  region?: string;
  storageId: string;
  storageSecret: string;
}) => {
  const client = new DynamoDBClient({
    credentials: {
      accessKeyId: storageId,
      secretAccessKey: storageSecret,
    },
    region,
  });
  const documentClient = DynamoDBDocumentClient.from(client);
  return {
    client,
    documentClient,
  };
};

export const addItem = async ({
  item,
  region,
  storageId,
  storageSecret,
  tableName,
}: {
  item: Record<string, boolean | number | string>;
  region?: string;
  storageId: string;
  storageSecret: string;
  tableName: string;
}) => {
  const { documentClient } = getClient({
    region,
    storageId,
    storageSecret,
  });
  const command = new PutCommand({
    Item: item,
    TableName: tableName,
  });
  await documentClient.send(command);
};

export const createTable = async ({
  hashKey,
  region = 'us-east-2',
  sortKey,
  storageId,
  storageSecret,
  tableName,
}: {
  hashKey: string;
  region?: string;
  sortKey?: string;
  storageId: string;
  storageSecret: string;
  tableName: string;
}) => {
  const { client } = getClient({
    region,
    storageId,
    storageSecret,
  });
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
  await client.send(createTableCommand);
  let hasFinishedCreating = false;
  do {
    /* eslint-disable no-await-in-loop */
    const { Table: { TableStatus: status } = {} } = await describeTable({
      region,
      storageId,
      storageSecret,
      tableName,
    });
    /* eslint-enable no-await-in-loop */
    hasFinishedCreating = status !== 'CREATING';
  } while (!hasFinishedCreating);
};

export const deleteAllTables = async ({
  prefix,
  region,
  storageId,
  storageSecret,
}: {
  prefix: string;
  region?: string;
  storageId: string;
  storageSecret: string;
}) => {
  const { TableNames: tableNames } = await listTables({
    prefix,
    region,
    storageId,
    storageSecret,
  });
  if (!tableNames) return;
  const promises = tableNames.map((tableName) =>
    deleteTable({
      region,
      storageId,
      storageSecret,
      tableName,
    })
  );
  await Promise.all(promises);
};

export const deleteTable = async ({
  region,
  storageId,
  storageSecret,
  tableName,
}: {
  region?: string;
  storageId: string;
  storageSecret: string;
  tableName: string;
}) => {
  const { client } = getClient({ region, storageId, storageSecret });
  const command = new DeleteTableCommand({
    TableName: tableName,
  });
  return client.send(command);
};

export const describeTable = async ({
  region = 'us-east-2',
  storageId,
  storageSecret,
  tableName,
}: {
  region?: string;
  storageId: string;
  storageSecret: string;
  tableName: string;
}) => {
  const { client } = getClient({ region, storageId, storageSecret });
  const command = new DescribeTableCommand({
    TableName: tableName,
  });
  return client.send(command);
};

export const getItem = async ({
  hashKeyName,
  hashKeyValue,
  region = 'us-east-2',
  sortKeyName,
  sortKeyValue,
  storageId,
  storageSecret,
  tableName,
}: {
  hashKeyName: string;
  hashKeyValue: string;
  region?: string;
  sortKeyName?: string;
  sortKeyValue?: string;
  storageId: string;
  storageSecret: string;
  tableName: string;
}) => {
  const { documentClient } = getClient({
    region,
    storageId,
    storageSecret,
  });
  const key = {
    [hashKeyName]: hashKeyValue,
  };
  if (sortKeyName && sortKeyValue) key[sortKeyName] = sortKeyValue;
  const command = new GetCommand({
    Key: key,
    TableName: tableName,
  });
  return documentClient.send(command);
};

export const listTables = async ({
  prefix = '',
  region = 'us-east-2',
  storageId,
  storageSecret,
}: {
  prefix?: string;
  region?: string;
  storageId: string;
  storageSecret: string;
}) => {
  const { client } = getClient({ region, storageId, storageSecret });
  const command = new ListTablesCommand({
    ExclusiveStartTableName: prefix,
  });
  const response = await client.send(command);
  response.TableNames = response.TableNames?.filter((name) =>
    name.startsWith(prefix)
  );
  return response;
};

export default {};
