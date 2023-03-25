import client from './client';

export { default as DynamoEngine } from './dynamoEngine';
export { default as ExistingTableError } from './errors/existingTableError';
export { default as MissingKeyError } from './errors/missingKeyError';
export { default as MissingLocationError } from './errors/missingLocationError';
export { default as MissingTableError } from './errors/missingTableError';
export { default as StorageError } from './errors/storageError';
export type { default as StorageEngine } from './engine';
export { default as FileEngine } from './fileEngine';
export { default as MemoryEngine } from './memoryEngine';

export default client;
