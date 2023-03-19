import StorageError from './storageError';

class MissingTableError extends StorageError {
  constructor({ tableName }: { tableName: string }) {
    super(`A table with the name '${tableName}' could not be found.`);
  }
}

export default MissingTableError;
