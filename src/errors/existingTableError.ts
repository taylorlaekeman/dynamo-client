import StorageError from './storageError';

class ExistingTableError extends StorageError {
  constructor({ tableName }: { tableName: string }) {
    super(
      `A table with the name '${tableName}' could not be created because table with the same name and different keys already exists.`
    );
  }
}

export default ExistingTableError;
