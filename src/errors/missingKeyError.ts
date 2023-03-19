import StorageError from './storageError';

class MissingKeyError extends StorageError {
  constructor() {
    super('The item is missing a key attribute.');
  }
}

export default MissingKeyError;
