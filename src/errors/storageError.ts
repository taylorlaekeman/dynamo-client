class StorageError extends Error {
  constructor(message?: string) {
    super(message || 'A storage error occured.');
  }
}

export default StorageError;
