const getEnv = (): Env => {
  const env = {
    storageId: process.env.STORAGE_ID,
    storageRegion: process.env.STORAGE_REGION,
    storageSecret: process.env.STORAGE_SECRET,
  };
  const missingVariables: string[] = [];
  if (!env.storageId) missingVariables.push('STORAGE_ID');
  if (!env.storageRegion) missingVariables.push('STORAGE_REGION');
  if (!env.storageSecret) missingVariables.push('STORAGE_SECRET');
  if (missingVariables.length > 0) throw new EnvironmentError(missingVariables);
  return env as Env;
};

export interface Env {
  storageId: string;
  storageRegion: string;
  storageSecret: string;
}

class EnvironmentError extends Error {
  constructor(missingVariables: string[]) {
    super(pluralizeMessageIfNecessary(missingVariables));
  }
}

const pluralizeMessageIfNecessary = (missingVariables: string[]): string => {
  if (missingVariables.length === 0)
    return `Environment variable '${missingVariables[0]}' is not set`;
  const serializedList = serializeList(missingVariables);
  return `Environment variables ${serializedList} are not set`;
};

const serializeList = (list: string[]): string =>
  list.map((item) => `'${item}'`).join(', ');

export default getEnv();
