import { promises as fs } from 'fs';

export const createDirectoryIfMissing = async ({
  directory,
}: {
  directory: string;
}) => {
  try {
    await fs.mkdir(directory);
  } catch (e) {
    const error = e as { message?: string };
    if (!error.message?.startsWith('EEXIST: file already exists')) throw error;
  }
};

export const readFile = async ({ name }: { name: string }) => {
  const contents = await fs.readFile(name, 'utf-8');
  return JSON.parse(contents);
};

export const removeAll = async ({
  location = '.',
  prefix,
}: {
  location?: string;
  prefix: string;
}) => {
  const rootDirectoryName = `${location}/.filedb`;
  const dbDirectoryNames = await fs.readdir(rootDirectoryName);
  const matchingDbDirectoryNames = dbDirectoryNames.filter((directoryName) =>
    directoryName.startsWith(prefix)
  );
  const promises = matchingDbDirectoryNames.map((dbDirectoryName) =>
    fs.rm(`${rootDirectoryName}/${dbDirectoryName}`, { recursive: true })
  );
  await Promise.all(promises);
};

export const writeFile = async ({
  contents,
  name,
}: {
  contents: Record<string, any>; // eslint-disable-line @typescript-eslint/no-explicit-any
  name: string;
}) => {
  await fs.writeFile(name, JSON.stringify(contents));
};
