module.exports = {
    env: {
        es2021: true,
        node: true,
    },
    extends: [
        'airbnb-base',
        'eslint:recommended',
        'plugin:@typescript-eslint/recommended',
        'plugin:@typescript-eslint/recommended-requiring-type-checking',
        'prettier',
    ],
    overrides: [],
    parser: '@typescript-eslint/parser',
    parserOptions: {
        ecmaVersion: 'latest',
        project: 'tsconfig.json',
        sourceType: 'module',
    },
    plugins: ['@typescript-eslint'],
    rules: {
        'import/extensions': ['error', { ts: 'never' }],
        'no-use-before-define': 'off',
    },
    settings: {
        'import/resolver': {
            node: { extensions: ['.js', '.ts'] },
        },
    },
};
