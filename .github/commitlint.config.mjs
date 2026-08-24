export default {
  extends: ['@commitlint/config-conventional'],
  /*
   * Any rules defined here will override rules from @commitlint/config-conventional
   */
  rules: {
    'body-max-line-length': [2, 'always', 200],
  },
  // ponytail: dependabot writes its own body with unwrappable summary/compare URLs
  ignores: [(message) => message.includes('Signed-off-by: dependabot[bot]')],
};
