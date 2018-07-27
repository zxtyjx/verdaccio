// @flow
import assert from 'assert';
import { generateGravatarUrl, GRAVATAR_DEFAULT } from '../../../src/utils/user';
import { spliceURL } from '../../../src/utils/string';
import Package from '../../../src/webui/components/Package/index';
import {
  validateName as validate,
  convertDistRemoteToLocalTarballUrls,
  parseReadme,
  addGravatarSupport
} from '../../../src/lib/utils';
import Logger, { setup } from '../../../src/lib/logger';
import { readFile } from '../../functional/lib/test.utils';

const readmeFile = (fileName: string = 'markdown.md') =>
  readFile(`../../unit/partials/readme/${fileName}`);

setup([]);

describe('Utilities', () => {
  describe('String utilities', () => {
    test('should splice two strings and generate a url', () => {
      const url: string = spliceURL('http://domain.com', '/-/static/logo.png');

      expect(url).toMatch('http://domain.com/-/static/logo.png');
    });

    test('should splice a empty strings and generate a url', () => {
      const url: string = spliceURL('', '/-/static/logo.png');

      expect(url).toMatch('/-/static/logo.png');
    });
  });

  describe('User utilities', () => {
    test('should generate gravatar url with email', () => {
      const gravatarUrl: string = generateGravatarUrl('user@verdaccio.org');

      expect(gravatarUrl).toMatch('https://www.gravatar.com/avatar/');
      expect(gravatarUrl).not.toMatch('000000000');
    });

    test('should generate generic gravatar url', () => {
      const gravatarUrl: string = generateGravatarUrl();

      expect(gravatarUrl).toMatch(GRAVATAR_DEFAULT);
    });
  });

  describe('Validations', () => {
    test('good ones', () => {
      assert(validate('verdaccio'));
      assert(validate('some.weird.package-zzz'));
      assert(validate('old-package@0.1.2.tgz'));
    });

    test('uppercase', () => {
      assert(validate('EVE'));
      assert(validate('JSONStream'));
    });

    test('no package.json', () => {
      assert(!validate('package.json'));
    });

    test('no path seps', () => {
      assert(!validate('some/thing'));
      assert(!validate('some\\thing'));
    });

    test('no hidden', () => {
      assert(!validate('.bin'));
    });

    test('no reserved', () => {
      assert(!validate('favicon.ico'));
      assert(!validate('node_modules'));
      assert(!validate('__proto__'));
    });

    test('other', () => {
      assert(!validate('pk g'));
      assert(!validate('pk\tg'));
      assert(!validate('pk%20g'));
      assert(!validate('pk+g'));
      assert(!validate('pk:g'));
    });
  });

  describe('Packages utilities', () => {
    const metadata: Package = {
      name: 'npm_test',
      versions: {
        '1.0.0': {
          dist: {
            tarball: 'http://registry.org/npm_test/-/npm_test-1.0.0.tgz'
          }
        },
        '1.0.1': {
          dist: {
            tarball: 'http://registry.org/npm_test/-/npm_test-1.0.1.tgz'
          }
        }
      }
    };

    const buildURI = (host, version) =>
      `http://${host}/npm_test/-/npm_test-${version}.tgz`;
    const host = 'fake.com';

    test('convertDistRemoteToLocalTarballUrls', () => {
      const convertDist = convertDistRemoteToLocalTarballUrls(
        Object.assign({}, metadata),
        // $FlowFixMe
        {
          headers: {
            host
          },
          get: () => 'http',
          protocol: 'http'
        },
        ''
      );

      expect(convertDist.versions['1.0.0'].dist.tarball).toEqual(
        buildURI(host, '1.0.0')
      );
      expect(convertDist.versions['1.0.1'].dist.tarball).toEqual(
        buildURI(host, '1.0.1')
      );
    });
  });

  describe('parseReadme', () => {
    test('should pass for ascii/makrdown text to html template', () => {
      const markdown = '# markdown';
      const ascii = '= AsciiDoc';

      expect(parseReadme('testPackage', markdown)).toEqual(
        '<h1 id="markdown">markdown</h1>\n'
      );
      expect(parseReadme('testPackage', ascii)).toEqual('<h1>AsciiDoc</h1>\n');
      expect(
        parseReadme('testPackage', String(readmeFile('markdown.md')))
      ).toMatchSnapshot();
      expect(
        parseReadme('testPackage', String(readmeFile('ascii.adoc')))
      ).toMatchSnapshot();
    });

    test('should pass for conversion of non-ascii to markdown text', () => {
      const simpleText = 'simple text';
      const randomText = '%%%%%**##==';
      const randomTextNonAscii = 'simple text \n = ascii';
      const randomTextMarkdown = 'simple text \n # markdown';

      expect(parseReadme('testPackage', randomText)).toEqual(
        '<p>%%%%%**##==</p>\n'
      );
      expect(parseReadme('testPackage', simpleText)).toEqual(
        '<p>simple text</p>\n'
      );
      expect(parseReadme('testPackage', randomTextNonAscii)).toEqual(
        '<p>simple text \n = ascii</p>\n'
      );
      expect(parseReadme('testPackage', randomTextMarkdown)).toEqual(
        '<p>simple text </p>\n<h1 id="markdown">markdown</h1>\n'
      );
    });

    test('should show error for no readme data', () => {
      const noData = '';
      const spy = jest.spyOn(Logger.logger, 'error');
      expect(parseReadme('testPackage', noData)).toEqual(
        '<p>ERROR: No README data found!</p>\n'
      );
      expect(spy).toHaveBeenCalledWith(
        { packageName: 'testPackage' },
        '@{packageName}: No readme found'
      );
    });
  });

  describe('addGravatarSupport', () => {
    test('check for blank object', () => {
      expect(addGravatarSupport({})).toEqual({});
    });
    test('author field', () => {
      const pkgWithAuthorInfo = {
        latest: { author: { name: 'verdaccio', email: 'user@verdccio.org' } }
      };
      const pkgWithAuthor = {
        latest: { author: 'user@verdccio.org' }
      };
      const pkgWithAuthorResult = {
        latest: {
          author: {
            author: 'user@verdccio.org',
            avatar:
              'https://www.gravatar.com/avatar/00000000000000000000000000000000?d=mm',
            email: ''
          }
        }
      };
      const pkgWithNoAuthorInfo = {
        latest: {
          author: {}
        }
      };
      const pkgWithNoAuthor = {
        latest: {}
      };
      const result = {
        latest: {
          author: {
            avatar:
              'https://www.gravatar.com/avatar/794d7f6ef93d0689437de3c3e48fadc7',
            email: 'user@verdccio.org',
            name: 'verdaccio'
          }
        }
      };
      expect(addGravatarSupport(pkgWithAuthorInfo)).toEqual(result);
      expect(addGravatarSupport(pkgWithNoAuthorInfo)).toEqual(
        pkgWithNoAuthorInfo
      );
      expect(addGravatarSupport(pkgWithNoAuthor)).toEqual(pkgWithNoAuthor);
      expect(addGravatarSupport(pkgWithAuthor)).toEqual(pkgWithAuthorResult);
    });

    test('contributors field', () => {
      const pkgWithContributorsInfo = {
        latest: {
          contributors: [
            { name: 'user', email: 'user@verdccio.org' },
            { name: 'user1', email: 'user1@verdccio.org' }
          ]
        }
      };
      const pkgWithNoContributorsInfo = {
        latest: {
          contributors: []
        }
      };
      const pkgWithNoContributors = {
        latest: {}
      };
      const result = {
        latest: {
          contributors: [
            {
              avatar:
                'https://www.gravatar.com/avatar/794d7f6ef93d0689437de3c3e48fadc7',
              email: 'user@verdccio.org',
              name: 'user'
            },
            {
              avatar:
                'https://www.gravatar.com/avatar/51105a49ce4a9c2bfabf0f6a2cba3762',
              email: 'user1@verdccio.org',
              name: 'user1'
            }
          ]
        }
      };
      expect(addGravatarSupport(pkgWithContributorsInfo)).toEqual(result);
      expect(addGravatarSupport(pkgWithNoContributorsInfo)).toEqual(
        pkgWithNoContributorsInfo
      );
      expect(addGravatarSupport(pkgWithNoContributors)).toEqual(
        pkgWithNoContributors
      );
    });

    test('maintainers field', () => {
      const pkgWithMaintainersInfo = {
        latest: {
          maintainers: [
            { name: 'user', email: 'user@verdccio.org' },
            { name: 'user1', email: 'user1@verdccio.org' }
          ]
        }
      };
      const pkgWithNoMaintainersInfo = {
        latest: {
          maintainers: []
        }
      };
      const pkgWithNoMaintainers = {
        latest: {}
      };
      const result = {
        latest: {
          maintainers: [
            {
              avatar:
                'https://www.gravatar.com/avatar/794d7f6ef93d0689437de3c3e48fadc7',
              email: 'user@verdccio.org',
              name: 'user'
            },
            {
              avatar:
                'https://www.gravatar.com/avatar/51105a49ce4a9c2bfabf0f6a2cba3762',
              email: 'user1@verdccio.org',
              name: 'user1'
            }
          ]
        }
      };
      expect(addGravatarSupport(pkgWithMaintainersInfo)).toEqual(result);
      expect(addGravatarSupport(pkgWithNoMaintainersInfo)).toEqual(
        pkgWithNoMaintainersInfo
      );
      expect(addGravatarSupport(pkgWithNoMaintainers)).toEqual(
        pkgWithNoMaintainers
      );
    });
  });
});
