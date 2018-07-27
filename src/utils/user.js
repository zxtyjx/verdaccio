// @flow
import {stringToMD5} from '../lib/crypto-utils';

export const GRAVATAR_DEFAULT =
  'https://www.gravatar.com/avatar/00000000000000000000000000000000?d=mm';
/**
 * Generate gravatar url from email address
 */
export function generateGravatarUrl(email?: string): string {
  let emailCopy = email;
  if (typeof email === 'string' && email.length > 0) {
    emailCopy = email.trim().toLocaleLowerCase();
    const emailMD5 = stringToMD5(emailCopy);

    return `https://www.gravatar.com/avatar/${emailMD5}`;
  }
  return GRAVATAR_DEFAULT;
}
