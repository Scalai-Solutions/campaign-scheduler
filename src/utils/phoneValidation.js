const { parsePhoneNumberFromString } = require('libphonenumber-js');

/**
 * Normalize a phone number to E.164 when valid.
 * Returns null for invalid or unparsable numbers.
 *
 * @param {string} phone - Raw phone string (e.g. "918120653", "+34662319254", "+34 662 319 254")
 * @param {string} [defaultRegion] - ISO 3166-1 alpha-2 region code used as fallback when no
 *   country prefix is present (e.g. "ES" so that "918120653" resolves to "+34918120653").
 *   Has no effect when the number already includes a + country code.
 */
function normalizeE164Phone(phone, defaultRegion) {
    if (typeof phone !== 'string') return null;

    const candidate = phone.trim();
    if (!candidate) return null;

    const parsed = parsePhoneNumberFromString(candidate, defaultRegion || undefined);
    if (!parsed || !parsed.isValid()) return null;

    return parsed.number;
}

function isValidE164Phone(phone) {
    return !!normalizeE164Phone(phone);
}

module.exports = {
    normalizeE164Phone,
    isValidE164Phone
};
