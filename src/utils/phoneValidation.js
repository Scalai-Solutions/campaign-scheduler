const { parsePhoneNumberFromString } = require('libphonenumber-js');

/**
 * Normalize a phone number to E.164 when valid.
 * Returns null for invalid or unparsable numbers.
 */
function normalizeE164Phone(phone) {
    if (typeof phone !== 'string') return null;

    const candidate = phone.trim();
    if (!candidate) return null;

    const parsed = parsePhoneNumberFromString(candidate);
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
