const hubspotAudienceResolver = require('./hubspotAudienceResolver');
const customEndpointAudienceResolver = require('./customEndpointAudienceResolver');

class AudienceResolver {
    static async resolveAudience(subaccountId, pipelineConfig = {}, options = {}) {
        const provider = String(pipelineConfig.provider || '').toLowerCase();

        if (provider === 'hubspot') {
            return hubspotAudienceResolver.resolveAudience(subaccountId, pipelineConfig, options);
        }

        if (provider === 'custom_endpoint') {
            return customEndpointAudienceResolver.resolveAudience(subaccountId, pipelineConfig, options);
        }

        throw new Error(`Unsupported audience provider: ${provider || 'missing'}`);
    }
}

module.exports = AudienceResolver;