function makeTaskDedupeKey(runId, nodeId, dueAtIso) {
    return `task:${runId}:${nodeId}:${dueAtIso}`;
}

function makeStepDedupeKey(runId, nodeId, attempt) {
    return `step:${runId}:${nodeId}:${attempt}`;
}

function getNode(workflow, nodeId) {
    if (!workflow || !workflow.nodes) return undefined;
    return workflow.nodes.find((n) => n.id === nodeId);
}

function parseDelayToMs(delay) {
    if (delay == null) return 0;

    if (typeof delay === 'object') {
        const value = Number(delay.value);
        const unit = String(delay.unit || '').trim().toLowerCase();
        if (!Number.isFinite(value) || value < 0) return 0;

        if (['ms', 'millisecond', 'milliseconds'].includes(unit)) return value;
        if (['s', 'sec', 'secs', 'second', 'seconds'].includes(unit)) return value * 1000;
        if (['m', 'min', 'mins', 'minute', 'minutes'].includes(unit)) return value * 60 * 1000;
        if (['h', 'hr', 'hrs', 'hour', 'hours'].includes(unit)) return value * 60 * 60 * 1000;
        if (['d', 'day', 'days'].includes(unit)) return value * 24 * 60 * 60 * 1000;

        // Backward compatibility with workflow validator naming.
        if (unit === 'mins') return value * 60 * 1000;
        if (unit === 'hrs') return value * 60 * 60 * 1000;

        return 0;
    }

    if (typeof delay === 'number' && Number.isFinite(delay)) {
        // Backward compatibility: numeric delays are treated as seconds.
        return Math.max(0, delay * 1000);
    }

    if (typeof delay !== 'string') return 0;

    const raw = delay.trim().toLowerCase();
    if (!raw) return 0;

    if (/^\d+$/.test(raw)) {
        return parseInt(raw, 10) * 1000;
    }

    const compactMatch = raw.match(/^(\d+)\s*(ms|s|sec|secs|second|seconds|m|min|mins|minute|minutes|h|hr|hrs|hour|hours|d|day|days)$/);
    if (!compactMatch) return 0;

    const value = parseInt(compactMatch[1], 10);
    const unit = compactMatch[2];

    if (unit === 'ms') return value;
    if (['s', 'sec', 'secs', 'second', 'seconds'].includes(unit)) return value * 1000;
    if (['m', 'min', 'mins', 'minute', 'minutes'].includes(unit)) return value * 60 * 1000;
    if (['h', 'hr', 'hrs', 'hour', 'hours'].includes(unit)) return value * 60 * 60 * 1000;
    if (['d', 'day', 'days'].includes(unit)) return value * 24 * 60 * 60 * 1000;

    return 0;
}

function resolveNext(workflow, fromNodeId, outcome) {
    const node = getNode(workflow, fromNodeId);
    if (!node) throw new Error(`Node ${fromNodeId} not found`);

    const nodeEdges = Array.isArray(node.edges) ? node.edges : [];
    const workflowEdges = Array.isArray(workflow?.edges) ? workflow.edges : [];

    let edge = nodeEdges.find((e) => e && e.outcome === outcome);
    if (!edge) {
        edge = workflowEdges.find((e) => e && e.fromNodeId === fromNodeId && e.outcome === outcome);
    }

    if (!edge) return { toNodeId: null, delay: undefined };
    return { toNodeId: edge.toNodeId || null, delay: edge.delay };
}

function computeDueAt(now, delay) {
    const delayMs = parseDelayToMs(delay);
    return new Date(now.getTime() + delayMs);
}

/**
 * Return all outgoing edges from a node, deduplicating between
 * node-level edges and workflow-level edges.
 */
function getOutgoingEdges(workflow, fromNodeId) {
    const node = getNode(workflow, fromNodeId);
    const nodeEdges = node && Array.isArray(node.edges) ? node.edges : [];
    const workflowEdges = Array.isArray(workflow?.edges) ? workflow.edges : [];
    const wfEdges = workflowEdges.filter(e => e && e.fromNodeId === fromNodeId);

    const seen = new Set();
    const result = [];
    for (const e of [...nodeEdges, ...wfEdges]) {
        const key = `${e.outcome || ''}|${e.toNodeId || ''}`;
        if (!seen.has(key)) { seen.add(key); result.push(e); }
    }
    return result;
}

/**
 * Return every edge in the workflow (from both node.edges and workflow.edges).
 */
function flattenAllEdges(workflow) {
    const edges = [];
    if (workflow?.nodes) {
        for (const node of workflow.nodes) {
            if (Array.isArray(node.edges)) edges.push(...node.edges.map(e => ({ ...e, fromNodeId: node.id })));
        }
    }
    if (Array.isArray(workflow?.edges)) edges.push(...workflow.edges);
    return edges;
}

/**
 * Determine the campaign outcome for a completed Retell chat.
 *
 * Priority:
 *   1. Not answered — no message was ever sent (failedFirstMessage flag)
 *   2. Successful   — chat_analysis.chat_successful === true
 *   3. Unsuccessful — default
 *
 * @param {object} chatAnalysis - Retell chat_analysis object
 * @param {boolean} [failedFirstMessage=false] - true when WAHA delivery failed
 * @returns {'successful'|'unsuccessful'|'not_answered'}
 */
function determineChatOutcome(chatAnalysis, failedFirstMessage = false) {
    if (failedFirstMessage) return 'not_answered';
    const successful = chatAnalysis?.chat_successful;
    if (successful === true || successful === 'true') return 'successful';
    return 'unsuccessful';
}

module.exports = {
    makeTaskDedupeKey,
    makeStepDedupeKey,
    getNode,
    parseDelayToMs,
    resolveNext,
    computeDueAt,
    getOutgoingEdges,
    flattenAllEdges,
    determineChatOutcome
};
