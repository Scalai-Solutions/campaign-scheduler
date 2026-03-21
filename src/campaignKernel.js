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

function resolveNext(workflow, fromNodeId, outcome) {
    const node = getNode(workflow, fromNodeId);
    if (!node) throw new Error(`Node ${fromNodeId} not found`);
    const edge = node.edges.find((e) => e.outcome === outcome);
    if (!edge) return { toNodeId: null, delay: undefined };
    return { toNodeId: edge.toNodeId, delay: edge.delay };
}

function computeDueAt(now, delay) {
    if (!delay || typeof delay !== 'string') return now;
    const match = delay.match(/^(\d+)([hdm])$/);
    if (!match) return now;
    const value = parseInt(match[1]);
    const unit = match[2];
    const dueAt = new Date(now.getTime());
    switch (unit) {
        case 'h': dueAt.setHours(dueAt.getHours() + value); break;
        case 'd': dueAt.setDate(dueAt.getDate() + value); break;
        case 'm': dueAt.setMinutes(dueAt.getMinutes() + value); break;
    }
    return dueAt;
}

module.exports = {
    makeTaskDedupeKey,
    makeStepDedupeKey,
    getNode,
    resolveNext,
    computeDueAt
};
