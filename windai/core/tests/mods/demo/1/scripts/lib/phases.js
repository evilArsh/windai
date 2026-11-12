const TERMINAL = ['done', 'failed', 'aborted', 'cancelled'];

// 观测投递池按 (mod, event, scope) 合批，一条时是值本身，多条时是数组
export function each(input) {
  if (Array.isArray(input)) {
    return input;
  }
  if (input === null || typeof input !== 'object') {
    return [];
  }
  return [input];
}

export function summarise(events) {
  const phases = [];
  let instanceId = null;
  for (const event of events) {
    if (event === null || typeof event !== 'object') {
      continue;
    }
    if (typeof event.instance_id === 'number') {
      instanceId = event.instance_id;
    }
    if (typeof event.phase === 'string') {
      phases.push(event.phase);
    }
  }
  let terminal = null;
  for (const phase of phases) {
    if (TERMINAL.includes(phase)) {
      terminal = phase;
    }
  }
  return { phases, instance_id: instanceId, terminal };
}
