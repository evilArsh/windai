import { each, summarise } from './lib/phases.js';
import { appendFragment, MARKER } from './lib/prompt.js';

export function onPrompt(input, ctx) {
  const fragments = Array.isArray(input.fragments) ? input.fragments : [];
  ctx.log('debug', 'demo prompt', `${fragments.length} -> ${fragments.length + 1}`);
  ctx.notify({ text: 'demo: prompt rewritten' });
  return appendFragment(input, fragments);
}

export function onRequest(input, ctx) {
  const text = JSON.stringify(input.contexts || []);
  ctx.emit('verify', { marker: MARKER, seen: text.includes(MARKER) });
  return null;
}

export function onToolCall(input, ctx) {
  const calls = Array.isArray(input.calls) ? input.calls : [];
  const names = calls
    .map((call) => call && call.name)
    .filter((name) => typeof name === 'string');
  ctx.log('info', 'demo tool calls', names.join(','));
  ctx.emit('toolcall', { names, suspend: input.suspend === true });
  return input;
}

export function onStatus(input, ctx) {
  const summary = summarise(each(input));
  ctx.log('info', 'demo phases', summary.phases.join(','));
  ctx.emit('activity', {
    mod: ctx.mod.name,
    version: ctx.mod.version,
    scope: ctx.scope.kind,
    phases: summary.phases,
    instance_id: summary.instance_id,
    terminal: summary.terminal,
  });
  if (summary.terminal) {
    ctx.notify({
      text: `demo: ${summary.terminal}`,
      level: summary.terminal === 'done' ? 'info' : 'error',
    });
  }
  return null;
}
