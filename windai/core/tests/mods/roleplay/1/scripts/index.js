import { each, unique } from './lib/batch.js';
import { appendFragment, MARKER } from './lib/guard.js';

export function onPrompt(input, ctx) {
  const fragments = Array.isArray(input.fragments) ? input.fragments : [];
  ctx.log('debug', 'roleplay prompt', `${fragments.length} -> ${fragments.length + 1}`);
  ctx.notify({ text: 'roleplay: prompt rewritten' });
  return appendFragment(input, fragments);
}

export function onRequest(input, ctx) {
  const text = JSON.stringify(input.contexts || []);
  ctx.emit('verify', { marker: MARKER, seen: text.includes(MARKER) });
  return null;
}

export function onSeal(input, ctx) {
  const seals = each(input).filter((seal) => seal !== null && typeof seal === 'object');
  const reasons = unique(
    seals
      .map((seal) => seal.finish_reason)
      .filter((reason) => typeof reason === 'string'),
  );
  const outputTokens = seals.reduce((total, seal) => {
    const usage = seal.usage || {};
    return total + (Number(usage.output_tokens) || 0);
  }, 0);
  ctx.log('info', 'roleplay seal', reasons.join(','));
  ctx.emit('seal', {
    reasons,
    output_tokens: outputTokens,
    scope: ctx.scope.kind,
  });
  if (reasons.includes('length') || reasons.includes('content_filter')) {
    ctx.notify({ text: `roleplay: finish ${reasons.join(',')}`, level: 'warn' });
  }
  return null;
}

export function onSealed(input, ctx) {
  const roots = each(input).filter((event) => event !== null && typeof event === 'object');
  const ids = unique(
    roots
      .map((event) => event.instance_id)
      .filter((id) => typeof id === 'number'),
  );
  ctx.log('info', 'roleplay sealed', ids.join(','));
  ctx.emit('report', {
    sealed: ids.length,
    topic_id: ctx.scope.kind === 'topic' ? ctx.scope.topic_id : null,
  });
  ctx.notify({ text: 'roleplay: sealed' });
  return null;
}
