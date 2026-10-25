export function onNow() {
  const now = new Date();
  return {
    iso: now.toISOString(),
    local: now.toString(),
    epoch_ms: now.getTime(),
  };
}
