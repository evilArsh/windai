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

export function unique(values) {
  return [...new Set(values)];
}
