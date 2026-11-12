export const MARKER = '[script:roleplay]';

export function appendFragment(input, fragments) {
  return {
    ...input,
    fragments: fragments.concat([
      `${MARKER} 这一段由 roleplay 的脚本在 core/prompt.collect 上追加`,
    ]),
  };
}
