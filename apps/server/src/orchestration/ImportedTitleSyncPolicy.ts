const HERDR_TITLE_MARKER_PATTERN = /^● Herdr \S+ /;

/** Identify titles produced by the old first-line Codex import fallback. */
export function hasLegacyCodexContextTitle(title: string): boolean {
  return (
    title === "<recommended_plugins>" ||
    title === "<environment_context>" ||
    title === "<user_instructions>" ||
    /^# AGENTS\.md instructions(?:\s|$)/.test(title)
  );
}

export function manualImportedTitleSyncPolicy(
  title: string,
): { readonly prefix: string; readonly titleForFallback: string } | null {
  const marker = title.match(HERDR_TITLE_MARKER_PATTERN)?.[0];
  if (marker !== undefined) {
    return { prefix: marker, titleForFallback: title.slice(marker.length) };
  }
  return hasLegacyCodexContextTitle(title) ? { prefix: "", titleForFallback: title } : null;
}
