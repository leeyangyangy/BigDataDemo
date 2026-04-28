export const VALID_RULE_IDS = new Set([1, 2, 3, 4, 5, 6, 7, 8])
export const MIN_RULE_ID = 1
export const MAX_RULE_ID = 8
export const TOTAL_RULES = MAX_RULE_ID - MIN_RULE_ID + 1

export function isValidRuleId(id) {
  return Number.isInteger(id) && id >= MIN_RULE_ID && id <= MAX_RULE_ID
}

export function sanitizeRuleIds(raw) {
  if (!raw || !Array.isArray(raw)) return []
  return raw.filter(id => VALID_RULE_IDS.has(id))
}

export function formatValidRange() {
  return `[${MIN_RULE_ID}-${MAX_RULE_ID}]`
}