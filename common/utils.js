function cleanSku(input) {
  if (input == null) return null;
  const s = String(input).trim().replace(/[^0-9]/g, '').replace(/^0+/, '');
  return s.length > 0 ? s : null;
}
module.exports = { cleanSku };
