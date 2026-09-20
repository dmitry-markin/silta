// Typst header for md2pdf: let tables flow across pages instead of jumping to a new page and overflowing it.
#show figure: set block(breakable: true)
#show table: set text(size: 9.5pt)
// Inline code that can break after punctuation and at camelCase boundaries (zero-width spaces;
// raw text is never hyphenated), so long identifiers fit narrow table columns on phone-sized pages.
#show raw.where(block: false): it => {
  let t = it.text
  for p in ("::", "_", "/", ".", "-", "=") { t = t.replace(p, p + "\u{200b}") }
  t = t.replace(regex("([a-z0-9])([A-Z])"), m => m.captures.at(0) + "\u{200b}" + m.captures.at(1))
  if t == it.text or it.text.contains("\u{200b}") { it } else { raw(t, lang: it.lang) }
}
