// Typst header for md2pdf: let tables flow across pages instead of jumping to a new page and overflowing it.
#show figure: set block(breakable: true)
#show table: set text(size: 9.5pt)

// Code size, multiplied with Typst's default of 0.8em for raw text: 1.15 gives about 92 % of the body size.
#let code-scale = 1.15

// Inline code that can break after punctuation and at camelCase boundaries (zero-width spaces;
// raw text is never hyphenated), so long identifiers fit narrow table columns on phone-sized pages.
// The rebuilt raw element goes through raw's default 0.8em and this rule's size once more, so the
// wrapper cancels one round of scaling and every inline code ends up at the same size.
#show raw.where(block: false): it => {
  set text(size: code-scale * 1em)
  if it.text.contains("\u{200b}") { it } else {
    let t = it.text
    for p in ("::", ":", "_", "/", ".", "-", "=") { t = t.replace(p, p + "\u{200b}") }
    t = t.replace(regex("([a-z0-9])([A-Z])"), m => m.captures.at(0) + "\u{200b}" + m.captures.at(1))
    if t == it.text { it } else { text(size: 1em / (0.8 * code-scale), raw(t, lang: it.lang)) }
  }
}

// Code blocks at the same size as inline code.
#show raw.where(block: true): set text(size: code-scale * 1em)
