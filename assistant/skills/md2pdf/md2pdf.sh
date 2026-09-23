#!/bin/sh
# Render a markdown file to PDF with pandoc + typst.
# Usage: md2pdf.sh input.md [output.pdf]   (lives in the md2pdf skill directory)
# Defaults (A4, 2 cm margins, fonts) come from md2pdf.yaml next to this script;
# md2pdf.typ makes tables breakable.
# --wrap=none: pandoc wrapped a "/ " onto a line start in the Typst source and typst
# read it as a term-list item.
# mainfont/monofont must always be set: pandoc 3.1.11's Typst template passes an
# empty font list otherwise, which typst >= 0.13 rejects.
set -eu
in="$1"
out="${2:-${in%.md}.pdf}"
here="$(cd "$(dirname "$0")" && pwd)"
exec pandoc "$in" -f markdown-citations --wrap=none --pdf-engine=typst --metadata-file="$here/md2pdf.yaml" --include-in-header="$here/md2pdf.typ" -o "$out"
