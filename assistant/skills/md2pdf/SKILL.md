---
name: md2pdf
description: Render a markdown document to PDF (pandoc + typst, A4, 2 cm margins, emoji and Cyrillic OK). Use when a report or long result should go to the chat as a PDF file.
---
Run `md2pdf.sh input.md out/name.pdf` (script is in this skill's directory), then send the PDF with send_file and a one-sentence caption.
Defaults live in `md2pdf.yaml` next to the script; a document's own YAML header overrides them. Keep the fonts set: without them pandoc's Typst template fails.
Markdown tables are fine in the PDF, and a title/date YAML header gives a title block.
