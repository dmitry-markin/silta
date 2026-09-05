//! Text helpers for outbound messages.

/// Split Markdown text into chunks of at most `max_bytes` bytes, preferring
/// paragraph boundaries, then line boundaries, then character boundaries. Fenced code
/// blocks count as one paragraph so a fence is never split across chunks unless it
/// alone exceeds the cap. Empty text yields no chunks.
pub fn chunk_text(text: &str, max_bytes: usize) -> Vec<String> {
    assert!(max_bytes >= 4, "cap must fit a character");
    let text = text.trim_end();
    if text.is_empty() {
        return Vec::new();
    }
    if text.len() <= max_bytes {
        return vec![text.to_owned()];
    }

    let mut chunks = Vec::new();
    let mut current = String::new();
    for paragraph in paragraphs(text) {
        if paragraph.len() > max_bytes {
            flush(&mut chunks, &mut current);
            for piece in split_lines(paragraph, max_bytes) {
                append(&mut chunks, &mut current, &piece, "\n", max_bytes);
            }
            continue;
        }
        append(&mut chunks, &mut current, paragraph, "\n\n", max_bytes);
    }
    flush(&mut chunks, &mut current);
    chunks
}

/// Paragraphs separated by blank lines, with fenced code blocks kept whole.
fn paragraphs(text: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut start = 0;
    let mut in_fence = false;
    let mut pos = 0;
    let mut last_blank = false;
    for line in text.split_inclusive('\n') {
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.starts_with("```") || trimmed.starts_with("~~~") {
            in_fence = !in_fence;
        }
        let blank = trimmed.trim().is_empty();
        if blank && !in_fence {
            if !last_blank && pos > start {
                out.push(text[start..pos].trim_end());
            }
            start = pos + line.len();
            last_blank = true;
        } else {
            last_blank = false;
        }
        pos += line.len();
    }
    if pos > start {
        let tail = text[start..pos].trim_end();
        if !tail.is_empty() {
            out.push(tail);
        }
    }
    out
}

fn split_lines(paragraph: &str, max_bytes: usize) -> Vec<String> {
    let mut out = Vec::new();
    for line in paragraph.split('\n') {
        if line.len() <= max_bytes {
            out.push(line.to_owned());
        } else {
            out.extend(split_chars(line, max_bytes));
        }
    }
    out
}

fn split_chars(line: &str, max_bytes: usize) -> Vec<String> {
    let mut out = Vec::new();
    let mut start = 0;
    let mut last = 0;
    for (i, c) in line.char_indices() {
        if i + c.len_utf8() - start > max_bytes {
            out.push(line[start..i].to_owned());
            start = i;
        }
        last = i + c.len_utf8();
    }
    if last > start {
        out.push(line[start..last].to_owned());
    }
    out
}

fn append(chunks: &mut Vec<String>, current: &mut String, piece: &str, sep: &str, max_bytes: usize) {
    if current.is_empty() {
        current.push_str(piece);
    } else if current.len() + sep.len() + piece.len() <= max_bytes {
        current.push_str(sep);
        current.push_str(piece);
    } else {
        flush(chunks, current);
        current.push_str(piece);
    }
}

fn flush(chunks: &mut Vec<String>, current: &mut String) {
    if !current.is_empty() {
        chunks.push(std::mem::take(current));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_text_is_one_chunk() {
        assert_eq!(chunk_text("hello\n\nworld\n", 100), vec!["hello\n\nworld"]);
        assert!(chunk_text("  \n", 100).is_empty());
    }

    #[test]
    fn keeps_paragraphs_whole() {
        let text = "aaaa aaaa\n\nbbbb bbbb\n\ncccc cccc";
        let chunks = chunk_text(text, 19);
        assert_eq!(chunks, vec!["aaaa aaaa", "bbbb bbbb", "cccc cccc"]);
        // Two paragraphs plus the separator are exactly 20 bytes.
        let chunks = chunk_text(text, 20);
        assert_eq!(chunks, vec!["aaaa aaaa\n\nbbbb bbbb", "cccc cccc"]);
    }

    #[test]
    fn never_exceeds_the_cap() {
        let long_line = "x".repeat(50);
        let text = format!("p1\n\n{long_line}\nshort\n\np3 {}", "y".repeat(30));
        for cap in [8, 13, 20, 33, 64] {
            for chunk in chunk_text(&text, cap) {
                assert!(chunk.len() <= cap, "cap {cap}: {chunk:?}");
                assert!(!chunk.is_empty());
            }
        }
        let joined: String = chunk_text(&text, 13).join("");
        assert_eq!(joined.matches('x').count(), 50);
    }

    #[test]
    fn splits_multibyte_text_on_char_boundaries() {
        let text = "жжжжжжжжжж"; // 10 chars, 20 bytes
        let chunks = chunk_text(text, 7);
        assert_eq!(chunks, vec!["жжж", "жжж", "жжж", "ж"]);
    }

    #[test]
    fn fenced_code_stays_together() {
        let text = "intro\n\n```\nline 1\n\nline 3\n```\n\noutro";
        let chunks = chunk_text(text, 25);
        assert_eq!(chunks, vec!["intro", "```\nline 1\n\nline 3\n```", "outro"]);
        // With room to spare the fence joins its neighbour instead of being split.
        let chunks = chunk_text(text, 30);
        assert_eq!(chunks, vec!["intro\n\n```\nline 1\n\nline 3\n```", "outro"]);
    }
}
