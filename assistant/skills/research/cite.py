#!/usr/bin/env python3
"""Renumber inline citations in a markdown report.

Pass 1 (the researcher) writes citations as [@](URL) right after a claim and a
"Sources" list at the end with [Title](URL) entries. This script numbers the
distinct URLs in order of first citation, replaces the markers with clickable superscript
[n], rebuilds the Sources section in that order (uncited entries kept at the
end), and prints a short report to stderr: citations, sources, uncited sources,
bare URLs left in the body (likely broken citations).

Safe to run again on its own output (an addendum with new markers gets numbered after the rest).

Usage: cite.py report.md            # rewrites in place
       cite.py report.md out.md
"""
import re, sys
from urllib.parse import urlsplit, urlunsplit

SRC_HEAD = re.compile(r'^(#{1,6})\s*(Sources|Lähteet|Källor|Источники|Quellen)\s*$', re.M | re.I)
MARK = re.compile(r'[ \t]*\[@\]\((https?://[^)\s]+)\)')
GROUP = re.compile(r'(?:[ \t]*\[@\]\(https?://[^)\s]+\)[,;]?)+')
LINK = re.compile(r'\[([^\]]+)\]\((https?://[^)\s]+)\)')
BARE = re.compile(r'(?<![(<])https?://[^\s)>\]]+')

def norm(u):
    p = urlsplit(u.strip())
    return urlunsplit((p.scheme.lower(), p.netloc.lower(), p.path.rstrip('/') or '/', p.query, ''))

SUPER = re.compile(r'\^\\\[((?:\[\d+\]\(https?://[^)\s]+\),?)+)\\\]\^')
NUMLINK = re.compile(r'\[\d+\]\((https?://[^)\s]+)\)')

def unsuper(text):
    # Make a second run idempotent: turn this script's own superscript output back into [@](URL) markers.
    return SUPER.sub(lambda m: ''.join(f'[@]({u})' for u in NUMLINK.findall(m.group(1))), text)

def main(src, dst):
    text = unsuper(open(src, encoding='utf-8').read())
    m = SRC_HEAD.search(text)
    body, head, tail = (text, None, '') if not m else (text[:m.start()], m.group(0).strip(), text[m.end():])
    titles, order_listed = {}, []
    for t, u in LINK.findall(tail):
        k = norm(u)
        titles.setdefault(k, (t.strip(), u))
        if k not in order_listed: order_listed.append(k)
    for u in BARE.findall(tail):
        k = norm(u)
        titles.setdefault(k, (u, u))
        if k not in order_listed: order_listed.append(k)
    nums, cited = {}, 0
    def repl(g):
        nonlocal cited
        ns = []
        for u in MARK.findall(g.group(0)):
            cited += 1
            k = norm(u)
            if k not in nums:
                nums[k] = len(nums) + 1
                titles.setdefault(k, (u, u))
            if nums[k] not in ns: ns.append(nums[k])
        # superscript [n] where each number links to its source
        byn = {v: k for k, v in nums.items()}
        return '^\\[' + ','.join(f'[{n}]({titles[byn[n]][1]})' for n in ns) + '\\]^'
    body = GROUP.sub(repl, body)
    heading = head or '## Sources'
    lines = [heading, '']
    for k, n in sorted(nums.items(), key=lambda kv: kv[1]):
        t, u = titles[k]
        lines.append(f'{n}. [{t}]({u})')
    uncited = [k for k in order_listed if k not in nums]
    if uncited:
        lines += ['', 'Also consulted:', '']
        for k in uncited:
            t, u = titles[k]
            lines.append(f'- [{t}]({u})')
    out = body.rstrip() + '\n\n' + '\n'.join(lines) + '\n'
    open(dst, 'w', encoding='utf-8').write(out)
    body_urls = [u for u in BARE.findall(body)]
    print(f'citations: {cited}, distinct sources cited: {len(nums)}, listed but uncited: {len(uncited)}, '
          f'bare URLs left in body: {len(body_urls)}', file=sys.stderr)
    for u in body_urls: print('  bare:', u, file=sys.stderr)

if __name__ == '__main__':
    a = sys.argv[1:]
    if not a: sys.exit(__doc__)
    main(a[0], a[1] if len(a) > 1 else a[0])
