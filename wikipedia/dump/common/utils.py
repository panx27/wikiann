import re
import warnings


RE_LINK = re.compile(r'<a href="(.*?)" type="(\w+)">(.*?)</a>')
RE_P14 = re.compile(r'\[\[Category:[^][]*\]\]', re.UNICODE)
RE_CAT = re.compile(r'\[\[Category:([^\|\]]+)', re.UNICODE)
RE_SEC = re.compile(r'(==+)\s*(.*?)\s*\1')


def is_in_range(x, y):
    return x[0] >= y[0] and x[1] <= y[1]


def is_overlapping(x, y):
    return max(x[0], y[0]) < min(x[1], y[1])


def normalize_wiki_title(s):
    s = s.replace(' ', '_').strip('_').strip()
    s = s.replace('_', ' ')
    # https://en.wikipedia.org/wiki/Wikipedia:Naming_conventions_(capitalization)
    if s and s[0].islower():
        s = s[0].upper() + s[1:]
    return s


def replace_links(text, shift=0):
    internal_links, external_links = [], []
    for i in re.finditer(RE_LINK, text):
        a_target, a_type, a_text = i.group(1), i.group(2), i.group(3)
        if not a_target:
            continue
        if not a_text:
            a_text = a_target
        text = text[:i.start() - shift] + a_text + text[i.end() - shift:]
        beg = i.start() - shift
        shift += len(i.group()) - len(a_text)

        link = {
            'text': a_text,
            'start': beg,
            'end': beg + len(a_text)
        }
        if a_type == 'internal':
            link['title'] = normalize_wiki_title(a_target)
            internal_links.append(link)
        elif a_type == 'external':
            link['url'] = a_target
            external_links.append(link)
        else:
            # raise Exception(f'Unrecognized anchor type: {a_type}')
            warnings.warn(f'Unrecognized anchor type: {a_type}', UserWarning)

    for i in internal_links + external_links:
        if text[i['start']:i['end']] != i['text']:
            raise Exception('Unmatched link offset')

    return text, internal_links, external_links


def extract_categories(text):
    cats = []
    error_count = 0
    for i in re.finditer(RE_P14, text):
        try:
            cats.append(re.match(RE_CAT, i.group()).group(1).strip())
        except AttributeError:
            error_count += 1
    return cats


def extract_sections(text):
    sects = []
    for i in re.finditer(RE_SEC, text):
        sects.append({
            'text': i.group(),
            'start': i.start(),
            'end': i.end(),
            'title': i.group().replace('=', '').strip(),
            'level': int(i.group().count('=') / 2)
        })
    return sects


def extract_infobox(text):
    m = re.search('{{infobox', text, re.I)
    if m:
        result = ''
        left = 0
        right = 0
        for c in text[m.start():]:
            if c == '{':
                left += 1
            elif c == '}':
                right += 1
            result += c
            if left == right:
                return result
    return None