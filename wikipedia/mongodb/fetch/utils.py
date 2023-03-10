def is_overlapping(x, y):
    return max(x[0], y[0]) < min(x[1], y[1])


def is_valid_tok(tok):
    tok = tok.strip()
    if tok == '':
        return False
    if tok == '*':
        return False
    if tok == '=':
        return False
    return True


def normalise_link(lang, kbid, kbname='Wikipedia'):
    if kbname == 'Wikipedia':
        return '%s.wikipedia.org/wiki/%s' % (lang, kbid.replace(' ', '_'))
    elif kbname == 'Wikidata':
        return 'www.wikidata.org/wiki/%s' % (kbid)
    else:
        raise Exception('Unexpected KB name: %' % kbname)


def convert_link_plain(link, lower=True):
    if lower:
        s = ' '.join([t[0].lower() for t in link['tokens']])
    else:
        s = ' '.join([t[0] for t in link['tokens']])
    return s


def convert_link(lang, link, html=False):
    s = ''
    if link['id']:
        s = normalise_link(lang, link['title'])
    if html:
        s = '<a href="https://%s" target="_blank">%s</a>' \
            % (s, convert_link_plain(link))
    return s


def convert_link_ll(lang, ll_lang, link, keep_link_wo_ll=False, html=False):
    s = ''
    if 'title_ll' in link:
        s = normalise_link(ll_lang, link['title_ll'])
    elif keep_link_wo_ll and link['id']:
        s = normalise_link(lang, link['title'])
    if html:
        s = '<a href="https://%s" target="_blank">%s</a>' \
            % (s, convert_link_plain(link))
    return s
