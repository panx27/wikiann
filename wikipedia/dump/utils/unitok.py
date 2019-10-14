import unicodedata as ud


def unitok_tokenize(data):
    toks = []
    for offset, char in enumerate(data):
        cc = ud.category(char)
        # separate text by punctuation or symbol
        if char in ['ʼ', '’', '‘', '´', '′', "'"]:  # do not tokenize oromo apostrophe
            toks.append(char)
        elif cc.startswith("P") or cc.startswith("S") \
                or char in ['።', '፡']:  # Tigrinya period and comma
            toks.append(' ')
            toks.append(char)
            toks.append(' ')
        else:
            toks.append(char)
    toks = [item for item in ''.join(toks).split() if item]

    return toks
