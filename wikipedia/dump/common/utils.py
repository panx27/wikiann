def is_in_range(x, y):
    return x[0] >= y[0] and x[1] <= y[1]


def is_overlapping(x, y):
    return max(x[0], y[0]) < min(x[1], y[1])


def normalise_wikilink(s, prefix):
    s = s.replace(' ', '_').strip('_').strip()
    return prefix + s
