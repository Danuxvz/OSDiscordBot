import difflib

VALID_ROUTES = [
    "Rio Barakawa", "Academia Mofunoakabe", "El Cluster", "Templo Fudakudai", "Bosque de Onigashima", "Crimson Light District",
    "Academia Saint George", "The Fae Parliament", "Everything Hill", "St. Peter Cathedral", "Thames River", "The Botanical Forest",
    "Academia St Peter", "Gehenna Door", "Abandoned Colosseum", "Elysian Garden", "Central Church", "Hopeless River", "Toy Factory"
]

ROUTE_ALIASES = {
    "Rio Barakawa": [
        "barakawa river",
        "rio barakawa",
        "river barakawa",
        "río barakawa"
    ],
    "Academia Mofunoakabe": [
        "mofuno",
        "academia mofuno",
        "mofunoakabe",
        "mofunoakabe academy"
    ],
    "El Cluster": [
        "cluster",
        "el cluster",
        "the cluster"
    ],
    "Templo Fudakudai": [
        "fudakudai temple",
        "templo fudakudai",
        "temple fudakudai"
    ],
    "Bosque de Onigashima": [
        "onigashima forest",
        "bosque onigashima",
        "bosque de onigashima",
        "forest of onigashima",
        "onigashima"
    ],
    "Crimson Light District": [
        "crimson district",
        "crimson light",
        "crimson light district",
        "red light district",
        "red light",
        "distrito luz carmesí",
        "distrito carmesí"
    ],
    "Academia Saint George": [
        "saint george academy",
        "academia san george",
        "san george academy",
        "st. george academy"
    ],
    "The Fae Parliament": [
        "fae parliament",
        "the fae parliament",
        "parlamento feérico",
        "parlamento fae"
    ],
    "Everything Hill": [
        "everything hill",
        "colina del todo",
        "la colina todo"
    ],
    "St. Peter Cathedral": [
        "st peter cathedral",
        "st. peter cathedral",
        "catedral san pedro",
        "cathedral st peter",
        "san pedro cathedral"
    ],
    "Thames River": [
        "river thames",
        "thames",
        "rio thames",
        "río thames"
    ],
    "The Botanical Forest": [
        "botanical forest",
        "forest botanical",
        "bosque botánico",
        "jardin botánico",
        "botanical garden"
    ],
    "Academia St Peter": [
        "st peter academy",
        "st. peter academy",
        "academia st peter",
        "academia san pedro",
        "san pedro academy"
    ],
    "Gehenna Door": [
        "gehenna door",
        "puerta gehenna",
        "la puerta gehenna",
        "door of gehenna"
    ],
    "Abandoned Colosseum": [
        "abandoned colosseum",
        "coliseo abandonado",
        "coliseo viejo",
        "colosseum"
    ],
    "Elysian Garden": [
        "elysian garden",
        "jardín elíseo",
        "jardin elyseo",
        "jardín elíseo"
    ],
    "Central Church": [
        "central church",
        "iglesia central",
        "la iglesia central"
    ],
    "Hopeless River": [
        "hopeless river",
        "río sin esperanza",
        "rio sin esperanza",
        "Hopeless"
    ],
    "Toy Factory": [
        "toy factory",
        "fábrica de juguetes",
        "fabrica de juguetes",
        "la fábrica de juguetes",
        "jugeteria",
        "Santa's little secret stash of the good stuff",
    ]
}

ALIAS_MAP = {}
for canonical, alias_list in ROUTE_ALIASES.items():
    for a in alias_list:
        ALIAS_MAP[a.lower()] = canonical

def match_route(user_route, items_table, cutoff=0.7):
    if not user_route:
        return None
    normalized = user_route.strip().lower()

    # 1) Alias exact match
    if normalized in ALIAS_MAP:
        return ALIAS_MAP[normalized]

    # 2) Fuzzy match on aliases
    alias_candidates = list(ALIAS_MAP.keys())
    matches = difflib.get_close_matches(normalized, alias_candidates, n=1, cutoff=cutoff)
    if matches:
        return ALIAS_MAP[matches[0]]

    # 3) Exact route name match
    if user_route in VALID_ROUTES:
        return user_route

    # 4) Fuzzy match on VALID_ROUTES
    matches = difflib.get_close_matches(user_route, VALID_ROUTES, n=1, cutoff=cutoff)
    if matches:
        return matches[0]

    # 5) Exact key in items_table
    if user_route in items_table:
        return user_route

    # 6) Fuzzy match on items_table keys
    keys = list(items_table.keys())
    matches = difflib.get_close_matches(user_route, keys, n=1, cutoff=cutoff)
    if matches:
        return matches[0]

    return None