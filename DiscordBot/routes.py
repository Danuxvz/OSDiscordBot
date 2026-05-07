import difflib
from .config import supabase

VALID_ROUTES = [
    "Rio Barakawa", "Academia Mofunoakabe", "El Cluster", "Templo Fudakudai", "Bosque de Onigashima", "Crimson Light District",
    "Academia Saint George", "The Fae Parliament", "Everything Hill", "St. Peter Cathedral", "Thames River", "The Botanical Forest",
    "Academia St Peter", "Gehenna Door", "Abandoned Colosseum", "Elysian Garden", "Central Church", "Hopeless River", "Toy Factory"
]

BUILTIN_ALIASES = {
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


async def load_guild_aliases(guild_id: str):
    """Fetch custom aliases for a guild from Supabase."""
    if not supabase:
        return {}
    try:
        res = supabase.table("route_aliases") \
            .select("canonical, alias") \
            .eq("guild_id", guild_id) \
            .execute()
        return {row["alias"].lower(): row["canonical"] for row in (res.data or [])}
    except Exception as e:
        print("[ROUTES] Failed to load aliases:", e)
        return {}

def get_alias_map(guild_aliases: dict):
    """Merge built‑in aliases and guild‑specific ones."""
    alias_map = {}
    for canonical, alias_list in BUILTIN_ALIASES.items():
        for a in alias_list:
            alias_map[a.lower()] = canonical
    # guild aliases override built‑ins if there's a conflict
    alias_map.update(guild_aliases)
    return alias_map

async def match_route(user_route, items_table, guild_id=None, cutoff=0.7):
    if not user_route:
        return None
    normalized = user_route.strip().lower()

    guild_aliases = await load_guild_aliases(str(guild_id)) if guild_id else {}
    alias_map = get_alias_map(guild_aliases)

    # Exact alias match
    if normalized in alias_map:
        return alias_map[normalized]

    # Fuzzy match on alias keys
    alias_candidates = list(alias_map.keys())
    matches = difflib.get_close_matches(normalized, alias_candidates, n=1, cutoff=cutoff)
    if matches:
        return alias_map[matches[0]]

    # Exact route name match
    if user_route in VALID_ROUTES:
        return user_route

    # Fuzzy match on VALID_ROUTES
    matches = difflib.get_close_matches(user_route, VALID_ROUTES, n=1, cutoff=cutoff)
    if matches:
        return matches[0]

    # Exact key in items_table
    if user_route in items_table:
        return user_route

    # Fuzzy match on items_table keys
    keys = list(items_table.keys())
    matches = difflib.get_close_matches(user_route, keys, n=1, cutoff=cutoff)
    if matches:
        return matches[0]

    return None