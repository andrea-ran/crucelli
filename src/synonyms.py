import re

TEAM_SYNONYMS = {
    "bayern munich": ["bayern munich", "bayern münchen", "bayern m", "bayern", "bayern-munich", "bayern-muenchen", "bayernmunich", "bayernmuenchen"],
    "borussia dortmund": ["borussia dortmund", "dortmund", "b. dortmund", "borussia-dortmund"],
    "manchester city": ["manchester city", "man city", "manchester-city", "mancity"],
    # aggiungi altri sinonimi se necessario
}

LEAGUE_SYNONYMS = {
    "saudi pro league": ["saudi pro league", "saudi professional league"],
    "prva hnl": ["prva hnl", "hnl"],
    "primeira liga": ["primeira liga", "liga portugal"],
    "bundesliga": ["bundesliga"],
    "ligue 1": ["ligue 1"],
    "serie a": ["serie a"],
    "premier league": ["premier league"],
    "la liga": ["la liga", "laliga"],
    "eredivisie": ["eredivisie"],
    "super lig": ["super lig"],
}

def normalize_team_name(name):
    n = name.lower()
    n = re.sub(r"[^a-z0-9]", "", n)  # rimuove tutto tranne lettere e numeri
    n = n.replace("ü", "u").replace("é", "e").replace("á", "a").replace("ö", "o").replace("ä", "a")
    for key, values in TEAM_SYNONYMS.items():
        for v in values:
            v_norm = re.sub(r"[^a-z0-9]", "", v.lower())
            if n == v_norm:
                return key
    return n

def normalize_league_name(name):
    name = name.lower().strip()
    for key, values in LEAGUE_SYNONYMS.items():
        if name in [v.lower().strip() for v in values]:
            return key
    return name
    return n
