# synonyms.py
# Centralizzazione dei sinonimi per leghe e squadre

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

def normalize_league_name(name):
    name = name.lower().strip()
    for key, values in LEAGUE_SYNONYMS.items():
        if name in [v.lower().strip() for v in values]:
            return key
    return name

# Esempio di normalizzazione squadra (puoi estendere con sinonimi se necessario)
def normalize_team_name(name):
    return name.lower().replace("sl ", "").replace("fc ", "").strip()
