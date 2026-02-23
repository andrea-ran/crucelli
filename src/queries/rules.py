def selezione_filtro_5(df, df_coppa, df_upcoming, champions_slots, champions_slots_prev, stagione_corrente, stagione_precedente):
    # Funzioni di supporto
    def get_champions_zone(df, stagione, champions_slots):
        df_season = df[df["season"] == stagione]
        result = []
        league_synonyms = {
            "saudi pro league": ["saudi pro league", "saudi professional league"]
        }
        for league, slot in champions_slots.items():
            if league in league_synonyms:
                mask = df_season["league_name"].str.lower().isin([s.lower() for s in league_synonyms[league]])
                squadre = df_season[mask].sort_values("rank").head(slot)["team_name"].tolist()
            else:
                squadre = df_season[df_season["league_name"] == league].sort_values("rank").head(slot)["team_name"].tolist()
            result.extend(squadre)
        return set(result)

    def get_coppa_winners(df_coppa, stagione):
        return set(df_coppa[df_coppa["season"] == stagione]["team_name"].tolist())

    # Squadre qualificate Champions stagione precedente (classifica + coppa)
    zone_precedente = get_champions_zone(df, stagione_precedente, champions_slots)
    coppa_winners = get_coppa_winners(df_coppa, stagione_precedente)
    qualificate_precedente = zone_precedente.union(coppa_winners)

    results = []

    for league in champions_slots.keys():
        df_league = df[(df["season"] == stagione_corrente) & (df["league_name"] == league)].copy()
        if df_league.empty:
            continue
        df_sorted = df_league.sort_values("rank")
        if len(df_sorted) < 2:
            continue
        seconda = df_sorted.iloc[1]
        punti_seconda = seconda["points"]
        partite_seconda = seconda["matches"] if "matches" in seconda else None
        home_teams = set(df_upcoming["home_team"].unique())
        for _, row in df_league.iterrows():
            team = row["team_name"]
            punti = row["points"]
            partite = row["matches"] if "matches" in row else None
            diff = punti - punti_seconda
            diff_partite = None
            if partite is not None and partite_seconda is not None:
                diff_partite = partite_seconda - partite
            condizione = False
            if row["rank"] in [1,2]:
                condizione = True
            elif diff < 0 and diff_partite == -1:
                condizione = True
            if condizione and team in home_teams:
                if team in qualificate_precedente:
                    results.append(team)
    return results
# rules.py
from synonyms import normalize_league_name

def get_champions_zone(df, stagione, champions_slots):
    df_season = df[df["season"] == stagione].copy()
    df_season["league_name"] = df_season["league_name"].apply(normalize_league_name)
    result = []
    for league, slot in champions_slots.items():
        league_norm = normalize_league_name(league)
        squadre = df_season[df_season["league_name"] == league_norm].sort_values("rank").head(slot)["team_name"].tolist()
        result.extend(squadre)
    return set(result)

def get_coppa_winners(df_coppa, stagione):
    return set(df_coppa[df_coppa["season"] == stagione]["team_name"].tolist())

# SOLO la logica di selezione

def selezione_filtro_1(df, df_coppa, champions_slots, champions_slots_prev, stagione_corrente, stagione_precedente):
    zone_corrente = get_champions_zone(df, stagione_corrente, champions_slots)
    zone_precedente = get_champions_zone(df, stagione_precedente, champions_slots_prev)
    coppa_winners = get_coppa_winners(df_coppa, stagione_precedente)
    qualificate_precedente = zone_precedente.union(coppa_winners)
    selezionate = [team for team in zone_corrente if team in qualificate_precedente]
    return selezionate

def selezione_filtro_2(df, champions_slots, stagione_corrente, stagione_precedente):
    from synonyms import normalize_league_name
    # Squadre in zona Champions stagione corrente
    def get_champions_zone(df, stagione, champions_slots):
        df_season = df[df["season"] == stagione].copy()
        df_season["league_name_norm"] = df_season["league_name"].apply(normalize_league_name)
        result = []
        for league, slot in champions_slots.items():
            league_norm = normalize_league_name(league)
            squadre = df_season[df_season["league_name_norm"] == league_norm].sort_values("rank").head(slot)["team_name"].tolist()
            result.extend(squadre)
        return set(result)

    def get_champions_points_threshold(df, stagione, league, champions_slots):
        slot = champions_slots.get(league, 4)
        league_norm = normalize_league_name(league)
        df_season = df[df["season"] == stagione].copy()
        df_season["league_name_norm"] = df_season["league_name"].apply(normalize_league_name)
        league_teams = df_season[df_season["league_name_norm"] == league_norm]
        if league_teams.empty:
            return None
        sorted_teams = league_teams.nsmallest(slot, "rank")
        if sorted_teams.empty:
            return None
        return sorted_teams.iloc[-1]["points"]

    zone_corrente = get_champions_zone(df, stagione_corrente, champions_slots)
    results = []
    for team in zone_corrente:
        row_corr = df[(df["season"] == stagione_corrente) & (df["team_name"] == team)].copy()
        if row_corr.empty:
            continue
        league = row_corr.iloc[0]["league_name"]
        league_norm = normalize_league_name(league)
        row_prev = df[(df["season"] == stagione_precedente) & (df["team_name"] == team)].copy()
        row_prev["league_name_norm"] = row_prev["league_name"].apply(normalize_league_name)
        row_prev = row_prev[row_prev["league_name_norm"] == league_norm]
        if row_prev.empty:
            continue
        punti_squadra = row_prev.iloc[0]["points"]
        soglia_champions = get_champions_points_threshold(df, stagione_precedente, league, champions_slots)
        if soglia_champions is None:
            continue
        diff = punti_squadra - soglia_champions
        if diff >= -3:
            results.append(team)

    return results


# Filtro 4: logica copiata da filter_teams_4_2026.py
def selezione_filtro_4(df, champions_slots, stagione_corrente, stagione_penultima, stagione_terzultima):
    league_synonyms = {
        "saudi pro league": ["saudi pro league", "saudi professional league"],
        "prva hnl": ["prva hnl", "hnl"]
    }
    results = []
    for league in champions_slots.keys():
        # Gestione sinonimi Saudi Pro League
        if league in league_synonyms:
            mask = (df["season"] == stagione_corrente) & (df["league_name"].str.lower().isin([s.lower() for s in league_synonyms[league]]))
            df_league = df[mask].copy()
        else:
            df_league = df[(df["season"] == stagione_corrente) & (df["league_name"] == league)].copy()
        if df_league.empty:
            continue
        df_sorted = df_league.sort_values("rank")
        if len(df_sorted) < 2:
            continue
        prima = df_sorted.iloc[0]
        seconda = df_sorted.iloc[1]
        for _, row in df_league.iterrows():
            team = row["team_name"]
            punti = row["points"]
            partite = row["matches"] if "matches" in row else None
            punti_prima = prima["points"]
            partite_prima = prima["matches"] if "matches" in prima else None
            diff = punti - punti_prima
            diff_partite = None
            if partite is not None and partite_prima is not None:
                diff_partite = partite_prima - partite
            condizione = False
            if row["rank"] in [1,2]:
                condizione = True
            elif diff == -1:
                condizione = True
            elif diff == -3 and diff_partite == -1:
                condizione = True
            elif diff in [-2, -6, -8] and diff_partite == -1:
                condizione = True
            if not condizione:
                continue
            penultima = df[(df["season"] == stagione_penultima) & (df["league_name"] == league) & (df["team_name"] == team)]
            terzultima = df[(df["season"] == stagione_terzultima) & (df["league_name"] == league) & (df["team_name"] == team)]
            condizione_storica = False
            if not penultima.empty and penultima.iloc[0]["rank"] in [1,2]:
                condizione_storica = True
            if not terzultima.empty and terzultima.iloc[0]["rank"] in [1,2]:
                condizione_storica = True
            if condizione_storica:
                results.append(team)
    return results


# Filtro 3: logica copiata da filter_teams_3_2026.py
def selezione_filtro_3(df, df_coppa, champions_slots, champions_slots_prev, stagione_corrente, stagione_precedente):
    from synonyms import normalize_league_name, normalize_team_name
    # Squadre qualificate Champions stagione precedente (classifica + coppa)
    def get_champions_zone(df, stagione, champions_slots):
        df_season = df[df["season"] == stagione].copy()
        df_season["team_name"] = df_season["team_name"].apply(normalize_team_name)
        df_season["league_name"] = df_season["league_name"].apply(normalize_league_name)
        result = []
        for league, slot in champions_slots.items():
            league_norm = normalize_league_name(league)
            squadre = df_season[df_season["league_name"] == league_norm].sort_values("rank").head(slot)["team_name"].tolist()
            result.extend(squadre)
        return set(result)

    def get_coppa_winners(df_coppa, stagione):
        df_coppa_season = df_coppa[df_coppa["season"] == stagione].copy()
        df_coppa_season["team_name"] = df_coppa_season["team_name"].apply(normalize_team_name)
        return set(df_coppa_season["team_name"].tolist())

    zone_precedente = get_champions_zone(df, stagione_precedente, champions_slots)
    coppa_winners = get_coppa_winners(df_coppa, stagione_precedente)
    qualificate_precedente = zone_precedente.union(coppa_winners)

    results = []

    for league, slot in champions_slots.items():
        league_norm = normalize_league_name(league)
        df_league = df[(df["season"] == stagione_corrente) & (df["league_name"].apply(normalize_league_name) == league_norm)].copy()
        df_league["team_name"] = df_league["team_name"].apply(normalize_team_name)
        if df_league.empty:
            continue
        df_sorted = df_league.sort_values("rank")
        if len(df_sorted) < slot:
            continue
        soglia_champions = df_sorted.iloc[slot-1]["points"]
        partite_champions = df_sorted.iloc[slot-1]["matches"] if "matches" in df_sorted.columns else None
        df_out = df_league[df_league["rank"] > slot]
        for _, row in df_out.iterrows():
            team = row["team_name"]
            punti = row["points"]
            partite_giocate = row["matches"] if "matches" in row else None
            diff = punti - soglia_champions
            diff_partite = None
            if partite_champions is not None and partite_giocate is not None:
                diff_partite = partite_champions - partite_giocate
            condizione = False
            if team in qualificate_precedente:
                if diff >= -3 and diff <= 0:
                    condizione = True
                elif diff >= -6 and diff <= 0 and diff_partite == -1:
                    condizione = True
            if condizione:
                results.append(team)
    return results
