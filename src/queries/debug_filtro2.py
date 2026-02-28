def debug_filtro_2(df, df_coppa, champions_slots, champions_slots_prev, stagione_corrente, stagione_precedente):
    def normalize_team_name(name):
        return name.lower().replace("sl ", "").replace("fc ", "").strip()
    def normalize_league_name(name):
        return name.lower().strip()
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
        if df_coppa.empty or "season" not in df_coppa.columns or "team_name" not in df_coppa.columns:
            return set()
        df_coppa_season = df_coppa[df_coppa["season"] == stagione].copy()
        df_coppa_season["team_name"] = df_coppa_season["team_name"].apply(normalize_team_name)
        return set(df_coppa_season["team_name"].tolist())
    zone_precedente = get_champions_zone(df, stagione_precedente, champions_slots)
    coppa_winners = get_coppa_winners(df_coppa, stagione_precedente)
    qualificate_precedente = zone_precedente.union(coppa_winners)
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
            punti_dalla_zona_champions = soglia_champions - punti
            partite_in_meno_rispetto_ultima_champions = partite_champions - partite_giocate if partite_champions is not None and partite_giocate is not None else None
            condizione_precedente = team in qualificate_precedente
            condizione_punti_base = 0 < punti_dalla_zona_champions <= 3
            condizione_punti_con_partita_in_meno = (
                0 < punti_dalla_zona_champions <= 6 and
                partite_in_meno_rispetto_ultima_champions == 1
            )
            print(f"Team: {team}, punti: {punti}, partite: {partite_giocate}, soglia: {soglia_champions}, partite_champions: {partite_champions}")
            print(f"  - In qualificate_precedente: {condizione_precedente}")
            print(f"  - punti_dalla_zona_champions: {punti_dalla_zona_champions}")
            print(f"  - partite_in_meno_rispetto_ultima_champions: {partite_in_meno_rispetto_ultima_champions}")
            print(f"  - condizione_punti_base: {condizione_punti_base}")
            print(f"  - condizione_punti_con_partita_in_meno: {condizione_punti_con_partita_in_meno}")
            if condizione_precedente and (condizione_punti_base or condizione_punti_con_partita_in_meno):
                print("  -> SELEZIONATA\n")
            else:
                print("  -> NON SELEZIONATA\n")

if __name__ == "__main__":
    import pandas as pd
    import json
    # Carica i dati standings di test
    df = pd.read_csv("data/standings_bundesliga_test.csv")
    # Nessuna coppa per il test
    df_coppa = pd.DataFrame()
    with open("champions_slots.json") as f:
        champions_slots = json.load(f)
    stagione_corrente = 2025
    stagione_precedente = 2024
    debug_filtro_2(
        df,
        df_coppa,
        champions_slots[str(stagione_corrente)],
        champions_slots[str(stagione_precedente)],
        stagione_corrente,
        stagione_precedente
    )
