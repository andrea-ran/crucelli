import pandas as pd
import json

class FiltroBase:
    def __init__(self, archive_path, current_path, coppa_path, champions_slots_path, stagione_corrente, stagione_precedente):
        self.archive_path = archive_path
        self.current_path = current_path
        self.coppa_path = coppa_path
        self.champions_slots_path = champions_slots_path
        self.stagione_corrente = stagione_corrente
        self.stagione_precedente = stagione_precedente
        self.champions_slots = self._load_slots()
        self.team_stats = self._load_team_stats()
        self.df_coppa = pd.read_csv(self.coppa_path)

    def _load_slots(self):
        with open(self.champions_slots_path, "r") as f:
            champions_slots_all = json.load(f)
        return champions_slots_all[str(self.stagione_corrente)]

    def _load_team_stats(self):
        df_archive = pd.read_csv(self.archive_path)
        df_current = pd.read_csv(self.current_path)
        return pd.concat([df_archive, df_current], ignore_index=True)

    def apply(self):
        raise NotImplementedError("Devi implementare la logica del filtro in una sottoclasse!")
