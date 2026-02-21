from filtro_uno import FiltroUno
from season_config import STAGIONE_CORRENTE, STAGIONE_PRECEDENTE

ARCHIVE_PATH = "data/raw/team_stats_archive.csv"
CURRENT_PATH = "data/raw/team_stats_current.csv"
COPPA_PATH = "data/raw/coppa_nazionale.csv"
CHAMPIONS_SLOTS_PATH = "champions_slots.json"

if __name__ == "__main__":
    filtro = FiltroUno(
        archive_path=ARCHIVE_PATH,
        current_path=CURRENT_PATH,
        coppa_path=COPPA_PATH,
        champions_slots_path=CHAMPIONS_SLOTS_PATH,
        stagione_corrente=STAGIONE_CORRENTE,
        stagione_precedente=STAGIONE_PRECEDENTE
    )
    filtro.apply()
