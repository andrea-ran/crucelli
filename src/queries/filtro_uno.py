from filtro_base import FiltroBase

class FiltroUno(FiltroBase):
    def apply(self):
        # Esempio: stampa le squadre in zona Champions
        print("Squadre in zona Champions:")
        for league, slot in self.champions_slots.items():
            squadre = self.team_stats[self.team_stats["league_name"] == league].sort_values("rank").head(slot)["team_name"].tolist()
            print(f"{league}: {squadre}")
