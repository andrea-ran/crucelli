import requests

API_KEY = "691ccc74c6d55850f0b5c836ec0b10f2"
HEADERS = {"x-apisports-key": API_KEY}
league_id = 213  # Croatian Cup
season = 2024    # Cambia se vuoi indagare un'altra stagione

url = "https://v3.football.api-sports.io/fixtures"
params = {
    "league": league_id,
    "season": season
}
response = requests.get(url, headers=HEADERS, params=params)
data = response.json()

rounds = set()
for f in data.get("response", []):
    rounds.add(f["league"]["round"])

print("Tutti i valori di 'round' trovati per la Croatian Cup:")
for r in sorted(rounds):
    print(r)