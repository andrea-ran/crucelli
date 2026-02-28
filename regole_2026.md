TERMINOLOGIA

stagione corrente > 2025/2026
stagione precedente > 2024/2025
penultima stagione > 2023/2024
terzultima stagione > 2022/2023




Seleziona le squadre che in base alla classifica della mattina sono:

1) selezionare le squadre in zona qualificazione Champions League stagione corrente E ALLO STESSO TEMPO che si sono qualificate in Champions League nella stagione precedente, anche a causa della vittoria della coppa nazionale, o che hanno concluso la stagione precedente a 3 punti o meno dalla zona qualificazione Champions. Ovviamente, per la Saudi Professional League il riferimento è la AFC Champions League.

Logica filtro 1:
(in_zona_champions_corrente)
AND (qualificata_champions_precedente OR vincitrice_coppa_precedente OR entro_3_punti_dalla_zona_champions_precedente)

2) seleziona le squadre che - solo se qualificate in Champions nella stagione precedente o vincitrici di coppa nazionale - sono a 3 o meno punti dalla zona Champions o a 6 o meno punti se hanno giocato una partita in meno dell'ultima squadra in classifica che si trova in zona Champions.

Logica filtro 2:
(qualificata_champions_precedente OR vincitrice_coppa_precedente)
AND (punti_dalla_zona_champions <= 3 OR (punti_dalla_zona_champions <= 6 AND partite_in_meno_rispetto_ultima_champions = 1))

3)  selezionare le squadre che nella stagione corrente sono prima e seconda in classifica e le squadre fino a 6 punti dalla prima in classifica (fino a 8 punti dalla prima in classifica se hanno una partita in meno della prima in classifica) e che nello stesso tempo:
o si sono classificate prima e seconda nella penultima stagione > 2023/2024
o si sono classificate prima e seconda nella terzultima stagione > 2022/2023.

(
  (prima_o_seconda_corrente)
  OR
  (
    entro_6_punti_dalla_prima_corrente
    OR
    (una_partita_in_meno_della_prima_corrente AND entro_8_punti_dalla_prima_corrente)
  )
)
AND
(
  prima_o_seconda_penultima_stagione
  OR
  prima_o_seconda_terzultima_stagione
)


4) selezionare le squadre che nella stagione corrente sono prima e seconda in classifica e le squadre fino a 6 punti dalla prima in classifica (fino a 8 punti dalla prima in classifica se hanno una partita in meno della prima in classifica) e che nello stesso tempo:
o si sono classificate prima e seconda nella penultima stagione > 2023/2024
o si sono classificate prima e seconda nella terzultima stagione > 2022/2023.




SOLO PER LE PRIME DIECI GIORNATE DI CAMPIONATO:
•⁠  ⁠considerare come stagione corrente quella precedente e (di conseguenza) come precedenti la penultima e la terzultima
