# 🧠 Appestat

Appestat is een persoonlijke "voedsel-accountant": hij leest je boodschappenbonnen (Albert Heijn, PLUS)
en laat precies zien waar je geld naartoe gaat. De naam verwijst naar de **appestat**, het mechanisme in de
hersenen dat de eetlust reguleert.

![Appestat](https://img.shields.io/badge/Appestat-3.0-00A0DC)
![Python](https://img.shields.io/badge/Python-3.10+-blue)
![Privacy](https://img.shields.io/badge/Privacy-lokaal-green)

## ✨ Wat Appestat 3.0 doet

**Cijfers die kloppen**
- Elke bon wordt gecontroleerd: producten + bijkomende kosten (statiegeld, bezorging, abonnement) + sparen
  (koopzegels, PLUSPunten) tellen tot op de cent op tot het betaalde bedrag. Bonnen die niet sluiten worden
  gemarkeerd in plaats van stil meegeteld.
- "Uitgegeven" betekent overal hetzelfde: betaald min gespaard. Categorieën, maanden en jaren tellen daar
  precies tot op.

**Eén product is één product**
- Een productcatalogus koppelt alle schrijfwijzen en verpakkingen: "Red Bull 8-pack", "…24-pack" en
  "RED BULL" op een kassabon zijn één product met varianten en een **prijs per stuk**.
- Productpagina's met prijsverloop per stuk, de voordeligste verpakking en AH tegenover PLUS.

**Slimme categorisatie**
- Nieuwe producten krijgen hun categorie › subcategorie van de OpenAI Decisions API (vaste keuzelijst,
  dus nooit een subcategorie buiten zijn categorie). Twijfelgevallen komen in de **reviewlijst**.
- Jouw eigen keuzes gaan altijd voor en gelden voor het product op al je bonnen, ook toekomstige.
- Zonder OpenAI-sleutel werkt alles met trefwoordregels.

**Inzichten**
- Dashboard per jaar of alles, per winkel; budget met tempo; jaarvergelijking; prijsstijgingen per stuk;
  wat een grotere verpakking had bespaard; 30-jaarsprojectie.

## 🚀 Installatie

```bash
git clone https://github.com/twetering/appestat.git
cd appestat
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
echo "OPENAI_API_KEY=sk-..." > .env      # optioneel, voor AI-categorisatie
python app.py                            # http://localhost:5050
```

Ontwikkelen: `pip install -r requirements-dev.txt`, `APPESTAT_DEBUG=1 python app.py`, tests met `pytest`.

## 📥 Bonnen importeren

Sleep bestanden op de pagina **Importeren**, of zet ze in `data/invoices/` (submappen mogen) en klik op
"Map opnieuw doorzoeken".

| Bron | Bestand |
|---|---|
| Albert Heijn online-bestelling | factuur-PDF (`1069835-00414.pdf`) |
| Albert Heijn winkel | kassabon-PDF uit de AH-app (`AH_kassabon_….pdf`) |
| PLUS (en andere winkels) | foto van de kassabon (HEIC/JPG/PNG), bv. in `data/invoices/plus/` |

Foto's worden lokaal gelezen met Apple Vision (macOS, Xcode Command Line Tools). Alleen als die lezing niet
sluit, gaat de foto naar OpenAI als vangnet. Dezelfde bon twee keer importeren (ook als ander bestand) wordt
herkend.

## 🔒 Privacy

Alle data staat lokaal in `data/appestat.db`. Naar OpenAI gaan alleen productnamen (voor categorisatie en
normalisatie) en, uitsluitend als vangnet, foto's die lokaal niet goed gelezen konden worden.

## 🤖 Voor AI-agents

Zie `CLAUDE.md` (architectuur en regels) en `API.md` (endpoints).

## 📝 Licentie

MIT, zie `LICENSE`.
