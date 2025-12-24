# 🧠 Appestat - Appie Analytics

Appestat is een persoonlijke "voedsel-accountant" die je helpt je Albert Heijn-uitgaven te analyseren. De naam verwijst naar de **appestat**, het mechanisme in de hersenen dat de eetlust reguleert.

Deze applicatie geeft je diepgaand inzicht in je uitgaven, helpt je patronen herkennen, en stimuleert gezonder en/of prijsbewuster boodschappen te doen.

![Appestat](https://img.shields.io/badge/Appestat-Food%20Regulator-00ADE6)
![Python](https://img.shields.io/badge/Python-3.9+-blue)
![Privacy](https://img.shields.io/badge/Privacy-Local%20Only-green)

## ✨ Kenmerken

### 📊 Inzicht & Regulatie
- **Totaaloverzicht** van al je voedseluitgaven.
- **Categorie-analyse**: Zie precies hoeveel je uitgeeft aan snacks, frisdrank, vs. groente en fruit.
- **30-jaar projectie**: Een confronterende blik op de lange termijn kosten van je huidige gewoontes.
- **Besparingsadviezen**: Concrete tips om gezonder te eten én goedkoper boodschappen te doen.

### 🔍 Geavanceerde Analyse
- **Productdetails**: Bekijk prijshistorie en inflatie per product.
- **A-merk vs Huismerk**: Ontdek waar je onnodig te veel betaalt.
- **Jaaroverzichten**: Vergelijk je gedrag over de jaren heen.

### 🛠️ Technische Kracht
- **PDF & Bonnen Import**: Verwerkt automatisch je Albert Heijn facturen en kassabonnen.
- **Slimme Categorisatie**: Herkent automatisch honderden producten en deelt ze in (Zuivel, Groente, Snacks, etc.).
- **Privacy First**: Alle data blijft 100% lokaal op je eigen computer. Geen cloud, geen tracking.

---

## 🚀 Installatie

Je kunt Appestat op twee manieren draaien:

### Optie A: Via Docker (Aanbevolen)

1. **Bouw de image**
   ```bash
   docker build -t appestat .
   ```

2. **Start de container**
   ```bash
   docker run -p 5050:5050 -v $(pwd)/data:/app/data appestat
   ```
   *De `-v` flag zorgt ervoor dat je database en geïmporteerde bestanden bewaard blijven.*

3. **Open je browser**
   Ga naar [http://localhost:5050](http://localhost:5050)

### Optie B: Lokaal (Python)

1. **Vereisten**
   - Python 3.9 of hoger

2. **Installatie**
   ```bash
   # Clone de repo
   git clone https://github.com/twetering/appestat.git
   cd appestat

   # Maak virtual environment (optioneel maar aanbevolen)
   python3 -m venv venv
   source venv/bin/activate

   # Installeer dependencies
   pip install -r requirements.txt
   ```

3. **Starten**
   ```bash
   python app.py
   ```
   Ga naar [http://localhost:5050](http://localhost:5050)

---

## 📥 Je Data Importeren

Appestat werkt met je eigen data. Zo importeer je je bonnen:

1. **Online Facturen**: Download je facturen van bestellingen (PDF) via je online Albert Heijn account en plaats ze in `data/invoices/`.
2. **Kassabonnen**: Kassabonnen van Albert Heijn aankopen in de winkel plaats je in `data/bonnen/`. Je kunt de kassabonnen downloaden via je Albert Heijn app (iOS of Android). De snelste manier is via 'Deel Kassabon' en dan 'Save to files' waarbij je een iCloud-map kan selecteren. Alle PDF's vind je op je laptop dan terug op je bijvoorbeeld je Desktop of Downloads.
3. **Import Starten**: 
   - Start de app en navigeer naar **Facturen**.
   - Klik op de **Importeer** knop.
   - De app verwerkt alle nieuwe bestanden automatisch.

---

## 🤝 Bijdragen

Appestat is open source. Heb je ideeën om de "appestat" van gebruikers beter te reguleren? 
- Verbeterde categorisatie-regels
- Nieuwe psychologische inzichten/features
- Ondersteuning voor andere supermarkten

Pull requests zijn welkom!

## 📝 Licentie

Dit project is beschikbaar onder de MIT licentie. Zie `LICENSE` voor details.
