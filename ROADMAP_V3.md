# Appestat 3.0: review en verbeterplan

*Opgesteld 2 november 2026, op basis van een volledige code-review, een UI-rondgang door alle pagina's
en metingen op de echte database (216 bonnen, 6.452 productregels, €22.465 betaald).*

---

## 1. Samenvatting

Appestat doet al veel: AH-facturen en PLUS-bonnen importeren, categoriseren, budgetteren en
projecteren. Maar de **cijfers zijn niet betrouwbaar en niet consistent**, en de **eenheid "product" klopt
niet**. Dat ondermijnt elk inzicht dat erop gebouwd is.

De drie grootste problemen:

1. **Een product is nu gewoon de letterlijke tekst van de bon.** Red Bull staat er als 11+ losse
   producten in (8-pack, 12-pack, 24-pack, `ENERGY DRINK 4PACK`, …), samen ±€1.450. Elke weging van
   kaas of fruit is ook een apart product (`ZAANLANDER (0.092kg)`, `(0.136kg)`, …): 1.851 "producten"
   waar er veel minder zijn. Gemiddelde prijzen, prijsstijgingen, top-20 en 30-jaarsprojecties zijn
   daardoor misleidend.
2. **Dezelfde vraag geeft op verschillende plekken een ander antwoord.** "Uitgegeven in 2026" is
   €4.113,75, €3.384,85 of €3.385, afhankelijk van de pagina. "Bonus bespaard" is €460,01 of €2,54. De
   30-jaarsprojectie van Red Bull 8-pack is €2.558 of €11.375. Overig is 195, 138 of €729.
3. **De AH-parser mist producten.** Het eerste product van elke factuurpagina plakt aan de paginakop vast
   en wordt weggegooid: **237 regels, €679**. 17,9% van alles wat je bij AH betaalde komt niet terug in
   de productcijfers. Een deel is terecht geen product (statiegeld, kratten en €2.788 aan Koopzegels),
   maar dat is nergens zichtbaar.

Daarnaast is de codebase gegroeid tot een paar enorme bestanden: `database.py` (2.185 regels),
`app.py` (72 routes) en 9 templates van elk 600–1.800 regels met hun eigen CSS en JS. Daardoor is
elke verbetering duur, en ontstaan inconsistenties juist doordat dezelfde logica op meerdere plekken
staat.

**Visie voor 3.0:** *Eén betrouwbare waarheid over je boodschappen.* Elk bedrag sluit tot op de cent,
elk product is één product (met varianten), en elke pagina rekent met dezelfde definities.

---

## 2. Bevindingen

Ernst: 🔴 fout in cijfers of dataverlies · 🟠 serieus probleem voor gebruik of onderhoud · 🟡 verbetering

### 2.1 Data en productidentiteit

| # | Ernst | Bevinding | Bewijs |
|---|---|---|---|
| D1 | 🔴 | Alle analyses groeperen op `original_name`: product = ruwe bontekst | `GROUP BY original_name` op 10+ plekken in `database.py` |
| D2 | 🔴 | Varianten en verpakkingen worden niet herkend | Red Bull: 11+ namen, €1.450; 171 groepen van naamvarianten |
| D3 | 🔴 | Gewicht staat in de productnaam, dus elke weging is een nieuw product | `ZAANLANDER (0.092kg)`, `BROCCOLI (0.500kg)`, `AH ZALMFILET (0.244kg)` |
| D4 | 🟠 | Geen eenheidsprijs (per blik/kg/L), dus prijsvergelijkingen kloppen niet | "AH Notenmix +160%" vergelijkt verschillende verpakkingen |
| D5 | 🟠 | Hetzelfde product staat in verschillende categorieën | 39 productgroepen verspreid over meer dan 1 categorie |
| D6 | 🟠 | Geen winkel-entiteit: AH en PLUS zijn alleen te onderscheiden via `source_type` | geen winkelkolom of -filter in de UI |
| D7 | 🟡 | `quantity` en `price` (regeltotaal) worden niet consequent als stukprijs gebruikt | 646 regels met quantity > 1 |

### 2.2 Import en parsers

| # | Ernst | Bevinding | Bewijs |
|---|---|---|---|
| P1 | 🔴 | De naam-regex matcht over regeleinden (`\s` in de karakterklasse). Het eerste product per pagina gaat verloren of krijgt een verminkte naam | 237 regels / €678,98; `BONUS\nJean Bâton…`, `Red Bull 3-pack\nRed Bull 6-pack` (`parser.py`, `extract_products_from_online_invoice`) |
| P2 | 🔴 | `POST /api/invoices/<id>/reparse` draait de AH-parser op élke bon en wist alle producten plus je handmatige categorieën | `app.py:897`; PLUS- en API-bonnen hebben geen AH-`raw_text` |
| P3 | 🔴 | Niet-productkosten (koopzegels €2.788, statiegeld, kratten, verpakking, bezorgkosten) worden weggegooid in plaats van vastgelegd | AH: €21.565 betaald tegen €17.704 aan producten |
| P4 | 🟠 | `force=True` bij import faalt altijd (UNIQUE `file_hash`) | `parser.import_invoice` + `save_invoice` |
| P5 | 🟠 | `/api/invoices/import-single` accepteert een willekeurig pad op schijf | `app.py:388` |
| P6 | 🟡 | AH-factuurdatum = afleverdatum, geen besteldatum; dag-van-de-week-analyse is daardoor scheef | "Maandag €1.837" |
| P8 | 🟠 | 5 facturen zonder regels: jaarlijks €107,88 (7 mei) en €12–15 (oktober), vermoedelijk abonnementen. Ze tellen mee in "uitgegeven" maar zijn verder onzichtbaar | `1069835-00305/00327/00357/00384/00409` |
| P7 | 🟡 | Twee parsergeneraties naast elkaar (`parser.py` en `sources/*`); de registry wordt niet gebruikt bij import | `sources/ah_online.py` / `ah_receipt.py` zijn dode code |

### 2.3 Categorisatie

| # | Ernst | Bevinding | Bewijs |
|---|---|---|---|
| C1 | 🔴 | Een subcategorie kan buiten zijn categorie vallen | 109 regels / €243 (Wasa crackers → *Borrel* / "Fruit"; Waldkorn → *Bakkerij* / "Hollandse kaas") |
| C2 | 🔴 | Duidelijke foutcategorisaties door te losse keyword-matching | rode druiven → Wijn; Tony's Chocolonely → Ontbijtgranen; Alpro yoghurt → Mueslirepen; snoepgroente paprika → Sauzen; peen julienne → Overig |
| C3 | 🟠 | Categorisatie gebeurt per regel in plaats van per product, dus elke correctie geldt alleen voor die exacte naam | 1.361 handmatige correcties tegen 1.851 namen |
| C4 | 🟠 | 1.253 regels zonder subcategorie, 195 regels (€597) op Overig | DB-meting |
| C5 | 🟠 | 46 LLM-suggesties wachten op review, de meeste met 0% zekerheid en zonder categorie (mislukte calls opgeslagen als suggestie) | `/admin`; `llm_suggestions` |
| C6 | 🟠 | Twee LLM-paden naast elkaar | `CategorizationService.categorize_with_llm` en `services/llm_categorizer.py` |
| C7 | 🟡 | Geen woordenboek voor PLUS-afkortingen (`MRGRN`, `VLK`, `ONGBR`) | 65 PLUS-regels op Overig |
| C8 | 🟡 | Tabel `category_rules` is leeg en ongebruikt; `categories.parent_id` bestaat naast de tabel `subcategories` | schema |

### 2.4 Consistentie van cijfers

| # | Ernst | Bevinding | Bewijs |
|---|---|---|---|
| M1 | 🔴 | Drie definities van "uitgegeven": bontotalen, productsom en budget | dashboard €4.113,75 · jaren €3.384,85 · budget €3.385 |
| M2 | 🔴 | "Bonus Korting" telt producten met "Bonus" in de naam (alleen de kapotte `BONUS\n…`-regels) | dashboard: €2,54 naast "Bonus bespaard €460,01"; `get_bonus_analysis` |
| M3 | 🔴 | 30-jaarsprojectie heeft twee formules (delen door alle maanden of door maanden met aankoop) | Red Bull 8-pack: €7,11/mnd (dashboard) tegen €31,60/mnd (popup) |
| M4 | 🟠 | Overig-telling verschilt per weergave (het paneel negeert `user_category`) | 195 / 138 / €729,04 / €597 |
| M5 | 🟠 | Jaarfilter wordt niet overal toegepast | "A-merk €8.362", eenmalige aankopen, prijsstijgingen: all-time onder een 2026-selectie |
| M6 | 🟠 | "Gem. per maand" (productsom) staat naast "Totaal" (bontotaal) op dezelfde rij | dashboard |
| M7 | 🟠 | Drie budgetbegrippen | Budget-pagina (€7.191), Jaren "Jaarbudget Planner (€338 over)", tabel `budget_goals` (leeg) |
| M8 | 🟡 | Vaste bespaarheuristieken ("snacks × 0,5 × 30 jaar") zonder uitleg | `database.py:852–882` |
| M9 | 🟡 | Onvolledige jaren (2023: €163) worden gepresenteerd als jaartotaal | Jaren-pagina |

### 2.5 Backend-architectuur en codekwaliteit

| # | Ernst | Bevinding | Bewijs |
|---|---|---|---|
| A1 | 🟠 | `database.py` is een godmodule (2.185 regels): schema, queries, analytics, migraties en budget door elkaar | |
| A2 | 🟠 | `app.py` heeft 72 routes in één bestand, met businesslogica in route-handlers | AH-sync: 120 regels in de route |
| A3 | 🟠 | Vaste jaartallen in API-defaults, in strijd met CLAUDE.md | `year="2025"/"2026"` in de budgetroutes |
| A4 | 🟠 | SQL via f-strings met parameters | `database.py:2069, 2119` (`'{year}'`) |
| A5 | 🟠 | Geen invoervalidatie: `request.json` kan `None` zijn en geeft dan 500's | o.a. `api_update_category` |
| A6 | 🟡 | Legacy- en eenmalige endpoints staan nog open | `/api/migrate`, `/api/migrate/subcategories`, `reparse_all_invoices` in `__main__` |
| A7 | 🟡 | `app.run(debug=True)`: de Werkzeug-debugger is RCE als de poort ooit extern bereikbaar wordt | `app.py:1246` |
| A8 | 🟡 | Ongebruikte tabellen: `category_rules`, `budget_goals`, `validation_feedback` | 0 rijen |
| A9 | 🟡 | Dependencies zonder bovengrens, geen lockfile | `requirements.txt` |

### 2.6 Frontend en UX

| # | Ernst | Bevinding | Bewijs |
|---|---|---|---|
| U1 | 🟠 | Geen base-template: 9 losse pagina's met eigen CSS (~4.200 regels) en JS (~4.800 regels) | `templates/*.html` |
| U2 | 🟠 | Visueel inconsistent | logo "A" tegen "ah", grijze bovenbalk wisselt, eigen kleurtokens per pagina |
| U3 | 🟠 | Navigatie niet responsive: op 800 px valt "Facturen" weg, op mobiel is alleen "Dashboard" zichtbaar, geen hamburgermenu | alleen `index.html` heeft echte `@media`-regels |
| U4 | 🟠 | Beheer is vanaf geen enkele pagina bereikbaar | nav-links per template |
| U5 | 🟠 | Facturenlijst: 216 rijen in één keer, geen zoeken, filteren, sorteren of winkelkolom; breder dan het scherm | `/invoices` |
| U6 | 🟠 | Bondetail rendert 92 dropdowns met 1.440 opties en laadt ±5 s; prijs en acties vallen weg op smallere schermen; twee checkboxkolommen | `/invoice/<id>` |
| U7 | 🟠 | Lange spinners zonder skeleton; dubbele fetches (`/api/analysis` twee keer) | dashboard, jaren |
| U8 | 🟡 | Bedragnotatie wisselt: "€7,191" (en-US) naast "€ 4.113,75" (nl-NL) | budget, dashboard |
| U9 | 🟡 | Onbekende bon (`/invoice/999999`) geeft 200 en een eindeloze spinner | |
| U10 | 🟡 | 85× `innerHTML` met bonteksten zonder escaping (XSS via een vreemde productnaam) | templates |
| U11 | 🟡 | Chart.js van een CDN zonder vaste versie | `cdn.jsdelivr.net/npm/chart.js` |
| U12 | 🟡 | Productenpagina toont niets zonder zoekterm; "gemiddelde prijs" middelt verpakkingen | `/search` |
| U13 | 🟡 | Het AH-API-koppelblok staat prominent bovenaan Facturen, terwijl je die koppeling niet gebruikt | `/invoices` |
| U14 | 🟡 | "Geplande besparing −€914" groen met een minteken; "Bestellingen" telt ook winkelbezoeken | budget, dashboard |

### 2.7 Testen en kwaliteitsborging

- 20 tests, alleen op overrides, LLM-mapping en de PLUS-parser. **Geen tests op analytics, routes of de
  AH-parser**, terwijl daar de meeste fouten zitten.
- Geen CI, linting of type-checking.
- Geen "golden" testdata (geanonimiseerde voorbeeldbonnen) om regressies in parsers te vangen.

---

## 3. Doelarchitectuur 3.0

### 3.1 Datamodel

```
stores            id, name (AH, PLUS, …), chain, branch, city
receipts          id, store_id, source (ah_pdf|plus_photo|…), file_hash, purchased_at,
                  paid_total, savings_total, quality_score, notes, raw_text, metadata
receipt_lines     id, receipt_id, line_type (product|deposit|fee|stamps|discount|points),
                  raw_name, quantity, line_total, unit_price, weight_kg, product_id → products
products          id, name ("Red Bull Energy drink"), brand, family_id, category_id, subcategory_id,
                  size_value, size_unit (stuks|g|ml), pack_count, is_private_label
product_aliases   raw_name (genormaliseerd) → product_id, source (auto|user|llm), confidence
product_families  id, name ("Energiedrank") (optioneel, voor "alle Red Bull samen")
categories        id, name, parent_id (subcategorie = categorie met parent; één boom, afgedwongen)
budgets           id, year, category_id (NULL = totaal), amount, warning_threshold
```

Kernideeën:
- **`receipt_lines` legt alles vast, ook statiegeld, koopzegels, kortingen en PLUSPunten**, met een
  `line_type`. Daardoor geldt per bon: Σ regels = betaald totaal, tot op de cent. Analyses kiezen bewust
  "alleen producten" of "alles".
- **`products` is de catalogus, `product_aliases` koppelt bonteksten eraan.** "Red Bull Energy drink
  24-pack", "…8-pack voordeelverpakking" en `ENERGY DRINK 4PACK` worden aliassen van één product
  (of varianten in één family), met `pack_count` voor de prijs per blik.
- **Gewicht en verpakking gaan uit de naam** naar `weight_kg`, `size_value` en `pack_count`.
- **Categorie hoort bij het product, niet bij de regel.** Eén correctie geldt voor alle aankopen, ook
  toekomstige.

### 3.2 Eén metrics-laag

Een module `services/metrics.py` met precies één definitie per begrip, gebruikt door elke pagina én de API:

| Begrip | Definitie |
|---|---|
| Uitgegeven | Σ `paid_total` van bonnen in de periode |
| Boodschappen | Σ `line_total` van regels met `line_type = product` |
| Overige kosten | statiegeld, kratten, koopzegels, bezorging, punten (apart zichtbaar) |
| Bespaard | Σ `savings_total` (bonus, acties, korting) |
| Per maand | gedeeld door het aantal kalendermaanden in de periode (onvolledige maanden gemarkeerd) |
| Projectie | één formule, met de gebruikte periode erbij vermeld |
| Eenheidsprijs | `line_total / (quantity × pack_count)` of per kg/L |

Elke metric-functie neemt `(period, store, category)` als filter, zodat het jaarfilter overal hetzelfde
werkt.

### 3.3 Backend-structuur

```
appestat/
  app.py                 app-factory, blueprints registreren
  config.py              instellingen (paden, LLM-modellen, debug)
  db/                    schema, migraties, connection
  repositories/          receipts, products, categories, budgets (alleen SQL, geparametriseerd)
  services/              metrics, categorization, catalog (aliases/varianten), budget, import
  importers/             ah_pdf, plus_photo (+ registry die echt gebruikt wordt)
  api/                   blueprints: receipts, products, categories, budget, insights, admin
  web/                   pagina-routes
  templates/  static/
```

- Validatie van invoer per endpoint, met consistente foutmeldingen in JSON.
- Geen vaste jaartallen; standaard is altijd het huidige jaar.
- Legacy-endpoints verwijderen of achter `/api/admin` met bevestiging.

### 3.4 Frontend

CLAUDE.md blijft gelden: vanilla JS, geen framework. Wel met structuur:
- `templates/base.html` met één header, responsive navigatie (hamburgermenu onder 900 px), footer
  en bovenbalk; elke pagina `{% extends %}`.
- `static/css/app.css` met design tokens (kleuren, spacing, typografie, light/dark), componenten
  (card, table, badge, kpi, chip, modal).
- `static/js/` als ES-modules: `api.js` (fetch + foutafhandeling), `format.js` (één `Intl.NumberFormat('nl-NL')`),
  `dom.js` (veilige rendering, nooit `innerHTML` met data), `charts.js` (gepinde Chart.js, lokaal).
- Categorie-keuze als één gedeelde zoekbare picker (popover) in plaats van 1.440 `<option>`s per pagina.
- Skeletons in plaats van spinners; data van één endpoint per paneel; echte 404-pagina's.

---

## 4. Nieuwe en verbeterde functies

### 4.1 Productcatalogus en varianten (lost "Red Bull 8×" op)
- **Productpagina** per catalogusproduct: alle varianten (6/8/12/24-pack, PLUS-4pack) met **prijs per
  blik**, prijsgrafiek per eenheid, AH tegen PLUS, "beste koop" en aankoopfrequentie.
- **Automatisch groeperen**: normaliseren (merk, verpakking, gewicht, "voordeelverpakking", "BONUS"),
  dan fuzzy clusteren, en LLM-batch alleen voor twijfelgevallen. Alles als **voorstel** in een
  review-scherm.
- **Samenvoegen en splitsen** in de UI: varianten naar een product slepen, of een alias loskoppelen.
- Overzichten (top 20, projectie, prijsstijging) werken op **product**-niveau, met per variant uit te klappen.

### 4.2 Categorisatie v3
- Categorie per **product**; de boom wordt afgedwongen (een subcategorie hoort altijd bij zijn
  categorie, met een database-constraint).
- Volgorde: handmatige keuze → catalogus → keywords (op woordgrenzen, met uitzonderingen zoals
  "druiven" ≠ wijn) → LLM-batch (alleen met geldige sleutel, kosten vooraf getoond).
- **Review-wachtrij**: alles onder een zekerheidsdrempel, gesorteerd op € impact. Toetsenbord-workflow
  (↑↓, 1–9 voor categorie, Enter = accepteren).
- PLUS-afkortingenwoordenboek (en per winkel uitbreidbaar).
- Doel: **Overig < 1% van uitgaven, 0 regels zonder subcategorie, 0 subcategorieën buiten hun ouder.**

### 4.3 Kloppende cijfers overal
- Elke KPI-kaart heeft een tooltip "zo is dit berekend" en een klik naar de onderliggende bonnen.
- **Afstemmingsweergave per bon**: producten + overige kosten − kortingen = betaald (groen vinkje of
  afwijking).
- Periodekiezer (maand / kwartaal / jaar / eigen bereik) en winkelfilter, gedeeld over alle pagina's.

### 4.4 Importcentrum
- Upload via **slepen** (PDF en foto's) in de browser, naast de map `data/invoices/`.
- Importlog met per bon: winkel, aantal regels, afstemmingsstatus en waarschuwingen (`quality_score < 1`).
- **Bon handmatig corrigeren**: een bon die niet sluit openen naast de foto/PDF, regels aanpassen en
  opnieuw laten controleren.
- Herparsen per bron met de juiste parser, met behoud van handmatige categorieën (via de catalogus).

### 4.5 Budget (één model)
- Eén budgetmodel: jaar → totaal + per categorie, optioneel per product ("max €200 Red Bull").
- Tempo-indicator per maand (lineair of op basis van je seizoenspatroon), waarschuwingen op 80%/100%.
- De "Jaarbudget Planner" op de Jaren-pagina en de tabel `budget_goals` gaan op in dit model.

### 4.6 Inzichten die kloppen
- **Prijsstijging per eenheid** (en shrinkflation-detectie: zelfde prijs, kleinere verpakking).
- **AH tegen PLUS**: wat kost jouw vaste mandje waar?
- **Huismerk-alternatieven** op productniveau (bv. A-merk-energiedrank tegen huismerk, per blik).
- Koopzegels en statiegeld als aparte, uitlegbare posten ("je hebt €2.788 gespaard in koopzegels").
- 30-jaarsprojectie met één formule, de gebruikte periode erbij en een "wat als ik halveer"-schuif.

### 4.7 Onderhoud en data
- Export naar CSV/Excel (bonnen, regels, producten).
- Back-up/herstel vanuit de UI (de bestaande back-up-download plus terugzetten).
- Optioneel: de AH-API-koppeling verbergen achter een instelling (je gebruikt hem niet).

---

## 5. Roadmap

Elke fase levert iets bruikbaars op en heeft meetbare acceptatiecriteria.

### Fase 0: Kritieke fixes (1–2 dagen)
Snel veel betrouwbaarheid winnen, zonder de architectuur te raken.

- [ ] **P1** Naam-regex niet over regeleinden laten matchen; daarna AH-facturen herparsen.
      *Accept:* de 237 gemiste regels zijn terug; geen enkele productnaam bevat `\n`.
- [ ] **P2** `reparse` alleen voor AH-PDF's (of per bron de juiste parser) en handmatige categorieën behouden.
- [ ] **M2** "Bonus Korting" baseren op `total_savings`. *Accept:* gelijk aan "Bonus bespaard".
- [ ] **M3** Eén projectieformule in dashboard en popup.
- [ ] **M4** Overig-paneel telt `COALESCE(user_category, auto_category)`.
- [ ] **C1** Subcategorieën die niet bij hun categorie horen herstellen (109 regels) en bij opslaan valideren.
- [ ] **D3** Gewicht uit de naam halen (migratie plus parser) en varianten op dezelfde naam samenvoegen.
- [ ] **A3/A4/P5** Vaste jaartallen eruit, f-string-SQL parametriseren, `import-single` beperken tot `data/invoices/`.
- [ ] **U4/U8/U9** Beheer in de navigatie, één nl-NL-formatter, 404 bij een onbekende bon.
- [ ] Opruimen: 46 LLM-suggesties zonder categorie verwijderen.

### Fase 1: Fundament: datamodel en metrics (1–2 weken)
- [ ] Nieuw schema (`stores`, `receipts`, `receipt_lines` met `line_type`, `products`, `product_aliases`,
      één categorieboom) en een migratie van alle bestaande data, **zonder verlies van handmatige
      categorieën** (die worden productcategorieën).
- [ ] Parsers schrijven álle regels weg (koopzegels, statiegeld, verpakking, bezorging en abonnementen
      als `line_type`), zodat ook de 5 facturen zonder producten een regel krijgen.
- [ ] `services/metrics.py` gebruikt door alle bestaande endpoints.
- [ ] Testset: geanonimiseerde golden receipts (AH-PDF-tekst + PLUS-OCR-JSON) en metric-tests.
- *Accept:* voor 100% van de bonnen geldt Σ regels = betaald (± €0,01); "Uitgegeven 2026" is op elke
  pagina identiek; alle tests groen.

### Fase 2: Productcatalogus en categorisatie v3 (1–2 weken)
- [ ] Normalisatie en clustering naar catalogusproducten met varianten (`pack_count`, `size`).
- [ ] Review-scherm voor groeperingsvoorstellen; samenvoegen en splitsen.
- [ ] Categorie op productniveau met een afgedwongen boom; keyword-engine op woordgrenzen; PLUS-woordenboek.
- [ ] Review-wachtrij categorisatie op € impact; één LLM-pad met kosteninschatting.
- *Accept:* Red Bull is 1 product met ≥6 varianten en een prijs per blik; Overig < 1% van uitgaven;
  0 regels zonder subcategorie; 0 inconsistente subcategorieën.

### Fase 3: Frontend-herbouw (1–2 weken)
- [ ] `base.html`, gedeelde CSS-tokens en componenten, JS-modules, responsive navigatie.
- [ ] Pagina's één voor één migreren: Dashboard → Bonnen → Bondetail → Producten → Categorieën → Budget → Jaren → Beheer.
- [ ] Gedeelde categoriepicker; facturenlijst met zoeken, filter (winkel/periode), sortering en paginering.
- [ ] Skeletons, foutstaten, 404, veilige rendering (geen `innerHTML` met data), Chart.js lokaal en gepind.
- *Accept:* geen horizontale scroll op 375 px; elke pagina bruikbaar op mobiel; bondetail laadt < 1 s;
  één logo en navigatie overal.

### Fase 4: Nieuwe functies (2–3 weken, in volgorde van waarde)
1. Productpagina met varianten, prijs per eenheid en AH tegen PLUS.
2. Importcentrum met upload, importlog en het corrigeren van bonnen.
3. Periodekiezer en winkelfilter overal.
4. Eén budgetmodel met tempo en waarschuwingen.
5. Inzichten v2: eenheidsprijsinflatie, shrinkflation, huismerkalternatieven, koopzegels.
6. Export en back-up/herstel.

### Fase 5: Afronding en kwaliteit (doorlopend)
- [ ] Backend in blueprints, repositories en services (incrementeel tijdens fase 1–4).
- [ ] CI (GitHub Actions): tests, `ruff`, eventueel `mypy` op `services/` en `importers/`.
- [ ] Legacy-endpoints, dode tabellen en dode parsercode verwijderen.
- [ ] Debugmodus via config, standaard uit; dependencies vastzetten.
- [ ] CLAUDE.md, API.md en REQUIREMENTS.md bijwerken naar het 3.0-model.

---

## 6. Definition of done voor 3.0

| Meetpunt | Nu | Doel 3.0 |
|---|---|---|
| Bonnen waarvan de regels exact op het betaalde totaal sluiten | PLUS 37/37, AH 17/179 | 216/216 |
| Gemiste AH-productregels | 237 (€679) | 0 |
| Verschillende waarden voor "uitgegeven 2026" in de UI | 3 | 1 |
| Unieke "producten" (namen) | 1.851 | catalogus met varianten (verwacht ±1.200) |
| Red Bull als product | 11+ | 1 (met varianten) |
| Overig | €597 (2,7%) | < 1% |
| Regels zonder subcategorie | 1.253 | 0 |
| Subcategorie buiten categorie | 109 | 0 (afgedwongen) |
| Pagina's bruikbaar op 375 px | 0 van 9 | 9 van 9 |
| Laadtijd bondetail | ±5 s | < 1 s |
| Geautomatiseerde tests | 20 | ≥ 120, inclusief metrics en parsers, in CI |

---

## 7. Open beslissingen

1. **AH-API-koppeling**: verwijderen, verbergen achter een instelling, of laten staan? (Advies: verbergen.)
2. **Productgroepering**: wil je één niveau (product met varianten) of twee (family "Energiedrank" ⊃
   product "Red Bull" ⊃ varianten)? (Advies: twee, de family optioneel.)
3. **Koopzegels**: tellen als uitgave of als sparen? (Advies: apart tonen, niet in "boodschappen".)
4. **LLM-gebruik** voor groeperen en categoriseren: welke sleutel en model, en welk maandbudget? (De
   huidige OpenAI-sleutel is ongeldig.)
5. **Frontend**: strikt vanilla blijven (CLAUDE.md), of een lichte laag zoals htmx of Alpine toestaan?
   (Advies: vanilla met ES-modules is ruim voldoende.)
6. **Volgorde**: fase 0 eerst los opleveren (snel kloppende cijfers), of direct in fase 1 meenemen?
   (Advies: fase 0 eerst, want die raakt je huidige inzichten direct.)
