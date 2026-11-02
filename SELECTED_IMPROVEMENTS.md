# 50 Geselecteerde Verbeteringen voor Appestat

## 🖱️ Multi-select & Bulk Acties (8)
1. **Factuurdetails: Producten in bulk kunnen selecteren en categoriseren**
2. **Factuurdetails: Meerdere producten tegelijk "valideren"**
3. **Categorieën: Bulk-verplaatsen van producten van de ene categorie naar de andere**
4. **Zoeken: Bulk-actie om geselecteerde producten te verbergen uit analyses**
5. **Factuurdetails: Snel-selectie knoppen voor "Selecteer alle 'Overig' producten"**
6. **Factuurdetails: Keyboard modifiers (Shift+Click) voor snellere selectie**
7. **Invoices: Bulk-verwijderen van dubbele of foutieve facturen**
8. **Categorieën: Meerdere producten tegelijk toewijzen aan een subcategorie**

## 🧠 State Management & UX Flow (8)
9. **Onthouden selectie: Behoud actieve categorie-focus na verplaatsen product**
10. **Scroll-positie: Voorkom springen naar boven na AJAX-update**
11. **Focusbehoud: Na aanpassen subcategorie focus houden op volgende rij**
12. **Filters onthouden: Zoektermen bewaren bij navigeren**
13. **Auto-focus: Direct focus op zoekveld bij openen Producten-pagina**
14. **Standaard jaar: Onthoud laatst gekozen jaar over sessies**
15. **Undo-actie: "Ongedaan maken" melding na bulk-categorisatie**
16. **Loading states: Skeleton loaders bij grote datasets**

## ⚖️ Feature Parity (6)
17. **Icons: Subcategorieën eigen icoon kunnen hebben**
18. **Keywords: Keywords koppelen aan subcategorieën voor auto-parsing**
19. **Stats-interactie: Klikken op subcategorie-bars filtert productlijst**
20. **Export: Data-export op subcategorie-niveau**
21. **Budgeting: Budgetten instellen op subcategorie-niveau**
22. **Ranking: "Top Producten" lijstjes per subcategorie**

## 📊 Dashboard & Analyse (8)
23. **Merk-loyaliteit: Percentage A-merk vs Huismerk visualiseren**
24. **Dag-analyse: Visualiseer uitgaven per dag van de week**
25. **Besparings-tracker: Besparing door switch naar huismerk**
26. **Bonus-focus: Totaaloverzicht ontvangen Bonus-korting**
27. **Jaar-vergelijking: Overlay-grafiek dit jaar vs vorig jaar**
28. **Voorspelling: Schatting totale uitgaven deze maand**
29. **Inflatie-calculator: "Wat kost bon van 2022 vandaag?"**
30. **Gezondheidsscore: Ratio verse producten vs bewerkte snacks**

## 🛠️ Data & Parsing (6)
31. **Dubbele facturen: UI indicator voor "Dubbel" gemarkeerde facturen**
32. **Statiegeld detection: Betere verwerking statiegeld**
33. **Product-normalisatie: Varianten automatisch voorstellen om te mergen**
34. **Reparse knop: Per factuur opnieuw door parser halen**
35. **Prijstrend: Mini-sparkline van prijs-ontwikkeling in product-overzicht**
36. **Missing data alert: Melding bij groot gat tussen factuur-data**

## 🎨 UI & Esthetiek (6)
37. **Dark Mode: Volledige donkere modus**
38. **Micro-animaties: Subtiele feedback bij valideren**
39. **Sticky totals: Totaalbedrag altijd in beeld bij lange bonnen**
40. **Progress bars: "% gecategoriseerd" visualiseren**
41. **Tooltips: Uitleg bij complexe termen**
42. **Responsive Tables: Betere kolom-prioritering op mobiel**

## 🐞 Bugfixes & Kleine Ergernissen (5)
43. **Paginering: Lange lijsten opbreken (>2000 items)**
44. **Lege categorieën: Optie om te verbergen in Dashboard**
45. **Foutieve prijzen: Voorkomen dat "0.00" gemiddelden vervuilt**
46. **Modal scroll: Scroll-lock op body bij open modal**
47. **Search highlights: Zoektermen markeren in resultaten**

## 🚀 Technical Debt (3)
48. **Cache invalidatie: Slimmer flushen na imports**
49. **Database backup: Eén-klik knop om SQLite DB te downloaden**
50. **Async imports: Facturen op achtergrond importeren**

---

## Prioriteitsindeling

### 🔥 Hoge Prioriteit (Quick Wins)
- Items 1-8: Multi-select functionaliteit (grote productiviteitswinst)
- Items 9-16: State management (directe UX verbetering)
- Items 43-47: Bugfixes (stabiliteit)

### 🎯 Middelhoge Prioriteit (Features)
- Items 17-22: Feature parity (consistentie)
- Items 23-30: Dashboard analyse (toegevoegde waarde)
- Items 37-42: UI verbeteringen (gebruikersbeleving)

### 🔧 Lagere Prioriteit (Foundation)
- Items 31-36: Data & parsing (kwaliteit)
- Items 48-50: Technical debt (onderhoudbaarheid)

## Implementatie Suggestie

### Fase 1: Foundation (Weken 1-2)
- Bugfixes (43-47)
- State management basis (9-13)
- Technical: Async imports (50)

### Fase 2: Productiviteit (Weken 3-4)
- Multi-select core (1-5)
- Keyboard modifiers (6)
- Undo functionaliteit (15)

### Fase 3: Features (Weken 5-6)
- Dashboard analyse (23-28)
- Subcategorie parity (17-21)
- Data parsing verbeteringen (31-34)

### Fase 4: Polish (Weken 7-8)
- UI verbeteringen (37-42)
- Remaining multi-select (7-8)
- Advanced features (22, 29-30, 35-36, 48-49)
