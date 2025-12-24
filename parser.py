"""
PDF Parser for Albert Heijn invoices
Extracts invoice data and products from AH PDFs
"""

import os
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import pdfplumber

from database import (
    calculate_file_hash, invoice_exists, save_invoice, save_products,
    get_all_categories
)


def normalize_product_name(name: str) -> str:
    """
    Normaliseer productnaam door BONUS suffix te verwijderen.
    Dit zorgt ervoor dat 'Product X' en 'Product X BONUS' als hetzelfde product worden gezien.
    """
    # Strip whitespace
    name = name.strip()
    
    # Verwijder " BONUS" suffix (case-insensitive)
    if name.upper().endswith(' BONUS'):
        name = name[:-6].strip()
    
    return name


# Productcategorieën met uitgebreide keywords
CATEGORY_KEYWORDS = {
    "Zuivel & Eieren": [
        # Basis zuivel
        "melk", "yoghurt", "kaas", "boter", "room", "eieren", "ei ", "kwark",
        "creme fraiche", "slagroom", "cottage", "mascarpone", "ricotta",
        "zuivelspread", "kookzuivel", "sour cream", "griekse stijl",
        # Kaassoorten en -types
        "zaanlander", "geraspt", "plakken", "45+", "48+", "mozzarella",
        "pecorino", "parmigiano", "feta", "zaanse hoeve", "burrata", "brie",
        "geitenkaas", "camembert", "gorgonzola", "gruyere", "emmentaler",
        "grana padano", "parmareggio", "manchego", "streeckgenoten", "schrama",
        "belegen", "jong belegen", "extra belegen", "oud",
        # Plantaardige zuivel (valt ook onder zuivel)
        "alpro", "oatly", "mild & creamy",
        # Zuivel extra voor kassabonnen
        "margarine", "becel", "blue band", "vla", "kleintje vla", "custard",
    ],
    "Groente & Fruit": [
        # Groenten
        "tomaat", "tomaten", "appel", "peer", "banaan", "bananen", "aardappel", "sla",
        "rauwkost", "rauwkostsalade",
        "komkommer", "paprika", "wortel", "ui ", "uien", "knoflook", "champignon",
        "broccoli", "bloemkool", "spinazie", "prei", "courgette", "aubergine",
        "avocado", "mango", "ananas", "druiven", "sinaasappel", "citroen",
        "limoen", "aardbei", "bosbes", "frambo", "elstar", "peen", "roerbak",
        "snoepgroente", "trostomaten", "cherry", "roma", "julienne", "basilicum",
        "groente", "fruit", "dadels", "rozijn", "chiquita", "bimi", "sjalot",
        "gemengde salade", "salade erbij", "italiaanse mix", "salade mix",
        "lunchsalade", "caesar", "pompoen",
        # Verse kruiden (zijn groente)
        "peterselie", "bieslook", "koriander", "dille", "munt", "tijm", "oregano",
        "rozemarijn", "dragon", "kervel", "salie", "laurier", "bonenkruid",
        # Extra groenten
        "bosui", "lente-ui", "radijs", "rucola", "veldsla", "andijvie", "witlof",
        "rode kool", "witte kool", "savooiekool", "spitskool", "boerenkool",
        "snijbiet", "pak choi", "chinese kool", "ijsbergsla", "lollo", "radicchio",
        "venkel", "knolselderij", "selderij", "pastinaak", "bieten", "mais",
        "artisjok", "asperge", "zeekraal", "postelein", "shiitake", "oester",
        "sperziebonen", "tuinerwten", "doperwten", "sugarsnaps", "peultjes",
        "olijf", "taggiasca", "paddestoelen", "paddenstoel", "kappertjes",
        # Fruit
        "kersen", "pruimen", "abrikozen", "perzik", "nectarine", "melon", 
        "watermeloen", "kiwi", "granaatappel", "passievrucht", "lychee",
        "pitaya", "papaya", "kokosnoot", "vijgen", "grapefruit", "mandarijn",
        "clementine", "conference", "gala", "jonagold", "golden", "granny",
        # Schaal en verpakkingen
        "schaal", "tros", "doos",
        # Diepvries groenten
        "krieltjes",
    ],
    "Vlees & Vis": [
        # Vlees
        "kip", "varken", "rund", "gehakt", "filet", "worst", "bacon", "ham", 
        "kipfilet", "biefstuk", "schnitzel", "spek", "kalkoen", "lam", "scharrel", 
        "kipdij", "rosbief", "haricot", "ossenhaas", "entrecote", "ribeye",
        "slavink", "hamburger", "cordon bleu", "kipdrumstick", "kippenvleugel",
        "spareribs", "pulled pork", "rookvlees", "pastrami", "salami", "chorizo",
        "pancetta", "prosciutto", "carpaccio", "tartaar", "riblap", "sucadelapje",
        "stoofvlees", "sticky chicken", "shoarma", "mortadella", "fuet", "besos",
        # Vis en zeevruchten
        "zalm", "tonijn", "garnaal", "vis", "kabeljauw", "schelvis", "tilapia",
        "pangasius", "forel", "makreel", "haring", "kibbeling", "lekkerbek",
        "visstick", "visfilet", "rivierkreeft", "kreeft", "krab", "mosselen",
        "oesters", "inktvis", "calamares", "scampi", "coquilles",
        "gerookte", "warmgerookte", "warmgerookt", "garnalenkroket", "tempura",
        "garnalen rauw", "gepeld", "ansjovis", "princes",
        # Plantaardige vlees alternatieven
        "plantaardige balletjes", "plantaardige burger", "plantaardig gehakt",
    ],
    "Brood & Bakkerij": [
        # Broodsoorten
        "brood", "croissant", "stok", "pistolet", "bol", "baguette", "wrap",
        "pita", "toast", "beschuit", "cracker", "wasa", "volkoren", "waldkorn",
        "meergranen", "rogge", "spelt", "haver", "zuurdesem", "ciabatta",
        "focaccia", "naan", "chapati", "tortilla", "flatbread", "knäckebröd",
        "bagel", "muffin", "donut", "croffle", "pannenkoek", "poffertjes",
        "pizzabodem", "rosti", "boulogne", "pains",
        # Bakkerijproducten
        "taart", "cake", "gebak", "appelflap", "saucijzenbroodje", "kaasbroodje",
    ],
    "Pasta, Rijst & Granen": [
        # Pasta en noodles
        "pasta", "penne", "spaghetti", "macaroni", "noodle", "noedel", "lasagne", "fusilli",
        "tagliatelle", "fettuccine", "linguine", "rigatoni", "farfalle", "orzo",
        "tortellini", "ravioli", "gnocchi", "cappelletti", "wok ", "udon",
        "orecchiette", "de cecco", "barilla", "sfoglie",
        # Rijst en granen  
        "rijst", "couscous", "havermout", "muesli", "quinoa", "bulgur",
        "polenta", "risotto", "basmati", "jasmine", "pandan", "arborio",
        "sesamzaad", "sesam",
        # Peulvruchten (logisch bij granen)
        "kikkererwten", "zwarte bonen", "witte bonen", "bruine bonen", "linzen",
        "sojabonen", "kapucijners", "spliterwten", "tuinbonen", "limabonen",
        "kidneybonen", "cannellini", "borlotti", "lima bonen", "edamame",
        "peulvruchtenmix", "peulvruchten",
    ],
    "Dranken": [
        # Frisdrank en sappen
        "water", "cola", "fris", "sap", "limonade", "ice tea", "tonic", "bitter lemon",
        "cassis", "grenadine", "appelsap", "sinaasappelsap", "jus d'orange",
        "pellegrino", "spa ", "evian", "perrier", "mineraalwater",
        # Warme dranken
        "thee", "koffie", "espresso", "cappuccino", "latte", "cacao", "chocomel",
        "capsules", "pads", "lungo", "ristretto", "perla", "pukka",
        # Alcoholisch
        "bier", "wijn", "prosecco", "champagne", "cava", "abdij", "blond",
        "leffe", "heineken", "grolsch", "amstel", "hertog",
        # Energy en sport
        "energy", "sportdrank", "vitamin well", "red bull", "monster", "energy drink",
        # Plantaardige dranken
        "haverdrink", "sojadrink", "amandeldrink", "rijstdrink", "kokosdrink",
        "havermelk", "sojamelk", "amandelmelk",
        # Wijn soorten
        "sauvignon", "chardonnay", "merlot", "cabernet", "pinot", "rioja",
        "chianti", "malbec", "shiraz", "riesling", "gewurztraminer", "moscato",
        "blanc", "rouge", "rosé", "rose",
    ],
    "Sauzen & Specerijen": [
        # Sauzen
        "saus", "ketchup", "mayo", "mayonaise", "mosterd", "pesto", "dressing",
        "vinaigrette", "aioli", "tzatziki", "hummus", "guacamole", "salsa",
        "tapenade", "sambal", "sriracha", "tabasco", "sojasaus", "ketjap",
        "teriyaki", "hoisin", "oestersaus", "vissaus", "worcestershire",
        # Olie en azijn
        "olie", "azijn", "balsamico", "olijfolie", "zonnebloemolie",
        # Kruiden en specerijen
        "peper", "zout", "kruiden", "curry", "paprika poeder", "kaneel",
        "nootmuskaat", "komijn", "kurkuma", "gember", "kardamom", "kruidnagel",
        "steranijs", "korianderzaad", "venkelzaad", "kerrie", "garam masala",
        "ras el hanout", "za'atar", "sumak", "cayenne",
        # Tomatenprodukten
        "tomatenpuree", "puree", "passata", "pelati",
        # Overige sauzen
        "honing", "cuisine", "paturain", "bâton", "smaakmakermix", "jus",
        "fond", "bouillon", "roux",
    ],
    "Snacks & Zoetwaren": [
        # Chips en zout
        "chips", "popcorn", "pretzels", "pinda", "borrel", "dipsaus", "zeewier",
        "krokant", "kroketjes", "cheese bites", "kaaskoekje",
        # Noten  
        "nootje", "noten", "walnoot", "amandel", "cashew", "notenmix", 
        "hazelnoot", "pistache", "macadamia", "pecannoot", "paranoot",
        "terra noten", "terra walnoot", "terra amandel", "pijnboompitten",
        "chiazaad", "zonnebloempitten", "pompoenpitten", "lijnzaad",
        "kokossnippers", "kokosrasp",
        # Chocolade en snoep
        "chocola", "chocolade", "koek", "snoep", "drop", "kauwgom",
        "tony", "reep", "biscuit", "sticks", "cookies", "bonbon",
        "marshmallow", "lolly", "zuurtje", "winegums", "m&m",
        "stroopwafel", "slofje", "slofjes", "musket", "lettertje", "macarons",
        "paashaas", "gips", "knettersuiker", "tompoucen", "tompouce",
        "nougatine", "nougat", "wafel",
        # Gebak en dessert
        "roomijs", "ijs ", "pavlova", "fruit mix", "mousse",
        "tiramisu", "panna cotta", "cheesecake", "brownie",
    ],
    "Huishouden": [
        # Schoonmaak
        "wc", "schoon", "afwas", "wasmiddel", "waspoeder", "wasverzachter",
        "doekje", "sponge", "bezem", "dweil", "allesreiniger", "bleek",
        "ontkalker", "vaatwas", "glansspoelmiddel", "glasreiniger",
        "glassex", "mullrose", "azijn", "schoonmaakazijn", "spray",
        "pedaalemmerzak", "emmerzak",
        # Papier en verpakking
        "papier", "toiletpapier", "keukenrol", "servet", "tissue", "tempo",
        "folie", "aluminiumfolie", "huishoudfolie", "bakpapier", "vershoudfolie",
        "zak", "vuilnis", "afvalzak", "diepvrieszak",
        # Keuken hulpmiddelen
        "cocktail prikker", "prikker", "sateprikker", "bakje", "doeboek",
        "toetenvegers",
        # Overig huishouden
        "batterij", "lamp", "kaars", "lucifer", "aansteker",
    ],
    "Persoonlijke Verzorging": [
        # Haar
        "shampoo", "conditioner", "haarlak", "gel", "mousse", "haarverf",
        "magic retouch", "l'oréal", "loreal",
        # Lichaam
        "douche", "zeep", "douchegel", "bodylotion", "bodycrème", "deodorant",
        "deo", "deoleen", "satin", "anti-transpirant", "creme", "lotion",
        "hand soap", "handzeep", "hygiene", "care mint", "refill",
        # Mondverzorging
        "tandpasta", "tandenb", "mondwater", "flosdraad", "elmex", "oral",
        # Scheren
        "scheerm", "scheermes", "scheergel", "aftershave",
        # Overig
        "make", "mascara", "lippenstift", "nagellak", "wattenschijf", "wattenstaaf",
        "maandverband", "tampon", "condoom",
    ],
    "Verpakking & Statiegeld": [
        "statiegeld", "krat", "fles", "blik", "tasje", "verpakking", "emballage",
    ],
    "Bezorgkosten": [
        "bezorg", "aflever", "service",
    ],
    "Abonnementen": [
        "premium", "abonnement", "lidmaatschap", "bundel",
    ],
}


def categorize_product(product_name: str) -> str:
    """Categoriseer een product op basis van keywords met prioriteit"""
    # Normaliseer eerst (BONUS strippen etc.)
    normalized = normalize_product_name(product_name)
    product_lower = normalized.lower()
    
    # Speciale gevallen eerst (hogere prioriteit)
    # Dit voorkomt conflicten tussen keywords
    priority_rules = [
        # Chips (inclusief alle types zoals lentil chips, tortilla chips) = Snacks
        (["chips", "lentil chips", "tortilla chips", "nacho chips", "proper sea salt", "proper paprika", "proper sweet"], "Snacks & Zoetwaren"),
        # Muesli/granola producten = Pasta, Rijst & Granen (ontbijtgranen)
        (["muesli", "granola", "cruesli"], "Pasta, Rijst & Granen"),
        # Azijn in schoonmaakcontext = Huishouden
        (["schoonmaakazijn", "mullrose", "vanish", "vlekverwijderaar"], "Huishouden"),
        # Pindakaas, siroop = Sauzen & Specerijen
        (["pindakaas", "ahornsiroop", "maple", "harissa", "souq"], "Sauzen & Specerijen"),
        # Haverdrink/plantaardige melk = Dranken
        (["haverdrink", "sojadrink", "amandeldrink", "kokosdrink", "fever-tree", "ginger beer", "tonic"], "Dranken"),
        # Chocolade producten = Snacks & Zoetwaren (niet dranken)
        (["chocolade", "chocola", "chocoladefiguurtjes", "chocolade druppels"], "Snacks & Zoetwaren"),
        # Cashewnoten, noten = Snacks & Zoetwaren
        (["cashewnoten", "cashew", "noten"], "Snacks & Zoetwaren"),
        # Bulgur, quinoa = Pasta, Rijst & Granen
        (["bulgur", "quinoa", "couscous"], "Pasta, Rijst & Granen"),
        # Vis producten met olie = Vlees & Vis
        (["ansjovis", "tonijn", "sardine"], "Vlees & Vis"),
        # Thee producten = Dranken
        (["pukka", "thee", "chamomile", "night time", "after dinner"], "Dranken"),
        # Kant-en-klaar vlees/vis producten
        (["gyoza", "biltong", "knaks", "nduja", "cha siu"], "Vlees & Vis"),
        # Kauwgom, mints, snoep
        (["smint", "sportlife", "gums", "mints", "haribo", "liga", "pappadum", "vlokken", "venz"], "Snacks & Zoetwaren"),
        # Persoonlijke verzorging merken
        (["etos", "lenzen", "maandlenzen", "vloeistof", "azaron"], "Persoonlijke Verzorging"),
        # Pizza producten
        (["pizza", "pinsa", "pizzetta"], "Brood & Bakkerij"),
        # Ramen noodles
        (["ramen", "brilliant broth", "itsu"], "Pasta, Rijst & Granen"),
        # Meer dranken
        (["rivella", "fanta", "dr pepper", "hi-five", "charitea", "mojo", "maté", "fentimans", "elderflower", "coolbest"], "Dranken"),
        # Meer snacks
        (["mars", "ijsrepen", "klene", "red band", "stophoest", "ben & jerry", "smoeltjes", "suikerschelpen", "mentos", "gum"], "Snacks & Zoetwaren"),
        # Meer huishouden
        (["hg ", "schoonmaak", "kookplaatreiniger", "beeldschermreiniger", "sportkleding", "tafelkleed"], "Huishouden"),
        # Meer sauzen
        (["chili", "chilli", "go-tan", "mazzetti", "condimento"], "Sauzen & Specerijen"),
        # Meer persoonlijke verzorging
        (["always", "inlegkruisje", "dailies"], "Persoonlijke Verzorging"),
        # Bakkerij/deeg producten
        (["bladerdeeg", "filo", "easy bakery", "gist", "droge gist"], "Brood & Bakkerij"),
        # Plantaardige vlees/vis
        (["vivera", "plantaardige chicken", "tenders"], "Vlees & Vis"),
        # Zuivel
        (["eru", "balans", "oat drink", "natrue"], "Zuivel & Eieren"),
        # Saus/curry kits
        (["fairtrade original", "butter chicken", "curry kit"], "Sauzen & Specerijen"),
        # Olie
        (["coconut oil", "kokosolie", "biofan"], "Sauzen & Specerijen"),
        # Bakpudding
        (["kloppudding", "dr. oetker", "pudding"], "Snacks & Zoetwaren"),
    ]
    
    for keywords, category in priority_rules:
        for keyword in keywords:
            if keyword in product_lower:
                return category
    
    # Normale keyword matching
    for category, keywords in CATEGORY_KEYWORDS.items():
        for keyword in keywords:
            if keyword in product_lower:
                return category
    
    return "Overig"


# Subcategorie keywords per hoofdcategorie
SUBCATEGORY_KEYWORDS = {
    "Zuivel & Eieren": {
        "Melk": ["melk", "halfvol", "volle melk", "magere melk", "lactosevrij"],
        "Kaas": ["kaas", "goudse", "gruyere", "pecorino", "mozzarella", "feta", "brie", "camembert", "parmesan", "manchego"],
        "Yoghurt & Kwark": ["yoghurt", "kwark", "griekse", "skyr", "cottage"],
        "Boter & Margarine": ["boter", "margarine", "becel", "blue band", "roomboter"],
        "Eieren": ["eieren", "ei ", "scharreleieren", "bio eieren"],
        "Room & Vla": ["slagroom", "room", "vla", "custard", "creme fraiche"],
        "Plantaardig": ["alpro", "oatly", "mild & creamy", "plantaardig"],
    },
    "Groente & Fruit": {
        "Groente": ["tomaat", "komkommer", "paprika", "wortel", "broccoli", "spinazie", "sla", "kool", "prei", "courgette", "champignon", "ui", "knoflook"],
        "Fruit": ["appel", "peer", "banaan", "druiven", "aardbei", "mango", "ananas", "sinaasappel", "citroen", "kiwi"],
        "Kruiden": ["basilicum", "peterselie", "bieslook", "koriander", "munt", "dille", "rozemarijn", "tijm"],
        "Peulvruchten": ["bonen", "kikkererwten", "linzen", "erwten", "peulvruchten"],
        "Salades": ["salade", "sla", "rucola", "rauwkost"],
        "Aardappelen": ["aardappel", "krieltjes", "puree", "friet"],
    },
    "Vlees & Vis": {
        "Kip": ["kip", "kipfilet", "kipgehakt", "kipdij", "drumstick", "vleugel"],
        "Rund & Varken": ["rund", "varken", "gehakt", "biefstuk", "schnitzel", "sparerib", "bacon", "spek"],
        "Vis": ["zalm", "tonijn", "kabeljauw", "garnaal", "vis", "forel", "makreel"],
        "Vleeswaren": ["ham", "salami", "filet americain", "rosbief", "chorizo", "pancetta", "prosciutto"],
        "Vegetarisch & Vegan": ["vegetarisch", "vegan", "plantaardig", "tofu", "tempeh", "vivera"],
        "Wild & Gevogelte": ["kalkoen", "eend", "konijn", "wild"],
    },
    "Dranken": {
        "Koffie": ["koffie", "espresso", "lungo", "nespresso", "cappuccino"],
        "Thee": ["thee", "tea", "pukka", "pickwick", "lipton"],
        "Frisdrank": ["cola", "fanta", "sprite", "pepsi", "sinas", "frisdrank", "dr pepper"],
        "Sap & Smoothies": ["sap", "jus", "smoothie", "appelsap", "sinaasappelsap"],
        "Bier": ["bier", "leffe", "heineken", "amstel", "jupiler", "hertog jan", "ipa", "pils"],
        "Wijn": ["wijn", "sauvignon", "chardonnay", "merlot", "pinot", "prosecco", "champagne"],
        "Sterke Drank": ["whisky", "vodka", "rum", "gin", "jenever", "likeur"],
        "Water": ["water", "spa", "chaudfontaine", "bar le duc"],
        "Sportdranken": ["red bull", "energy", "sportdrank", "aquarius", "aa drink"],
        "Plantaardige Dranken": ["haverdrink", "sojadrink", "amandeldrink", "kokosdrink"],
    },
    "Snacks & Zoetwaren": {
        "Chips & Noten": ["chips", "noten", "pinda", "cashew", "pistache", "borrelnoot"],
        "Snoep": ["snoep", "drop", "winegum", "lolly", "haribo", "smint", "mentos"],
        "Chocolade": ["chocola", "bonbon", "praline", "reep", "tony", "milka"],
        "Koekjes": ["koek", "biscuit", "speculaas", "stroopwafel", "bastogne"],
        "Ijs": ["ijs", "magnum", "cornetto", "ben & jerry"],
    },
    "Brood & Bakkerij": {
        "Brood": ["brood", "boterham", "volkoren", "wit brood", "meergranen", "pistolet"],
        "Gebak & Taart": ["taart", "gebak", "croissant", "appeltaart", "tompouce"],
        "Ontbijtproducten": ["muesli", "havermout", "cornflakes", "cruesli", "granola"],
        "Crackers & Toast": ["cracker", "toast", "beschuit", "knäckebröd"],
    },
    "Sauzen & Specerijen": {
        "Sauzen": ["saus", "mayonaise", "ketchup", "mosterd", "aioli"],
        "Kruiden & Specerijen": ["kruiden", "peper", "zout", "paprikapoeder", "komijn", "kerrie"],
        "Olie & Azijn": ["olie", "olijfolie", "azijn", "balsamico"],
        "Pasta Sauzen": ["pesto", "pastasaus", "bolognese", "arrabiata"],
    },
}


def determine_subcategory(product_name: str, main_category: str) -> str:
    """Bepaal de subcategorie van een product binnen een hoofdcategorie"""
    if main_category not in SUBCATEGORY_KEYWORDS:
        return None
    
    product_lower = product_name.lower()
    subcats = SUBCATEGORY_KEYWORDS[main_category]
    
    for subcat_name, keywords in subcats.items():
        for keyword in keywords:
            if keyword in product_lower:
                return subcat_name
    
    return None


def categorize_product_full(product_name: str) -> Dict:
    """Categoriseer een product met hoofdcategorie en subcategorie"""
    category = categorize_product(product_name)
    subcategory = determine_subcategory(product_name, category)
    
    return {
        "category": category,
        "subcategory": subcategory
    }


def parse_dutch_date(date_str: str) -> str:
    """Parse a Dutch date string to ISO format"""
    dutch_months = {
        "januari": 1, "februari": 2, "maart": 3, "april": 4,
        "mei": 5, "juni": 6, "juli": 7, "augustus": 8,
        "september": 9, "oktober": 10, "november": 11, "december": 12
    }
    
    parts = date_str.split()
    if len(parts) == 3:
        day = int(parts[0])
        month = dutch_months.get(parts[1].lower(), 1)
        year = int(parts[2])
        return datetime(year, month, day).strftime("%Y-%m-%d")
    
    return None


def extract_products_from_text(text: str) -> List[Dict]:
    """Extract products from invoice text using a universal pattern"""
    products = []
    seen_products = set()
    
    # Generiek pattern dat ALLE productregels vangt:
    # [Productnaam] [Aantal] [9%|21%|Geen] [Excl btw] [Btw-bedrag] [Incl btw]
    # De productnaam begint met een hoofdletter en bevat letters, spaties, cijfers, etc.
    # Het eindigt wanneer we het patroon: [getal] [btw%] [bedrag] [bedrag] [bedrag] vinden
    
    product_pattern = re.compile(
        r"^([A-Z][A-Za-z0-9àáâãäåæçèéêëìíîïðñòóôõöøùúûüýþÿÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞß'&\s\-\+\.%]+?)\s+(\d+)\s+(9%|21%|Geen)\s+([\d,]+)\s+([\d,]+)\s+([\d,]+)$",
        re.MULTILINE
    )
    
    # Zoek alle matches
    for match in product_pattern.finditer(text):
        original_name = match.group(1).strip()
        quantity = int(match.group(2))
        btw = match.group(3)
        price_incl = float(match.group(6).replace(",", "."))
        
        # Skip ongewenste regels
        skip_patterns = [
            "statiegeld", "retour", "klapkrat", "bezorgkosten", "verpakkingsmateriaal",
            "plastic verpakking", "koopzegels", "pagina ", "datum ", "factuurnummer",
            "debiteurnummer", "bestelling", "afleverdatum", "bezorgadres", "totaal",
            "boodschappen", "alle bedragen", "vragen over"
        ]
        
        name_lower = original_name.lower()
        if any(skip in name_lower for skip in skip_patterns):
            continue
        
        # Skip regels die te kort zijn (waarschijnlijk geen echte producten)
        if len(original_name) < 3:
            continue
            
        # Skip regels met alleen cijfers
        if original_name.replace(" ", "").isdigit():
            continue
        
        # Normaliseer de productnaam (BONUS strippen etc.)
        display_name = normalize_product_name(original_name)
        
        # Skip duplicates (gebaseerd op genormaliseerde naam + prijs)
        product_key = f"{display_name}_{quantity}_{price_incl}"
        if product_key in seen_products:
            continue
        seen_products.add(product_key)
        
        category = categorize_product(original_name)
        subcategory = determine_subcategory(original_name, category)
        
        products.append({
            "name": display_name,
            "original_name_raw": original_name,
            "quantity": quantity,
            "btw": btw,
            "price": price_incl,
            "category": category,
            "subcategory": subcategory
        })
    
    return products


def parse_invoice(pdf_path: str) -> Tuple[Dict, List[Dict], str]:
    """
    Parse een AH factuur PDF en extract alle relevante data
    Returns: (invoice_data, products, raw_text)
    """
    result = {
        "filename": os.path.basename(pdf_path),
        "file_hash": calculate_file_hash(pdf_path),
        "date": None,
        "invoice_number": None,
        "total": 0,
        "savings": 0
    }
    
    products = []
    raw_text = ""
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    raw_text += text + "\n"
            
            # Extract datum
            date_match = re.search(r"Datum\s+(\d{1,2}\s+\w+\s+\d{4})", raw_text)
            if date_match:
                result["date"] = parse_dutch_date(date_match.group(1))
            
            # Extract factuurnummer
            inv_match = re.search(r"Factuurnummer\s+(\S+)", raw_text)
            if inv_match:
                result["invoice_number"] = inv_match.group(1)
            
            # Extract totaal
            total_match = re.search(r"Totaal inclusief btw\s+([\d,]+)", raw_text)
            if total_match:
                result["total"] = float(total_match.group(1).replace(",", "."))
            
            # Extract korting
            savings_match = re.search(r"Uw voordeel\s+([\d,]+)", raw_text)
            if savings_match:
                result["savings"] = float(savings_match.group(1).replace(",", "."))
            
            # Extract producten
            products = extract_products_from_text(raw_text)
    
    except Exception as e:
        print(f"Error parsing {pdf_path}: {e}")
    
    return result, products, raw_text


def import_invoice(pdf_path: str, force: bool = False) -> Dict:
    """
    Import a single invoice into the database
    Returns status dict with success/skip/error info
    """
    result = {
        "filename": os.path.basename(pdf_path),
        "status": "unknown",
        "message": "",
        "invoice_id": None,
        "products_count": 0
    }
    
    try:
        # Check for duplicate
        file_hash = calculate_file_hash(pdf_path)
        
        if invoice_exists(file_hash) and not force:
            result["status"] = "skipped"
            result["message"] = "Invoice already exists in database"
            return result
        
        # Parse the PDF
        invoice_data, products, raw_text = parse_invoice(pdf_path)
        invoice_data["raw_text"] = raw_text
        
        if not invoice_data["date"]:
            result["status"] = "error"
            result["message"] = "Could not extract date from invoice"
            return result
        
        # Save to database
        invoice_id = save_invoice(invoice_data)
        save_products(invoice_id, products)
        
        result["status"] = "success"
        result["message"] = f"Imported {len(products)} products"
        result["invoice_id"] = invoice_id
        result["products_count"] = len(products)
        result["date"] = invoice_data["date"]
        result["total"] = invoice_data["total"]
        
    except Exception as e:
        result["status"] = "error"
        result["message"] = str(e)
    
    return result


def import_all_invoices(directory: str = "data/invoices", force: bool = False) -> Dict:
    """
    Import all invoices from a directory
    Returns summary of import results
    """
    invoice_dir = Path(directory)
    
    if not invoice_dir.exists():
        return {
            "status": "error",
            "message": f"Directory {directory} does not exist",
            "results": []
        }
    
    results = []
    success_count = 0
    skip_count = 0
    error_count = 0
    
    for pdf_file in sorted(invoice_dir.glob("*.pdf")):
        result = import_invoice(str(pdf_file), force=force)
        results.append(result)
        
        if result["status"] == "success":
            success_count += 1
        elif result["status"] == "skipped":
            skip_count += 1
        else:
            error_count += 1
    
    return {
        "status": "completed",
        "total": len(results),
        "success": success_count,
        "skipped": skip_count,
        "errors": error_count,
        "results": results
    }


def add_keyword_to_category(category: str, keyword: str) -> Dict:
    """
    Add a keyword to a category.
    Note: This modifies the in-memory CATEGORY_KEYWORDS but doesn't persist to file.
    """
    global CATEGORY_KEYWORDS
    
    keyword = keyword.lower().strip()
    if not keyword:
        return {"success": False, "error": "Keyword cannot be empty"}
    
    if category not in CATEGORY_KEYWORDS:
        return {"success": False, "error": f"Category '{category}' not found"}
    
    if keyword in CATEGORY_KEYWORDS[category]:
        return {"success": False, "error": f"Keyword '{keyword}' already exists in {category}"}
    
    CATEGORY_KEYWORDS[category].append(keyword)
    return {"success": True}


def remove_keyword_from_category(category: str, keyword: str) -> Dict:
    """
    Remove a keyword from a category.
    Note: This modifies the in-memory CATEGORY_KEYWORDS but doesn't persist to file.
    """
    global CATEGORY_KEYWORDS
    
    keyword = keyword.lower().strip()
    
    if category not in CATEGORY_KEYWORDS:
        return {"success": False, "error": f"Category '{category}' not found"}
    
    if keyword not in CATEGORY_KEYWORDS[category]:
        return {"success": False, "error": f"Keyword '{keyword}' not found in {category}"}
    
    CATEGORY_KEYWORDS[category].remove(keyword)
    return {"success": True}


def get_all_keywords() -> Dict[str, List[str]]:
    """Get all category keywords"""
    return CATEGORY_KEYWORDS.copy()


if __name__ == "__main__":
    # Test parsing
    from database import init_database
    
    init_database()
    
    results = import_all_invoices()
    print(f"Import completed:")
    print(f"  Success: {results['success']}")
    print(f"  Skipped: {results['skipped']}")
    print(f"  Errors: {results['errors']}")

