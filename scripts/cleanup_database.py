#!/usr/bin/env python3
"""
Complete database cleanup script.
Consolidates categories, fixes keywords, and recategorizes all products.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from database import get_connection

# Definitieve categorieën (gebaseerd op AH.nl structuur)
# Format: naam -> (icon, color)
FINAL_CATEGORIES = {
    "Groente & Aardappelen": ("🥦", "#7CB518"),
    "Fruit & Verse sappen": ("🍎", "#E63946"),
    "Maaltijden & Salades": ("🥗", "#2A9D8F"),
    "Vlees": ("🥩", "#C1121F"),
    "Vis": ("🐟", "#0077B6"),
    "Vegetarisch & Vegan": ("🌱", "#52B788"),
    "Vleeswaren": ("🥓", "#BC6C25"),
    "Kaas": ("🧀", "#FFBA08"),
    "Zuivel & Eieren": ("🥛", "#00ADE6"),
    "Bakkerij": ("🍞", "#D4A574"),
    "Borrel, Chips & Snacks": ("🥜", "#F4A261"),
    "Pasta, Rijst & Wereldkeuken": ("🍝", "#FFB347"),
    "Soepen, Sauzen & Kruiden": ("🫙", "#FF7043"),
    "Koek, Snoep & Chocolade": ("🍫", "#AB47BC"),
    "Ontbijtgranen & Beleg": ("🥣", "#FFC300"),
    "Tussendoortjes": ("🍪", "#E9C46A"),
    "Diepvries": ("❄️", "#48CAE4"),
    "Koffie & Thee": ("☕", "#6F4E37"),
    "Frisdrank & Water": ("🥤", "#4FC3F7"),
    "Bier, Wijn & Aperitieven": ("🍷", "#722F37"),
    "Drogisterij": ("🧴", "#F48FB1"),
    "Huishouden": ("🧹", "#78909C"),
    "Baby & Kind": ("👶", "#90CAF9"),
    "Huisdier": ("🐕", "#A1887F"),
    "Verpakking & Statiegeld": ("📦", "#90A4AE"),
    "Bezorgkosten": ("🚚", "#607D8B"),
    "Overig": ("📋", "#9E9E9E"),
}

# Mapping van oude categorieën naar nieuwe
CATEGORY_MAPPING = {
    # Directe mappings (oude naam -> nieuwe naam)
    "Groente & Fruit": "Fruit & Verse sappen",
    "Brood & Bakkerij": "Bakkerij",
    "Snacks & Zoetwaren": "Koek, Snoep & Chocolade",
    "Sauzen & Specerijen": "Soepen, Sauzen & Kruiden",
    "Soepen, Sauzen, Kruiden & Olie": "Soepen, Sauzen & Kruiden",
    "Pasta, Rijst & Granen": "Pasta, Rijst & Wereldkeuken",
    "Persoonlijke Verzorging": "Drogisterij",
    "Dranken": "Frisdrank & Water",
    "Frisdrank, Sappen & Water": "Frisdrank & Water",
    "Vlees & Vis": "Vlees",
    "Abonnementen": "Bezorgkosten",
    
    # Categorieën die al goed zijn (identity mapping)
    "Groente & Aardappelen": "Groente & Aardappelen",
    "Fruit & Verse sappen": "Fruit & Verse sappen",
    "Maaltijden & Salades": "Maaltijden & Salades",
    "Vlees": "Vlees",
    "Vis": "Vis",
    "Vegetarisch & Vegan": "Vegetarisch & Vegan",
    "Vleeswaren": "Vleeswaren",
    "Kaas": "Kaas",
    "Zuivel & Eieren": "Zuivel & Eieren",
    "Bakkerij": "Bakkerij",
    "Borrel, Chips & Snacks": "Borrel, Chips & Snacks",
    "Pasta, Rijst & Wereldkeuken": "Pasta, Rijst & Wereldkeuken",
    "Koek, Snoep & Chocolade": "Koek, Snoep & Chocolade",
    "Ontbijtgranen & Beleg": "Ontbijtgranen & Beleg",
    "Tussendoortjes": "Tussendoortjes",
    "Diepvries": "Diepvries",
    "Koffie & Thee": "Koffie & Thee",
    "Bier, Wijn & Aperitieven": "Bier, Wijn & Aperitieven",
    "Drogisterij": "Drogisterij",
    "Huishouden": "Huishouden",
    "Baby & Kind": "Baby & Kind",
    "Huisdier": "Huisdier",
    "Verpakking & Statiegeld": "Verpakking & Statiegeld",
    "Bezorgkosten": "Bezorgkosten",
    "Overig": "Overig",
}


def cleanup_categories():
    """Remove old categories and create clean new ones."""
    print("\n🗑️  Cleaning up categories...")
    
    conn = get_connection()
    cursor = conn.cursor()
    
    # Step 1: Get current category IDs for mapping
    cursor.execute("SELECT id, name FROM categories")
    old_categories = {row['name']: row['id'] for row in cursor.fetchall()}
    print(f"   Found {len(old_categories)} existing categories")
    
    # Step 2: Delete ALL existing categories and related data
    cursor.execute("DELETE FROM category_keywords")
    cursor.execute("DELETE FROM subcategories")
    cursor.execute("DELETE FROM budget_categories")
    cursor.execute("DELETE FROM categories")
    print("   Deleted all old category data")
    
    # Step 3: Create fresh categories
    new_cat_ids = {}
    for i, (name, (icon, color)) in enumerate(FINAL_CATEGORIES.items()):
        cursor.execute("""
            INSERT INTO categories (name, icon, color, is_system, sort_order)
            VALUES (?, ?, ?, 1, ?)
        """, (name, icon, color, i + 1))
        new_cat_ids[name] = cursor.lastrowid
    
    print(f"   Created {len(new_cat_ids)} new categories")
    
    conn.commit()
    conn.close()
    
    return old_categories, new_cat_ids


def update_products(old_cats, new_cats):
    """Update all product categories to use new names."""
    print("\n📦 Updating product categories...")
    
    conn = get_connection()
    cursor = conn.cursor()
    
    updated = 0
    
    for old_name, new_name in CATEGORY_MAPPING.items():
        cursor.execute("""
            UPDATE products SET auto_category = ? WHERE auto_category = ?
        """, (new_name, old_name))
        updated += cursor.rowcount
        
        cursor.execute("""
            UPDATE products SET user_category = ? WHERE user_category = ?
        """, (new_name, old_name))
        updated += cursor.rowcount
    
    # Set remaining unknown categories to Overig
    cursor.execute("""
        UPDATE products SET auto_category = 'Overig' 
        WHERE auto_category NOT IN (SELECT name FROM categories)
    """)
    updated += cursor.rowcount
    
    conn.commit()
    conn.close()
    
    print(f"   Updated {updated} product category references")


def reimport_keywords(new_cats):
    """Import keywords fresh from ah_categories.json."""
    print("\n🏷️  Importing fresh keywords...")
    
    import json
    
    json_path = Path(__file__).parent.parent / "data" / "ah_categories.json"
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    conn = get_connection()
    cursor = conn.cursor()
    
    # Core keyword mappings
    KEYWORD_TO_CATEGORY = {
        # Groente & Aardappelen
        "Groente & Aardappelen": [
            "tomaat", "tros", "paprika", "komkommer", "sla", "spinazie", "broccoli",
            "bloemkool", "wortel", "ui", "knoflook", "aardappel", "krieltjes",
            "champignon", "courgette", "aubergine", "prei", "selderij", "biet",
            "radijs", "snoepgroente", "rucola", "veldsla", "ijsbergsla", "andijvie",
            "bosui", "peterselie", "basilicum", "bieslook", "koriander", "munt",
            "dille", "rozemarijn", "tijm", "pompoen", "pastinaak", "knolselderij",
        ],
        # Fruit & Verse sappen
        "Fruit & Verse sappen": [
            "appel", "peer", "banaan", "sinaasappel", "mandarijn", "citroen", "limoen",
            "druif", "aardbei", "framboos", "bosbes", "braam", "kers", "pruim",
            "perzik", "nectarine", "mango", "ananas", "meloen", "watermeloen",
            "kiwi", "granaatappel", "passievrucht", "avocado", "smoothie", "vers sap",
            "jus d'orange", "fruitsalade", "dadel", "vijg",
        ],
        # Maaltijden & Salades
        "Maaltijden & Salades": [
            "maaltijd", "salade", "pizza", "lasagne", "quiche", "ovenschotel",
            "stamppot", "nasi", "bami", "macaroni", "sushi", "poke bowl",
            "wrap", "burrito", "taco", "gyoza", "dim sum", "loempia", "sate",
            "soep vers", "maaltijdsoep", "meal kit", "verspakket",
        ],
        # Vlees
        "Vlees": [
            "kipfilet", "kippenpoot", "kippenvleugel", "kipgehakt", "kip ",
            "rundergehakt", "biefstuk", "entrecote", "rosbief", "ossenhaas",
            "varkenshaas", "schnitzel", "speklapje", "varkensfilet", "braadworst",
            "gehakt", "hamburger", "tartaar", "worst", "spek", "bacon",
            "lam", "lamsbout", "eend", "kalkoen", "konijn", "wild",
            "shoarma", "gyros", "spareribs", "bbq vlees",
        ],
        # Vis
        "Vis": [
            "zalm", "tonijn", "kabeljauw", "schelvis", "scholfilet", "tilapia",
            "pangasius", "makreel", "haring", "sardine", "ansjovis", "forel",
            "zeebaars", "dorade", "garnaal", "scampi", "mossel", "oester",
            "kreeft", "krab", "inktvis", "gerookte zalm", "kibbeling", "lekkerbekje",
        ],
        # Vegetarisch & Vegan
        "Vegetarisch & Vegan": [
            "vegetarisch", "vegan", "plantaardig", "tofu", "tempeh", "seitan",
            "beyond", "impossible", "vivera", "garden gourmet", "quorn",
            "vega burger", "vega gehakt", "falafel", "hummus",
        ],
        # Vleeswaren
        "Vleeswaren": [
            "ham", "salami", "cervelaat", "rookvlees", "pastrami", "chorizo",
            "parmaham", "serrano", "coppa", "bacon", "ontbijtspek", "leverworst",
            "pate", "filet americain", "kipfilet beleg", "rosbief beleg", "knaks",
        ],
        # Kaas
        "Kaas": [
            "kaas", "gouda", "edammer", "mozzarella", "parmezaan", "pecorino",
            "brie", "camembert", "gorgonzola", "roquefort", "feta", "geitenkaas",
            "manchego", "gruyere", "emmentaler", "cheddar", "boursin", "philadelphia",
            "smeerkaas", "plakken kaas", "geraspte kaas", "belegen", "jong belegen",
            "extra belegen", "oud", "zaanlander", "maaslander",
        ],
        # Zuivel & Eieren
        "Zuivel & Eieren": [
            "melk", "halfvolle melk", "volle melk", "magere melk", "karnemelk",
            "yoghurt", "kwark", "skyr", "cottage cheese", "creme fraiche",
            "slagroom", "kookroom", "room", "boter", "margarine", "roomboter",
            "ei", "eieren", "vla", "pudding", "toetje", "panna cotta",
            "alpro", "oatly", "havermelk", "sojamelk", "amandelmelk",
        ],
        # Bakkerij
        "Bakkerij": [
            "brood", "wit brood", "bruin brood", "volkoren", "meergranen",
            "baguette", "ciabatta", "focaccia", "pistolet", "bol", "croissant",
            "pain au chocolat", "krentenbol", "rozijnenbrood", "afbakbrood",
            "toast", "beschuit", "cracker", "knackebrod", "wrap tortilla",
            "taart", "gebak", "vlaai", "cake", "muffin", "donut", "koek",
        ],
        # Borrel, Chips & Snacks
        "Borrel, Chips & Snacks": [
            "chips", "lay's", "pringles", "doritos", "cheetos", "bugles",
            "noten", "pinda", "cashew", "amandel", "walnoot", "pistache",
            "borrel", "bitterbal", "kaassouffle", "frikandel", "kroket",
            "popcorn", "wokkels", "takis", "zoutje", "toastje",
        ],
        # Pasta, Rijst & Wereldkeuken
        "Pasta, Rijst & Wereldkeuken": [
            "pasta", "spaghetti", "penne", "fusilli", "tagliatelle", "lasagne bladen",
            "rijst", "basmati", "jasmijn", "risotto", "noodles", "mie", "ramen",
            "couscous", "bulgur", "quinoa", "polenta", "tortilla", "taco shells",
            "woksaus", "currysaus", "teriyaki", "sojasaus", "kokosmelk", "sambal",
        ],
        # Soepen, Sauzen & Kruiden
        "Soepen, Sauzen & Kruiden": [
            "soep", "bouillon", "saus", "pastasaus", "pesto", "mayonaise", "ketchup",
            "mosterd", "dressing", "vinaigrette", "olie", "olijfolie", "zonnebloemolie",
            "azijn", "balsamico", "kruid", "specerij", "peper", "zout", "paprikapoeder",
            "kerrie", "kurkuma", "komijn", "oregano", "basilicum gedroogd",
        ],
        # Koek, Snoep & Chocolade
        "Koek, Snoep & Chocolade": [
            "chocolade", "bonbon", "praline", "reep", "tablet", "drops",
            "snoep", "winegum", "zuurtje", "lolly", "toffee", "karamel",
            "drop", "licorice", "zoethout", "koek", "biscuit", "speculaas",
            "stroopwafel", "gevulde koek", "bastogne", "oreo", "liga",
            "kauwgom", "mentos", "sportlife", "smint",
        ],
        # Ontbijtgranen & Beleg
        "Ontbijtgranen & Beleg": [
            "ontbijtgranen", "cornflakes", "muesli", "granola", "cruesli",
            "havermout", "havervlokken", "brinta", "pap",
            "jam", "marmelade", "honing", "stroop", "pindakaas", "chocopasta",
            "nutella", "speculoospasta", "hagelslag", "vlokken", "muisjes",
        ],
        # Tussendoortjes
        "Tussendoortjes": [
            "mueslireep", "cereal bar", "rijstwafel", "crackers", "liga milkbreak",
            "bifi", "noten mix", "studentenhaver", "gedroogd fruit", "rozijnen",
            "cranberry", "abrikoos gedroogd", "dadel", "vijg gedroogd",
        ],
        # Diepvries
        "Diepvries": [
            "diepvries", "vriezer", "ijs", "roomijs", "sorbet", "magnum", "cornetto",
            "ben & jerry", "häagen", "diepvriespizza", "diepvriesgroenten",
            "diepvriesvis", "visstick", "fishstick", "friet", "patat",
        ],
        # Koffie & Thee
        "Koffie & Thee": [
            "koffie", "koffiebonen", "gemalen koffie", "filterkoffie", "espresso",
            "nespresso", "dolce gusto", "senseo", "oploskoffie", "cappuccino",
            "thee", "theezakje", "groene thee", "zwarte thee", "kruidenthee",
            "rooibos", "pukka", "pickwick",
        ],
        # Frisdrank & Water
        "Frisdrank & Water": [
            "cola", "fanta", "sprite", "7up", "pepsi", "sinas", "cassis",
            "ice tea", "lipton", "fuze", "red bull", "energy drink", "monster",
            "water", "spa", "sourcy", "chaudfontaine", "bar le duc",
            "limonade", "siroop", "ranja", "karvan cevitam", "roosvicee",
            "rivella", "fernandes", "tonic",
        ],
        # Bier, Wijn & Aperitieven
        "Bier, Wijn & Aperitieven": [
            "bier", "pils", "hertog jan", "heineken", "amstel", "grolsch", "brand",
            "speciaalbier", "ipa", "wit bier", "radler", "0.0",
            "wijn", "rode wijn", "witte wijn", "rose", "champagne", "prosecco",
            "cava", "port", "sherry", "whisky", "wodka", "gin", "rum", "likeur",
        ],
        # Drogisterij
        "Drogisterij": [
            "shampoo", "conditioner", "douchegel", "zeep", "handzeep", "bodylotion",
            "deodorant", "deo", "tandpasta", "tandenborstel", "mondwater", "floss",
            "scheermesje", "scheerschuim", "aftershave", "parfum", "make-up",
            "maandverband", "tampon", "inlegkruisje", "condoom",
            "zonnebrand", "aftersun", "zelfbruiner", "lenzen", "lenzenvloeistof",
        ],
        # Huishouden
        "Huishouden": [
            "schoonmaak", "allesreiniger", "wc reiniger", "glasreiniger", "ontkalker",
            "afwasmiddel", "vaatwas", "wasmiddel", "wasverzachter", "vlekkenverwijderaar",
            "toiletpapier", "keukenpapier", "tissues", "zakdoek",
            "vuilniszak", "aluminiumfolie", "huishoudfolie", "bakpapier",
            "sponzen", "dweil", "bezem", "handschoenen", "schuurspons",
        ],
        # Baby & Kind
        "Baby & Kind": [
            "luier", "pampers", "kruidvat luier", "billendoekje",
            "babyvoeding", "potje", "fruithapje", "babymelk", "flesvoeding",
            "babyverzorging", "babylotion", "babyzalf", "fopspeen",
        ],
        # Huisdier
        "Huisdier": [
            "hondenvoer", "kattenvoer", "kattenbak", "kattenbakvulling",
            "hondensnack", "kattensnack", "dierverzorging",
        ],
        # Verpakking & Statiegeld
        "Verpakking & Statiegeld": [
            "statiegeld", "krat", "fles statiegeld", "retour", "plastic tas",
            "draagtas", "verpakking", "emballage",
        ],
        # Bezorgkosten
        "Bezorgkosten": [
            "bezorgkosten", "thuisbezorgd", "bezorging", "afhalen",
        ],
    }
    
    keywords_added = 0
    
    for cat_name, keywords in KEYWORD_TO_CATEGORY.items():
        cat_id = new_cats.get(cat_name)
        if not cat_id:
            print(f"   ⚠️  Category not found: {cat_name}")
            continue
        
        for keyword in keywords:
            try:
                cursor.execute("""
                    INSERT INTO category_keywords (category_id, keyword, match_type, priority, created_by)
                    VALUES (?, ?, 'contains', 0, 'system')
                """, (cat_id, keyword.lower()))
                keywords_added += 1
            except Exception as e:
                if "UNIQUE" not in str(e):
                    print(f"   ⚠️  Error adding '{keyword}': {e}")
    
    conn.commit()
    conn.close()
    
    print(f"   Added {keywords_added} keywords")


def fix_product_overrides(new_cats):
    """Update product overrides to use new category IDs."""
    print("\n🔧 Fixing product overrides...")
    
    conn = get_connection()
    cursor = conn.cursor()
    
    # Get new category IDs
    cursor.execute("SELECT id, name FROM categories")
    cat_ids = {row['name']: row['id'] for row in cursor.fetchall()}
    
    # Update overrides with valid category IDs
    updated = 0
    cursor.execute("SELECT id, product_name_pattern FROM product_overrides")
    overrides = cursor.fetchall()
    
    for override in overrides:
        # Re-categorize using the new system
        from services.categorization import get_categorization_service
        service = get_categorization_service()
        service._cache_timestamp = None  # Force refresh
        
        result = service.categorize(override['product_name_pattern'])
        new_cat = result['category']
        new_cat_id = cat_ids.get(new_cat)
        
        if new_cat_id:
            cursor.execute("""
                UPDATE product_overrides SET category_id = ? WHERE id = ?
            """, (new_cat_id, override['id']))
            updated += 1
    
    conn.commit()
    conn.close()
    
    print(f"   Updated {updated} overrides")


def verify_results():
    """Verify the cleanup results."""
    print("\n✅ Verification:")
    
    conn = get_connection()
    cursor = conn.cursor()
    
    # Count categories
    cursor.execute("SELECT COUNT(*) as cnt FROM categories")
    print(f"   Categories: {cursor.fetchone()['cnt']}")
    
    # Count keywords
    cursor.execute("SELECT COUNT(*) as cnt FROM category_keywords")
    print(f"   Keywords: {cursor.fetchone()['cnt']}")
    
    # Count products per category
    cursor.execute("""
        SELECT COALESCE(user_category, auto_category) as cat, COUNT(*) as cnt
        FROM products
        GROUP BY cat
        ORDER BY cnt DESC
        LIMIT 10
    """)
    print("   Top 10 product categories:")
    for row in cursor.fetchall():
        print(f"      {row['cat']}: {row['cnt']}")
    
    # Check for orphaned products
    cursor.execute("""
        SELECT COUNT(*) as cnt FROM products 
        WHERE COALESCE(user_category, auto_category) NOT IN (SELECT name FROM categories)
    """)
    orphaned = cursor.fetchone()['cnt']
    if orphaned > 0:
        print(f"   ⚠️  Orphaned products (no valid category): {orphaned}")
    else:
        print("   ✓ No orphaned products")
    
    conn.close()


def run_cleanup():
    """Run the complete cleanup."""
    print("=" * 60)
    print("🧹 APPESTAT DATABASE CLEANUP")
    print("=" * 60)
    
    old_cats, new_cats = cleanup_categories()
    update_products(old_cats, new_cats)
    reimport_keywords(new_cats)
    fix_product_overrides(new_cats)
    verify_results()
    
    print("\n" + "=" * 60)
    print("✅ CLEANUP COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    run_cleanup()


