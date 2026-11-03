"""
Migration v016: indexes for the joins every page does (lines per receipt, receipts per
period/store). Without them listing receipts scanned all product lines per receipt.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from database import get_connection

VERSION = "016"
NAME = "indexes"
DESCRIPTION = "Indexes on products.invoice_id, invoices.invoice_date/store_id"


def migrate():
    conn = get_connection()
    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_invoice ON products(invoice_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_invoices_date ON invoices(invoice_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_invoices_store ON invoices(store_id)")
    conn.execute("ANALYZE")
    conn.commit()
    conn.close()
    return True
