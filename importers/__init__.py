"""
Receipt importers (v3).

Every importer returns the same shape (see importers.common.new_result): product lines,
extras (deposits, delivery, subscriptions, savings stamps) and a reconciliation against
the totals printed on the receipt, so that products + extras == paid total.
"""
