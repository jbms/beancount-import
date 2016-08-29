POSTING_DATE_KEY = 'date'

POSTING_TRANSACTION_DATE_KEY = 'transaction_date'

def get_posting_date(entry, posting):
    """Returns the date associated with a posting."""
    return ((posting.meta and (posting.meta.get(POSTING_DATE_KEY) or
                               posting.meta.get(POSTING_TRANSACTION_DATE_KEY))) or entry.date)
