import beancount.parser.printer


class SortedEntryPrinter(beancount.parser.printer.EntryPrinter):
    """A subclass of EntryPrinter that sorts the meta keys before printing.

    This preserves the behavior of EntryPrinter from Beancount 2.3.5 and
    earlier.

    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def write_metadata(self, meta, oss, prefix=None):
        if meta is None:
            sorted_meta = None
        else:
            sorted_meta = dict(sorted(meta.items()))
        super().write_metadata(sorted_meta, oss, prefix)
