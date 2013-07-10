shopify_order_extractor
=======================

Extract orders from Shopify to CSV file using the same CSV schema than extract from web administrator page.

Note
----
There are no returned data corresponding to fields ``Paid at``, ``Lineitem compare at price``, ``Payment Reference``, ``Refunded Amount`` so I leave them empty. There is no returned data either for field ``Lineitem taxable`` but I remark that it's always ``true`` so I harcoded it to ``true``.
