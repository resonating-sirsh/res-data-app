# Rather than import millions of SKUs into QBO, sales are booked under a dummy
# product named after their sales channel. This dict is a record of which dummy
# products correspond to which sales channel for each dev and prod QBO account
SALES_CHANNEL_ITEM_IDS = {
    "resonance_companies": {
        "photo": {"development": "19", "production": "5"},
        "sample": {"development": "20", "production": "6"},
        "wholesale": {"development": "21", "production": "7"},
        "resmagic.io": {"development": "22", "production": "8"},
        "null": {"development": "23", "production": "9"},
        "first one": {"development": "24", "production": "15"},
        "ecom": {"development": "25", "production": "10"},
        "gifting": {"development": "26", "production": "11"},
        "development": {"development": "27", "production": "12"},
        "revenue_share": {"development": "28", "production": "13"},
        "shipping": {"development": "29", "production": "14"},
    },
    "resonance_manufacturing": {
        "photo": {"development": "23", "production": "38"},
        "sample": {"development": "24", "production": "42"},
        "wholesale": {"development": "25", "production": "41"},
        "resmagic.io": {"development": "26", "production": "280"},
        "null": {"development": "27", "production": "281"},
        "first one": {"development": "28", "production": "282"},
        "ecom": {"development": "29", "production": "279"},
        "gifting": {"development": "30", "production": "40"},
        "development": {"development": "31", "production": "283"},
        "shipping": {"development": "21", "production": "43"},
    },
}

# Record IDs for brands whose Resonance Companies invoices are
# handled as Quickbooks journal entries instead and the corresponding QBO
# company they should be booked under. Also includes the account ref IDs for
# the various billing accounts used
#
# brand_bcoo_account_id = DR: 50-1010 COGS:COGS
# brand_rev_share_account_id = DR: 50-2045 Revenue Share COGS
# brand_shipping_account_id = DR: 50-2050 COGS:Fulfillment:Freight Charges
# brand_intercompany_res_account_id = CR: 99-2100 Intercompany - Resonance


JOURNAL_ENTRY_BRANDS = {
    "rec2gH2SWsd3mCYqq": {
        "name": "jcrt",
        "brand_bcoo_account_id": "207",
        "brand_rev_share_account_id": "394",
        "brand_shipping_account_id": "214",
        "brand_intercompany_res_account_id": "388",
        "resonance_vendor_id": "26",
    },
    "recibcrw0UFkW6hNg": {
        "name": "jcrt",
        "brand_bcoo_account_id": "207",
        "brand_rev_share_account_id": "394",
        "brand_shipping_account_id": "214",
        "brand_intercompany_res_account_id": "388",
        "resonance_vendor_id": "26",
    },
    "recaGZfpjBFKUKRmu": {
        "name": "jcrt",
        "brand_bcoo_account_id": "207",
        "brand_rev_share_account_id": "394",
        "brand_shipping_account_id": "214",
        "brand_intercompany_res_account_id": "388",
        "resonance_vendor_id": "26",
    },
    "recz8TwhW5JYui4fs": {
        "name": "allcaps",
        "brand_bcoo_account_id": "145",
        "brand_rev_share_account_id": "1150000001",
        "brand_shipping_account_id": "1150000002",
        "brand_intercompany_res_account_id": "147",
        "resonance_vendor_id": "3",
    },
    "recXHC5NWBNclJjA5": {
        "name": "the_kit",
        "brand_bcoo_account_id": "214",
        "brand_rev_share_account_id": "403",
        "brand_shipping_account_id": "221",
        "brand_intercompany_res_account_id": "401",
        "resonance_vendor_id": "57",
    },
    "recasW18emnKNnecf": {
        "name": "tucker",
        "brand_bcoo_account_id": "217",
        "brand_rev_share_account_id": "439",
        "brand_shipping_account_id": "224",
        "brand_intercompany_res_account_id": "435",
        "resonance_vendor_id": "66",
    },
    "reclSE7AoXNFMxqO4": {
        "name": "bruce_glenn",
        "brand_bcoo_account_id": "146",
        "brand_rev_share_account_id": "149",
        "brand_shipping_account_id": "152",
        "brand_intercompany_res_account_id": "151",
        "resonance_vendor_id": "5",
    },
}
