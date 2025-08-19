# core/constants.py

# Shop pricing (by canonical rarity)
BUY_PRICES = {
    "common": 5,
    "rare": 10,
    "super": 20,
    "ultra": 50,
    "secret": 100,
    # intentionally no "starlight": not buyable by default
}

SELL_PRICES = {
    "common": 2,
    "rare": 4,
    "super": 7,
    "ultra": 20,
    "secret": 40,
    # intentionally no "starlight": UNSALEABLE
}
