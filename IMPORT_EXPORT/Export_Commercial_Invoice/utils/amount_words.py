from decimal import Decimal

ONES = ["", "One", "Two", "Three", "Four", "Five", "Six", "Seven", "Eight", "Nine"]
TEENS = ["Ten", "Eleven", "Twelve", "Thirteen", "Fourteen", "Fifteen", "Sixteen", "Seventeen", "Eighteen", "Nineteen"]
TENS = ["", "", "Twenty", "Thirty", "Forty", "Fifty", "Sixty", "Seventy", "Eighty", "Ninety"]

def _two_digits(n: int) -> str:
    if n == 0:
        return ""
    if n < 10:
        return ONES[n]
    if 10 <= n < 20:
        return TEENS[n - 10]
    t, o = divmod(n, 10)
    return (TENS[t] + (" " + ONES[o] if o else "")).strip()

def _three_digits(n: int) -> str:
    h, r = divmod(n, 100)
    parts = []
    if h:
        parts.append(ONES[h] + " Hundred")
    if r:
        parts.append(_two_digits(r))
    return " ".join([p for p in parts if p]).strip()

def int_to_words(n: int) -> str:
    if n == 0:
        return "Zero"

    parts = []
    billions, n = divmod(n, 1_000_000_000)
    millions, n = divmod(n, 1_000_000)
    thousands, n = divmod(n, 1_000)

    if billions:
        parts.append(_three_digits(billions) + " Billion")
    if millions:
        parts.append(_three_digits(millions) + " Million")
    if thousands:
        parts.append(_three_digits(thousands) + " Thousand")
    if n:
        parts.append(_three_digits(n))

    return " ".join(parts).strip()

def money_usd_to_words(amount: Decimal) -> str:
    amount = Decimal(amount or 0).quantize(Decimal("0.01"))
    dollars = int(amount)
    cents = int((amount - dollars) * 100)

    words = int_to_words(dollars)
    if cents:
        words = f"{words} and {int_to_words(cents)} Cents"
    return words
