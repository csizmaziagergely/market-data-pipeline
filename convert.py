"""
Metric <-> Imperial unit converter.
Supported categories: length, weight, temperature, volume, area, speed.
"""

CONVERSIONS = {
    # Length
    "km":  ("length", "mi",  0.621371),
    "mi":  ("length", "km",  1.60934),
    "m":   ("length", "ft",  3.28084),
    "ft":  ("length", "m",   0.3048),
    "cm":  ("length", "in",  0.393701),
    "in":  ("length", "cm",  2.54),
    "mm":  ("length", "in",  0.0393701),
    # Weight
    "kg":  ("weight", "lb",  2.20462),
    "lb":  ("weight", "kg",  0.453592),
    "g":   ("weight", "oz",  0.035274),
    "oz":  ("weight", "g",   28.3495),
    "t":   ("weight", "ton", 1.10231),
    "ton": ("weight", "t",   0.907185),
    # Volume
    "l":   ("volume", "gal", 0.264172),
    "gal": ("volume", "l",   3.78541),
    "ml":  ("volume", "floz", 0.033814),
    "floz":("volume", "ml",  29.5735),
    # Area
    "m2":  ("area",   "ft2", 10.7639),
    "ft2": ("area",   "m2",  0.092903),
    "km2": ("area",   "mi2", 0.386102),
    "mi2": ("area",   "km2", 2.58999),
    "ha":  ("area",   "ac",  2.47105),
    "ac":  ("area",   "ha",  0.404686),
    # Speed
    "kph": ("speed",  "mph", 0.621371),
    "mph": ("speed",  "kph", 1.60934),
    "ms":  ("speed",  "fts", 3.28084),
    "fts": ("speed",  "ms",  0.3048),
}

TEMPERATURE_UNITS = {"c", "f", "k"}


def convert_temperature(value: float, from_unit: str, to_unit: str) -> float:
    to_celsius = {
        "c": lambda v: v,
        "f": lambda v: (v - 32) * 5 / 9,
        "k": lambda v: v - 273.15,
    }
    from_celsius = {
        "c": lambda v: v,
        "f": lambda v: v * 9 / 5 + 32,
        "k": lambda v: v + 273.15,
    }
    if from_unit not in to_celsius or to_unit not in from_celsius:
        raise ValueError(f"Unknown temperature unit: '{from_unit}' or '{to_unit}'")
    return from_celsius[to_unit](to_celsius[from_unit](value))


def convert(value: float, from_unit: str, to_unit: str) -> tuple[float, str]:
    """
    Convert `value` from `from_unit` to `to_unit`.
    Returns (result, category).
    Raises ValueError for unknown or incompatible units.
    """
    f = from_unit.lower()
    t = to_unit.lower()

    # Temperature is handled separately (non-linear)
    if f in TEMPERATURE_UNITS or t in TEMPERATURE_UNITS:
        if f not in TEMPERATURE_UNITS or t not in TEMPERATURE_UNITS:
            raise ValueError("Cannot mix temperature with other unit types.")
        return convert_temperature(value, f, t), "temperature"

    if f not in CONVERSIONS:
        raise ValueError(f"Unknown unit: '{from_unit}'")
    if t not in CONVERSIONS:
        raise ValueError(f"Unknown unit: '{to_unit}'")

    category_f, direct_to, factor = CONVERSIONS[f]
    category_t = CONVERSIONS[t][0]

    if category_f != category_t:
        raise ValueError(
            f"Incompatible units: '{from_unit}' ({category_f}) and '{to_unit}' ({category_t})"
        )

    # Direct pairing
    if t == direct_to:
        return value * factor, category_f

    # Convert via the canonical direct pair (e.g. km→mi→... doesn't exist,
    # so we convert f→direct_to, then direct_to→t if that path exists)
    _, t_direct_to, t_factor = CONVERSIONS[t]
    if t_direct_to == f:
        # e.g. ft → m: use 1/factor of m→ft
        return value / t_factor, category_f

    # Two-hop: f → direct_to → t
    intermediate = value * factor  # f → direct_to
    _, _, hop2_factor = CONVERSIONS[direct_to]
    if CONVERSIONS[direct_to][1] == t:
        return intermediate * hop2_factor, category_f

    raise ValueError(f"No conversion path found from '{from_unit}' to '{to_unit}'")


def print_help() -> None:
    print("\nAvailable units:")
    categories: dict[str, list[str]] = {}
    for unit, (cat, _, _) in CONVERSIONS.items():
        categories.setdefault(cat, []).append(unit)
    categories["temperature"] = ["c", "f", "k"]
    for cat, units in sorted(categories.items()):
        print(f"  {cat:<12} {', '.join(units)}")
    print()
    print("Usage:  <value> <from_unit> <to_unit>")
    print("        e.g.  5 km mi   |   100 f c   |   70 kg lb")
    print("        'help' for this message, 'quit' to exit\n")


def main() -> None:
    print("Initializing unit converter...")
    print("=" * 45)
    print("   Metric <-> Imperial Unit Converter")
    print("=" * 45)
    print_help()

    while True:
        try:
            raw = input("Convert> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nBye!")
            break

        if not raw:
            continue
        if raw.lower() in {"q", "quit", "exit"}:
            print("Bye!")
            break
        if raw.lower() in {"h", "help", "?"}:
            print_help()
            continue

        parts = raw.split()
        if len(parts) != 3:
            print("  Please enter:  <value> <from_unit> <to_unit>   (e.g. 5 km mi)")
            continue

        value_str, from_unit, to_unit = parts
        try:
            value = float(value_str)
        except ValueError:
            print(f"  '{value_str}' is not a valid number.")
            continue

        try:
            result, category = convert(value, from_unit, to_unit)
        except ValueError as e:
            print(f"  Error: {e}")
            continue

        print(f"  {value:g} {from_unit}  =  {result:.6g} {to_unit}  [{category}]")


if __name__ == "__main__":
    main()
