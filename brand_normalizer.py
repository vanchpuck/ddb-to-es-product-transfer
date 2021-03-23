import itertools

# Flattened brand dict like {'inov-8': 'Inov-8', 'inov8': 'Inov-8', 'therm-a-rest': 'Therm-a-Rest', ...}
BRANDS = dict(
    (normalized, non_normalized) for normalized, non_normalized in
    itertools.chain.from_iterable(
        map(
            lambda brand_tuple: [(non_normalized.lower(), brand_tuple[0]) for non_normalized in brand_tuple[1]],
            {
                "Inov-8": ["Inov-8", "Inov8"],
                "Therm-a-Rest": ["Therm-a-rest", "thermarest"],
                "La Sportiva": ["La Sportiva, LaSportiva"],
                "MontBell": ["MontBell", "Mont Bell"],
                "Wildcountry": ["Wildcountry", "wild country"],
                "JetBoil": ["JetBoil", "Jet Boil"],
                "Black Diamond": ["Black diamond", "BlackDiamond", "BD"],
                "Outdoor Research": ["Outdoor Research", "OR"],
                "Trangoworld": ["Trangoworld", "Trango world"],
                "C.A.M.P.": ["camp usa - cassin", "camp", "c.a.m.p", "cassin"],
                "Climbing Technology": ["CT", "Climbing Technology", "climbingtechnology"]
            }.items()
        )
    )
)


def normalize(brand: str) -> str:
    return BRANDS.get(brand.lower(), brand)


# for testing purposes
if __name__ == "__main__":
    print(normalize("petzl"))
