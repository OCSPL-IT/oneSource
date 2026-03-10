COMPANY_GROUPS = {
    "OCSPL": [
        "OC Specialities Private Limited - Solapur",
        "OC Specialities Private Limited - Mumbai",
        "OC Specialities Private Limited - GJ",
        "OC Specialities Private Limited - Warehouse (AKOLEKATHI)",
        "OC Specialities Private Limited - Solapur (Unit II)",
        "OC Specialities Private Limited - WAREHOUSE (F-36 MIDC, CHINCHOLI)",
    ],
    "OCCHEM": [
        "OC Specialities Chemicals Private Limited",
        "OC Specialities Chemicals Private Limited -MH",
        "OC Specialities Chemicals Private Limited -GJ",
        "OC Specialities Chemicals Private Limited -AP",
    ],
}

def get_company_group(company_name: str) -> str:
    if not company_name:
        return "OTHER"
    cn = str(company_name).strip()
    for group, names in COMPANY_GROUPS.items():
        if cn in names:
            return group
    n = cn.lower()
    if "oc special" in n or "ocspl" in n:
        return "OCSPL"
    if "oc specialities chemicals" in n or "oc chem" in n or "occhem" in n:
        return "OCCHEM"
    return "OTHER"
