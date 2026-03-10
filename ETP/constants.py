# ETP/constants.py

TRANSPORTER_VEHICLES = {
    "M/s. Sai Waste Transport": [
        "MH42T0063", "MH11AL7217","Other"
    ],
    "M/s. Mangalmurti Enterprises": [
        "MH11AL7149", "MH11DD1154","Other"
    ],
    "M/s. S. S. Waste Transport": [
        "MH11AL3458", "MH11BD4272", "MH12KP6202",
        "MH13R5652", "MH11AL2352", "MH12HD2576", "MH42B9414","MH13R4744","Other",
    ],
    "M/s. Hazardous Waste Management": [
        "MH04LQ2879", "Other",
    ],
    "M/s. Green Gene Enviro Protection & Infrastructure Pvt. Ltd.": [
        "GJ16AV7413", "GJ16AV0522", "MH50N4069", "GJ16AW2580","KA705249","Other",
    ],
    "M/S. Mamta Enterprises": [
        "MH12MV4113","MH12MV4111","Other",
    ],
    
}

TYPE_OF_WASTE_CHOICES = [
    ("Distillation Residues", "Distillation Residues"),
    ("Process Residue and wastes", "Process Residue and wastes"),
    ("Chemical sludge from waste water treatment", "Chemical sludge from waste water treatment"),
    ("DIPEA Residue", "DIPEA Residue"),
    ("Spent HCL", "Spent HCL"),
    ("Spent Sulphuric", "Spent Sulphuric"),
    ("Sewage Water", "Sewage Water"),
    ("MEE Feed Water", "MEE Feed Water"),
]



# Add this mapping (edit numbers anytime to match your rate-card)
FACILITY_WASTE_RATES = {
    "M/s Maharashtra Enviro Power Ltd. (MEPL) Ranjangaon, Pune": {
        "Chemical sludge from waste water treatment": {"rate": 2205,  "transport": 65000},
        "Distillation Residues":                      {"rate": 26982, "transport": 49000},
        "Process Residue and wastes":                 {"rate": 29982, "transport": 49000},
    },
    "M/s. Hazardous Waste Management System": {
        "Distillation Residues":                      {"rate": 21500,  "transport": 0},
        "Process Residue and wastes":                 {"rate": 21500,  "transport": 0},
    },
    "M/s. Green Gene Enviro Protection & Infrastructure Pvt. Ltd.": {
        "Distillation Residues":                      {"rate": 23000, "transport": 0},
        "Process Residue and wastes":                 {"rate": 23000, "transport": 0},
        "DIPEA Residue":                              {"rate": 9000,  "transport": 0},
        "MEE Feed Water":                              {"rate": 0,  "transport": 0},
    },
    "M/S. Ferric Flow Private Ltd, Plot No G, 7/9, Near Cummins India pvt Ltd": {
        "Spent HCL":                                    {"rate": 4500, "transport": 0},
    },
    "M/S. Greenfield CET Plant Pvt Ltd, P-17, Chincholi MIDC, Solapur":{
        "Sewage Water":                                  {"rate": 0, "transport": 0},
    }
}


MEPL_FACILITY = "M/s Maharashtra Enviro Power Ltd. (MEPL) Ranjangaon, Pune"
MEPL_TRANSPORT_LT_EQ_15 = 49000
MEPL_TRANSPORT_GT_15    = 65000
MEPL_QTY_THRESHOLD      = 15





WASTE_CATEGORY_BY_TYPE = {
    "Distillation Residues": "20.3",
    "Process Residue and wastes": "28.1",
    "Chemical sludge from waste water treatment": "35.3",
    "Spent HCL":"26.3",
    # other types -> leave blank or add here
}