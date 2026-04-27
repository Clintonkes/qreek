import difflib

BANKS = [
    {"code": "044",    "name": "Access Bank",        "aliases": ["access", "access bank plc"]},
    {"code": "050",    "name": "Ecobank",             "aliases": ["eco", "ecobank nigeria"]},
    {"code": "070",    "name": "Fidelity Bank",       "aliases": ["fidelity", "fidelity bank plc"]},
    {"code": "011",    "name": "First Bank",          "aliases": ["first", "fbn", "first bank of nigeria"]},
    {"code": "214",    "name": "FCMB",                "aliases": ["first city monument", "first city monument bank"]},
    {"code": "058",    "name": "GTBank",              "aliases": ["gtb", "guaranty", "guaranty trust bank"]},
    {"code": "030",    "name": "Heritage Bank",       "aliases": ["heritage", "heritage bank ltd"]},
    {"code": "082",    "name": "Keystone Bank",       "aliases": ["keystone", "keystone bank limited"]},
    {"code": "076",    "name": "Polaris Bank",        "aliases": ["polaris", "skye", "skye bank"]},
    {"code": "101",    "name": "Providus Bank",       "aliases": ["providus", "providus bank plc"]},
    {"code": "221",    "name": "Stanbic IBTC",        "aliases": ["stanbic", "stanbic ibtc bank"]},
    {"code": "068",    "name": "Standard Chartered",  "aliases": ["stanchart", "standard chartered bank"]},
    {"code": "232",    "name": "Sterling Bank",       "aliases": ["sterling", "sterling bank plc"]},
    {"code": "032",    "name": "Union Bank",          "aliases": ["union", "union bank of nigeria"]},
    {"code": "033",    "name": "UBA",                 "aliases": ["united bank for africa"]},
    {"code": "215",    "name": "Unity Bank",          "aliases": ["unity", "unity bank plc"]},
    {"code": "035",    "name": "Wema Bank",           "aliases": ["wema", "wema bank plc"]},
    {"code": "057",    "name": "Zenith Bank",         "aliases": ["zenith", "zenith bank plc"]},
    {"code": "090267", "name": "Kuda Bank",           "aliases": ["kuda", "kuda microfinance"]},
    {"code": "100004", "name": "Opay",                "aliases": ["opay", "paycom", "opay digital services"]},
    {"code": "090265", "name": "VFD Microfinance",    "aliases": ["v bank", "vfd", "vbank"]},
    {"code": "100033", "name": "Palmpay",             "aliases": ["palm pay", "palmpay limited"]},
    {"code": "090115", "name": "Moniepoint",          "aliases": ["monie point", "moniepoint microfinance"]},
    {"code": "090405", "name": "Moniepoint MFB",      "aliases": ["moniepoint mfb"]},
    {"code": "090270", "name": "Dot Microfinance",    "aliases": ["dot", "dot mfb"]},
    {"code": "090328", "name": "Paga",                "aliases": ["paga", "pagatech"]},
    {"code": "090393", "name": "FairMoney",           "aliases": ["fairmoney", "fair money"]},
    {"code": "090317", "name": "Carbon",              "aliases": ["carbon", "carbon mfb"]},
    {"code": "090403", "name": "PiggyVest",           "aliases": ["piggyvest", "piggy vest"]},
]


def resolve_bank(query: str) -> dict | None:
    if not query:
        return None
    q = query.lower().strip()
    for b in BANKS:
        if q == b["code"]:
            return b
    for b in BANKS:
        if q == b["name"].lower() or q in [a.lower() for a in b["aliases"]]:
            return b
    for b in BANKS:
        if any(a.lower() in q for a in b["aliases"]) or b["name"].lower() in q:
            return b
    names_map = {}
    for b in BANKS:
        names_map[b["name"].lower()] = b
        for a in b["aliases"]:
            names_map[a.lower()] = b
    matches = difflib.get_close_matches(q, list(names_map.keys()), n=1, cutoff=0.6)
    if matches:
        return names_map[matches[0]]
    return None
