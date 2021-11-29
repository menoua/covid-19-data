import os
from dotenv import load_dotenv
from dataclasses import make_dataclass


def _get_project_dir(err: bool = False):
    load_dotenv()
    project_dir = os.environ.get("OWID_COVID_PROJECT_DIR")
    if project_dir is None:  # err and
        raise ValueError("Please have ${OWID_COVID_PROJECT_DIR}.")
    return project_dir


def _get_scripts_dir(project_dir: str):
    # SCRIPTS DIRECTORY
    _SCRIPTS_DIR = os.path.join(project_dir, "scripts")
    _SCRIPTS_OLD_DIR = os.path.join(_SCRIPTS_DIR, "scripts")
    _SCRIPTS_GRAPHER_DIR = os.path.join(_SCRIPTS_DIR, "grapher")
    _SCRIPTS_INPUT_DIR = os.path.join(_SCRIPTS_DIR, "input")
    _SCRIPTS_OUTPUT_DIR = os.path.join(_SCRIPTS_DIR, "output")
    _SCRIPTS_OUTPUT_VAX_DIR = os.path.join(_SCRIPTS_OUTPUT_DIR, "vaccinations")
    _SCRIPTS_OUTPUT_TEST_DIR = os.path.join(_SCRIPTS_OUTPUT_DIR, "testing")
    _SCRIPTS_DOCS_DIR = os.path.join(_SCRIPTS_DIR, "docs")
    _INPUT_FOLDER_LS = [
        "bsg",
        "gbd",
        "jh",
        "reproduction",
        "wb",
        "cdc",
        "gmobility",
        "jhu",
        "sweden",
        "who",
        "ecdc",
        "iso",
        "owid",
        "un",
        "yougov",
    ]

    _scripts_dirs = {
        "OLD": _SCRIPTS_OLD_DIR,
        "GRAPHER": _SCRIPTS_GRAPHER_DIR,
        "INPUT": _SCRIPTS_INPUT_DIR,
        **{f"INPUT_{e.upper()}": os.path.join(_SCRIPTS_INPUT_DIR, e) for e in _INPUT_FOLDER_LS},
        "OUTPUT": _SCRIPTS_OUTPUT_DIR,
        "OUTPUT_VAX": _SCRIPTS_OUTPUT_VAX_DIR,
        "OUTPUT_VAX_AGE": os.path.join(_SCRIPTS_OUTPUT_VAX_DIR, "by_age_group"),
        "OUTPUT_VAX_MANUFACT": os.path.join(_SCRIPTS_OUTPUT_VAX_DIR, "by_manufacturer"),
        "OUTPUT_VAX_MAIN": os.path.join(_SCRIPTS_OUTPUT_VAX_DIR, "main_data"),
        "OUTPUT_VAX_META": os.path.join(_SCRIPTS_OUTPUT_VAX_DIR, "metadata"),
        "OUTPUT_VAX_META_AGE": os.path.join(_SCRIPTS_OUTPUT_VAX_DIR, "metadata", "locations_age.csv"),
        "OUTPUT_VAX_META_MANUFACT": os.path.join(_SCRIPTS_OUTPUT_VAX_DIR, "metadata", "locations_manufacturer.csv"),
        "OUTPUT_VAX_PROPOSALS": os.path.join(_SCRIPTS_OUTPUT_VAX_DIR, "proposals"),
        "OUTPUT_TEST": _SCRIPTS_OUTPUT_TEST_DIR,
        "DOCS": _SCRIPTS_DOCS_DIR,
        "DOCS_VAX": os.path.join(_SCRIPTS_DOCS_DIR, "vaccination"),
        "TMP_VAX": os.path.join(_SCRIPTS_DIR, "vaccinations.preliminary.csv"),
        "TMP_VAX_META": os.path.join(_SCRIPTS_DIR, "metadata.preliminary.csv"),
    }
    _scripts_dirs = {**_scripts_dirs, "INPUT_CDC_VAX": os.path.join(_scripts_dirs["INPUT_CDC"], "vaccinations")}
    B = make_dataclass("Bucket", _scripts_dirs.keys(), frozen=True)
    B.__repr__ = lambda _: _SCRIPTS_DIR
    return B(**_scripts_dirs)


def _get_data_dir(project_dir: str):
    _PUBLIC_DIR = os.path.join(project_dir, "public")
    _DATA_DIR = os.path.join(_PUBLIC_DIR, "data")
    _DATA_FOLDER_LS = [
        "ecdc",
        "excess_mortality",
        "internal",
        "jhu",
        "latest",
        "testing",
        "vaccinations",
        "variants",
        "who",
    ]

    _data_dirs = {
        "PUBLIC": _PUBLIC_DIR,
        **{f"{e.upper()}": os.path.join(_DATA_DIR, e) for e in _DATA_FOLDER_LS},
    }

    _data_dirs = {
        **_data_dirs,
        "TIMESTAMP": os.path.join(_data_dirs["INTERNAL"], "timestamp"),
        "VAX_COUNTRY": os.path.join(_data_dirs["VACCINATIONS"], "country_data"),
        "VAX_META_MANUFACT": os.path.join(_data_dirs["VACCINATIONS"], "locations_manufacturer.csv"),
        "VAX_META_AGE": os.path.join(_data_dirs["VACCINATIONS"], "locations_age.csv"),
        "VAX_META": os.path.join(_data_dirs["VACCINATIONS"], "locations.csv"),
    }

    B = make_dataclass("Bucket", _data_dirs.keys(), frozen=True,)
    B.__repr__ = lambda _: _DATA_DIR
    return B(**_data_dirs)


PROJECT_DIR = _get_project_dir()
SCRIPTS = _get_scripts_dir(PROJECT_DIR)
DATA = _get_data_dir(PROJECT_DIR)


def out_vax(country: str, public=False, age=False, manufacturer=False, proposal=False):
    if not public:
        if age:
            return os.path.join(SCRIPTS.OUTPUT_VAX_AGE, f"{country}.csv")
        elif manufacturer:
            return os.path.join(SCRIPTS.OUTPUT_VAX_MANUFACT, f"{country}.csv")
        elif proposal:
            return os.path.join(SCRIPTS.OUTPUT_VAX_PROPOSALS, f"{country}.csv")
        return os.path.join(SCRIPTS.OUTPUT_VAX_MAIN, f"{country}.csv")
    else:
        return os.path.join(DATA.VAX_COUNTRY, f"{country}.csv")
