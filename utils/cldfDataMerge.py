import pandas as pd
import re
import bibtexparser


def cldfDataMerge(
    path_to_languages: str,
    path_to_values: str,
    path_to_parameters: str,
    path_to_result: str,
) -> int:
    """
    Merges data from separate .csv files from cldf-datasets into one database.

    Arguments:
        path_to_languages : str
            The path to the table 'languages.csv' from cldf-datasets.
        path_to_values : str
            The path to the table 'values.csv' from cldf-datasets.
        path_to_parameters : str
            The path to table 'parameters.csv' from cldf-datasets.
        path_to_result : str
            The path to the future table with united data.

    Return:
        int: 0 in case of successful execution

    """
    values = pd.read_csv(path_to_values)

    valuesToLanguage = {}
    for index, row in values.iterrows():
        if row["Parameter_ID"] not in valuesToLanguage:
            valuesToLanguage[row["Parameter_ID"]] = {}
        valuesToLanguage[row["Parameter_ID"]][row["Language_ID"]] = row["Value"]

    parameters = pd.read_csv(path_to_parameters)
    paramNames = {}
    for index, row in parameters.iterrows():
        paramNames[f"{row["ID"]}_{row["Name"]}"] = []

    languages = pd.read_csv(path_to_languages)
    for param in paramNames:
        code, name = param.split("_")
        for index, row in languages.iterrows():
            if row["ID"] in valuesToLanguage[code]:
                paramNames[param].append(valuesToLanguage[code][row["ID"]])
            else:
                paramNames[param].append(None)

    walsDataMerged = languages.copy()
    for param in paramNames:
        walsDataMerged[param] = paramNames[param]
    walsDataMerged.to_csv(path_to_result)

    return 0


def sourcesMerge(
    path_to_bibtex: str, path_to_dataframe: str, path_to_result: str
) -> int:
    """
    Merges data about sources from bibtex to dataframe with iso-codes.

    Arguments:
        path_to_bibtex : str
            The path to the bibtex file with sources.
        path_to_dataframe : str
            The path to the .csv file with iso-codes.
        path_to_result : str
            The path to the future table with united data.

    Return:
        int: 0 in case of successful execution

    """

    with open(path_to_bibtex, "r") as bibtex_str:
        bibtex_str = bibtex_str.read()

    languages = {}

    library = bibtexparser.parse_string(bibtex_str)

    for entry in library.entries:
        if "lgcode" in entry:
            iso_code = re.search("(?<=\[)\w+(?=\])", entry["lgcode"])
            if iso_code is not None:
                iso_code = iso_code.group(0)
                if iso_code not in languages:
                    languages[iso_code] = set()
                if "inlg" in entry:
                    lang = re.search("(?<=\[)\w+(?=\])", entry["inlg"])
                    if lang is not None:
                        lang = lang.group(0)
                        languages[iso_code].add(lang)

    data = pd.read_csv(path_to_dataframe)
    sources = []
    for index, row in data.iterrows():
        if row["ISO_codes"] in languages:
            sources.append(" ".join(list(languages[row["ISO_codes"]])))
        else:
            sources.append(None)

    data["Sources' Languages"] = sources
    data.to_csv(path_to_result)
    return 0
