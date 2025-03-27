import pandas as pd


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
