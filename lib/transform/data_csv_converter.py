import os

import pandas as pd

from lib.tracking_decorator import TrackingDecorator


@TrackingDecorator.track_time
def convert_data_to_csv(source_path, results_path, clean=False, quiet=False):
    # Iterate over files
    for subdir, dirs, files in sorted(os.walk(source_path)):

        # Make results path
        subdir = subdir.replace(f"{source_path}/", "")
        os.makedirs(os.path.join(results_path, subdir), exist_ok=True)

        for file_name in [file_name for file_name in sorted(files) if "~" not in file_name]:
            source_file_path = os.path.join(source_path, subdir, file_name)
            convert_file_to_csv(source_path, source_file_path, clean=clean, quiet=quiet)


def convert_file_to_csv(source_path, source_file_path, clean=False, quiet=False):
    source_file_name, source_file_extension = os.path.splitext(source_file_path)
    source_file_name = source_file_name.split(os.path.sep)[-1]

    if "berlin-election" in source_file_name:
        if source_file_name.endswith("2016-constituency-results"):
            convert_file_to_csv_2016_constituency_results_first_vote(source_path, source_file_path, clean=False,
                                                                     quiet=False)
            convert_file_to_csv_2016_constituency_results_second_vote(source_path, source_file_path, clean=False,
                                                                      quiet=False)
            convert_file_to_csv_2016_constituency_results_district_council(source_path, source_file_path, clean=False,
                                                                           quiet=False)
        if source_file_name.endswith("2023-constituency-results"):
            convert_file_to_csv_2023_constituency_results_first_vote(source_path, source_file_path, clean=False,
                                                                     quiet=False)
            convert_file_to_csv_2023_constituency_results_second_vote(source_path, source_file_path, clean=False,
                                                                      quiet=False)
            convert_file_to_csv_2023_constituency_results_district_council(source_path, source_file_path, clean=False,
                                                                           quiet=False)
        if source_file_name.endswith("2016-converted-results-berlin-elections-2011"):
            convert_file_to_csv_2016_converted_results_berlin_elections_2011(source_path, source_file_path, clean=False,
                                                                             quiet=False)
        if source_file_name.endswith("2016-converted-results-federal-election-2013"):
            convert_file_to_csv_2016_converted_results_federal_election_2013(source_path, source_file_path, clean=False,
                                                                             quiet=False)
        if source_file_name.endswith("2016-electoral-statistics"):
            convert_file_to_csv_2016_electoral_statistics_1(source_path, source_file_path, clean=False, quiet=False)
            convert_file_to_csv_2016_electoral_statistics_2(source_path, source_file_path, clean=False, quiet=False)
            convert_file_to_csv_2016_electoral_statistics_3(source_path, source_file_path, clean=False, quiet=False)
        if source_file_name.endswith("2016-structural-data"):
            convert_file_to_csv_2016_structural_data_1_1(source_path, source_file_path, clean=False, quiet=False)
            convert_file_to_csv_2016_structural_data_1_2(source_path, source_file_path, clean=False, quiet=False)
            convert_file_to_csv_2016_structural_data_1_3(source_path, source_file_path, clean=False, quiet=False)
            convert_file_to_csv_2016_structural_data_1_4(source_path, source_file_path, clean=False, quiet=False)
            convert_file_to_csv_2016_structural_data_1_5(source_path, source_file_path, clean=False, quiet=False)
            convert_file_to_csv_2016_structural_data_1_6(source_path, source_file_path, clean=False, quiet=False)
            convert_file_to_csv_2016_structural_data_1_7(source_path, source_file_path, clean=False, quiet=False)
            convert_file_to_csv_2016_structural_data_1_8(source_path, source_file_path, clean=False, quiet=False)
            convert_file_to_csv_2016_structural_data_2_1(source_path, source_file_path, clean=False, quiet=False)


def convert_file_to_csv_2016_constituency_results_first_vote(source_path, source_file_path, clean=False, quiet=False):
    source_file_name, source_file_extension = os.path.splitext(source_file_path)
    file_path_csv = f"{source_file_name}-first-vote.csv"

    # Check if result needs to be generated
    if not clean and os.path.exists(file_path_csv):
        if not quiet:
            print(f"✓ Already exists {os.path.basename(file_path_csv)}")
        return

    # Determine engine
    engine = build_engine(source_file_extension)

    try:
        sheet_name = "Erststimme"
        skiprows = 1
        names = ["Adresse", "Stimmart", "Bezirksnummer", "Bezirksname", "Wahlbezirk", "Wahlbezirksart",
                 "Abgeordnetenhauswahlkreis", "Bundestagswahlkreis", "Berlin OstWest", "Wahlberechtigte insgesamt",
                 "Wahlberechtigte A1", "Wahlberechtigte A2", "Wahlberechtigte A3", "Wähler", "Wähler B1",
                 "Ungültige Stimmen", "Gültige Stimmen", "SPD", "CDU", "GRÜNE", "DIE LINKE", "PIRATEN", "NPD", "FDP",
                 "Tierschutzpartei", "pro Deutschland", "Die PARTEI", "BIG", "DKP", "ödp", "PSG", "BüSo", "B", "DL",
                 "ALFA", "Tierschutzallianz", "AfD", "DIE EINHEIT", "DIE VIOLETTEN", "Graue Panther",
                 "MENSCHLICHE WELT", "MIETERPARTEI", "Gesundheitsforschung", "EB 1", "EB 2", "EB 3", "EB 4", "EB 5",
                 "EB 6", "EB 7", "EB 8", "EB 9", "EB 10", "EB 11", "EB 12", "EB 13", "EB 14", "EB 15", "EB 16", "EB 17",
                 "EB 18"]
        drop_columns = []

        dataframe = pd.read_excel(source_file_path, engine=engine, sheet_name=sheet_name, skiprows=skiprows,
                                  header=None, usecols=list(range(0, len(names))), names=names) \
            .drop(columns=drop_columns, errors="ignore") \
            .dropna()

        # Write csv file
        write_csv_file(dataframe, file_path_csv, quiet)
    except Exception as e:
        print(f"✗️ Exception: {str(e)}")


def convert_file_to_csv_2023_constituency_results_second_vote(source_path, source_file_path, clean=False, quiet=False):
    source_file_name, source_file_extension = os.path.splitext(source_file_path)
    file_path_csv = f"{source_file_name}-second-vote.csv"

    # Check if result needs to be generated
    if not clean and os.path.exists(file_path_csv):
        if not quiet:
            print(f"✓ Already exists {os.path.basename(file_path_csv)}")
        return

    # Determine engine
    engine = build_engine(source_file_extension)

    try:
        sheet_name = "AGH_W2"
        skiprows = 1
        names = ["Stimmart", "Adresse", "Bezirksnummer", "Bezirksname", "Wahlbezirk", "Wahlbezirksart",
                 "Briefwahlbezirk", "Abgeordnetenhauswahlkreis", "Bundestagswahlkreis", "OstWest",
                 "Wahlberechtigte insgesamt", "Wahlberechtigte A1", "Wahlberechtigte A2", "Wahlberechtigte A3",
                 "Wählende", "Wählende B1", "Gültige Stimmen", "Ungültige Stimmen", "SPD", "CDU", "GRÜNE", "DIE LINKE",
                 "AfD", "FDP", "Die PARTEI", "Tierschutzpartei", "PIRATEN", "Graue Panther", "NPD",
                 "Gesundheitsforschung", "LKR", "DKP", "SGP", "BüSo", "MENSCHLICHE WELT", "B*", "ÖDP", "dieBasis",
                 "Bildet Berlin!", "DL", "Deutsche Konservative", "Die Grauen", "Neue Demokraten", "REP", "du.",
                 "BÜNDNIS21", "DIE FRAUEN", "FREIE WÄHLER", "Klimaliste Berlin", "LD", "MIETERPARTEI", "Die Humanisten",
                 "Team Todenhöfer", "Volt"]
        drop_columns = []

        dataframe = pd.read_excel(source_file_path, engine=engine, sheet_name=sheet_name, skiprows=skiprows,
                                  header=None, usecols=list(range(0, len(names))), names=names) \
            .drop(columns=drop_columns, errors="ignore") \
            .dropna()

        # Write csv file
        write_csv_file(dataframe, file_path_csv, quiet)
    except Exception as e:
        print(f"✗️ Exception: {str(e)}")


def convert_file_to_csv_2016_constituency_results_second_vote(source_path, source_file_path, clean=False, quiet=False):
    source_file_name, source_file_extension = os.path.splitext(source_file_path)
    file_path_csv = f"{source_file_name}-second-vote.csv"

    # Check if result needs to be generated
    if not clean and os.path.exists(file_path_csv):
        if not quiet:
            print(f"✓ Already exists {os.path.basename(file_path_csv)}")
        return

    # Determine engine
    engine = build_engine(source_file_extension)

    try:
        sheet_name = "Erststimme"
        skiprows = 1
        names = ["Adresse", "Stimmart", "Bezirksnummer", "Bezirksname", "Wahlbezirk", "Wahlbezirksart",
                 "Abgeordnetenhauswahlkreis", "Bundestagswahlkreis", "Berlin OstWest", "Wahlberechtigte insgesamt",
                 "Wahlberechtigte A1", "Wahlberechtigte A2", "Wahlberechtigte A3", "Wähler", "Wähler B1",
                 "Ungültige Stimmen", "Gültige Stimmen", "SPD", "CDU", "GRÜNE", "DIE LINKE", "PIRATEN", "NPD", "FDP",
                 "Tierschutzpartei", "pro Deutschland", "Die PARTEI", "BIG", "DKP", "ödp", "PSG", "BüSo", "B", "DL",
                 "ALFA", "Tierschutzallianz", "AfD", "DIE EINHEIT", "DIE VIOLETTEN", "Graue Panther",
                 "MENSCHLICHE WELT", "MIETERPARTEI", "Gesundheitsforschung"]
        drop_columns = []

        dataframe = pd.read_excel(source_file_path, engine=engine, sheet_name=sheet_name, skiprows=skiprows,
                                  header=None, usecols=list(range(0, len(names))), names=names) \
            .drop(columns=drop_columns, errors="ignore") \
            .dropna()

        # Write csv file
        write_csv_file(dataframe, file_path_csv, quiet)
    except Exception as e:
        print(f"✗️ Exception: {str(e)}")


def convert_file_to_csv_2016_constituency_results_district_council(source_path, source_file_path, clean=False,
                                                                   quiet=False):
    source_file_name, source_file_extension = os.path.splitext(source_file_path)
    file_path_csv = f"{source_file_name}-district-council.csv"

    # Check if result needs to be generated
    if not clean and os.path.exists(file_path_csv):
        if not quiet:
            print(f"✓ Already exists {os.path.basename(file_path_csv)}")
        return

    # Determine engine
    engine = build_engine(source_file_extension)

    try:
        sheet_name = "BVV"
        skiprows = 1
        names = ["Adresse", "Stimmart", "Bezirksnummer", "Bezirksname", "Wahlbezirk", "Wahlbezirksart",
                 "Abgeordnetenhauswahlkreis", "Bundestagswahlkreis", "Berlin OstWest", "Wahlberechtigte insgesamt",
                 "Wahlberechtigte A1", "Wahlberechtigte A2", "Wahlberechtigte A3", "Wähler", "Wähler B1",
                 "Ungültige Stimmen", "Gültige Stimmen", "SPD", "CDU", "GRÜNE", "DIE LINKE", "PIRATEN", "NPD", "FDP",
                 "Tierschutzpartei", "pro Deutschland", "Die PARTEI", "BIG", "DKP", "ödp", "PSG", "BüSo", "B", "DL",
                 "ALFA", "Tierschutzallianz", "AfD", "DIE EINHEIT", "DIE VIOLETTEN", "Graue Panther",
                 "MENSCHLICHE WELT", "MIETERPARTEI", "Gesundheitsforschung", "EB 1", "EB 2", "EB 3", "EB 4", "EB 5",
                 "EB 6", "EB 7", "EB 8", "EB 9", "EB 10", "EB 11", "EB 12", "EB 13", "EB 14", "EB 15", "EB 16", "EB 17",
                 "EB 18", "Deutsche Konservative", "PdW", "Aktive Bürger", "WiN", "WisS", "ÖkoLinX-ARL B F-K", "PNE",
                 "BANC"]
        drop_columns = ["Abgeordnetenhauswahlkreis"]

        dataframe = pd.read_excel(source_file_path, engine=engine, sheet_name=sheet_name, skiprows=skiprows,
                                  header=None, usecols=list(range(0, len(names))), names=names) \
            .drop(columns=drop_columns, errors="ignore") \
            .dropna()

        # Write csv file
        write_csv_file(dataframe, file_path_csv, quiet)
    except Exception as e:
        print(f"✗️ Exception: {str(e)}")


def convert_file_to_csv_2023_constituency_results_first_vote(source_path, source_file_path, clean=False, quiet=False):
    source_file_name, source_file_extension = os.path.splitext(source_file_path)
    file_path_csv = f"{source_file_name}-first-vote.csv"

    # Check if result needs to be generated
    if not clean and os.path.exists(file_path_csv):
        if not quiet:
            print(f"✓ Already exists {os.path.basename(file_path_csv)}")
        return

    # Determine engine
    engine = build_engine(source_file_extension)

    try:
        sheet_name = "AGH_W1"
        skiprows = 1
        names = ["Stimmart", "Adresse", "Bezirksnummer", "Bezirksname", "Wahlbezirk", "Wahlbezirksart",
                 "Briefwahlbezirk", "Abgeordnetenhauswahlkreis", "Bundestagswahlkreis", "OstWest",
                 "Wahlberechtigte insgesamt", "Wahlberechtigte A1", "Wahlberechtigte A2", "Wahlberechtigte A3",
                 "Wählende", "Wählende B1", "Gültige Stimmen", "Ungültige Stimmen", "SPD", "CDU", "GRÜNE", "DIE LINKE",
                 "AfD", "FDP", "Die PARTEI", "Tierschutzpartei", "PIRATEN", "Graue Panther", "NPD",
                 "Gesundheitsforschung", "LKR", "DKP", "SGP", "BüSo", "MENSCHLICHE WELT", "B*", "ÖDP", "dieBasis",
                 "Bildet Berlin!", "DL", "Deutsche Konservative", "Die Grauen", "Neue Demokraten", "REP", "du.",
                 "BÜNDNIS21", "DIE FRAUEN", "FREIE WÄHLER", "Klimaliste Berlin", "LD", "MIETERPARTEI", "Die Humanisten",
                 "Team Todenhöfer", "Volt", "EB 1", "EB	2", "EB 3", "EB 4", "EB 5", "EB 6", "EB 7", "EB 8", "EB 9",
                 "EB 10", "EB 11", "EB 12"]
        drop_columns = []

        dataframe = pd.read_excel(source_file_path, engine=engine, sheet_name=sheet_name, skiprows=skiprows,
                                  header=None, usecols=list(range(0, len(names))), names=names) \
            .drop(columns=drop_columns, errors="ignore") \
            .dropna()

        # Write csv file
        write_csv_file(dataframe, file_path_csv, quiet)
    except Exception as e:
        print(f"✗️ Exception: {str(e)}")


def convert_file_to_csv_2023_constituency_results_district_council(source_path, source_file_path, clean=False,
                                                                   quiet=False):
    source_file_name, source_file_extension = os.path.splitext(source_file_path)
    file_path_csv = f"{source_file_name}-district-council.csv"

    # Check if result needs to be generated
    if not clean and os.path.exists(file_path_csv):
        if not quiet:
            print(f"✓ Already exists {os.path.basename(file_path_csv)}")
        return

    # Determine engine
    engine = build_engine(source_file_extension)

    try:
        sheet_name = "BVV"
        skiprows = 1
        names = ["Stimmart", "Adresse", "Bezirksnummer", "Bezirksname", "Wahlbezirk", "Wahlbezirksart",
                 "Briefwahlbezirk", "Abgeordnetenhauswahlkreis", "Bundestagswahlkreis", "OstWest",
                 "Wahlberechtigte insgesamt", "Wahlberechtigte A1", "Wahlberechtigte A2", "Wahlberechtigte A3",
                 "Wählende", "Wählende B1", "Gültige Stimmen", "Ungültige Stimmen", "SPD", "CDU", "GRÜNE", "DIE LINKE",
                 "AfD", "FDP", "Die PARTEI", "Tierschutzpartei", "PIRATEN", "NPD", "LKR", "B*", "ÖDP", "dieBasis",
                 "Die Grauen", "FREIE WÄHLER", "Klimaliste Berlin", "LD", "MIETERPARTEI", "Die Humanisten", "Volt",
                 "WB WsB F-K", "WB die-waehlbaren.de", "WB olf", "WB BÄRLÄ", "WB WsB Spandau", "WB TKBB", "WB WsB",
                 "WB WisS", "WB WsB"]
        drop_columns = []

        dataframe = pd.read_excel(source_file_path, engine=engine, sheet_name=sheet_name, skiprows=skiprows,
                                  header=None, usecols=list(range(0, len(names))), names=names) \
            .drop(columns=drop_columns, errors="ignore") \
            .dropna()

        # Write csv file
        write_csv_file(dataframe, file_path_csv, quiet)
    except Exception as e:
        print(f"✗️ Exception: {str(e)}")


def convert_file_to_csv_2016_converted_results_berlin_elections_2011(source_path, source_file_path, clean=False,
                                                                     quiet=False):
    source_file_name, source_file_extension = os.path.splitext(source_file_path)
    file_path_csv = f"{source_file_name}.csv"

    # Check if result needs to be generated
    if not clean and os.path.exists(file_path_csv):
        if not quiet:
            print(f"✓ Already exists {os.path.basename(file_path_csv)}")
        return

    # Determine engine
    engine = build_engine(source_file_extension)

    try:
        sheet_name = "2011_Zweitstimmen"
        skiprows = 1
        names = ["ID", "Bezirksnummer", "Bezirksname", "Wahlbezirk", "Wahlbezirksart", "Abgeordnetenhauswahlkreis",
                 "Bundestagswahlkreis", "Wahlberechtigte insgesamt", "Wähler", "Ungültige Stimmen", "Gültige Stimmen",
                 "SPD", "SPD in Prozent", "CDU", "CDU in Prozent", "GRÜNE", "GRÜNE in Prozent", "DIE LINKE",
                 "DIE LINKE in Prozent", "PIRATEN", "PIRATEN in Prozent", "NPD", "NPD in Prozent", "FDP",
                 "FDP in Prozent", "Sonstige", "Sonstige in Prozent"]
        drop_columns = []

        dataframe = pd.read_excel(source_file_path, engine=engine, sheet_name=sheet_name, skiprows=skiprows,
                                  header=None, usecols=list(range(0, len(names))), names=names) \
            .drop(columns=drop_columns, errors="ignore") \
            .dropna()

        # Write csv file
        write_csv_file(dataframe, file_path_csv, quiet)
    except Exception as e:
        print(f"✗️ Exception: {str(e)}")


def convert_file_to_csv_2016_converted_results_federal_election_2013(source_path, source_file_path, clean=False,
                                                                     quiet=False):
    source_file_name, source_file_extension = os.path.splitext(source_file_path)
    file_path_csv = f"{source_file_name}.csv"

    # Check if result needs to be generated
    if not clean and os.path.exists(file_path_csv):
        if not quiet:
            print(f"✓ Already exists {os.path.basename(file_path_csv)}")
        return

    # Determine engine
    engine = build_engine(source_file_extension)

    try:
        sheet_name = "2013_Zweitstimme"
        skiprows = 1
        names = ["ID", "Bezirksnummer", "Bezirksname", "Wahlbezirk", "Wahlbezirksart", "Abgeordnetenhauswahlkreis",
                 "Bundestagswahlkreis", "Wahlberechtigte insgesamt", "Wähler", "Ungültige Stimmen", "Gültige Stimmen",
                 "SPD", "SPD in Prozent", "CDU", "CDU in Prozent", "GRÜNE", "GRÜNE in Prozent", "DIE LINKE",
                 "DIE LINKE in Prozent", "PIRATEN", "PIRATEN in Prozent", "NPD", "NPD in Prozent", "FDP",
                 "FDP in Prozent", "AfD", "AfD in Prozent", "Sonstige", "Sonstige in Prozent"]
        drop_columns = []

        dataframe = pd.read_excel(source_file_path, engine=engine, sheet_name=sheet_name, skiprows=skiprows,
                                  header=None, usecols=list(range(0, len(names))), names=names) \
            .drop(columns=drop_columns, errors="ignore") \
            .dropna()

        # Write csv file
        write_csv_file(dataframe, file_path_csv, quiet)
    except Exception as e:
        print(f"✗️ Exception: {str(e)}")


def convert_file_to_csv_2016_electoral_statistics_1(source_path, source_file_path, clean=False,
                                                    quiet=False):
    source_file_name, source_file_extension = os.path.splitext(source_file_path)
    file_path_csv = f"{source_file_name}-eligible-voters-by-age-and-sex.csv"

    # Check if result needs to be generated
    if not clean and os.path.exists(file_path_csv):
        if not quiet:
            print(f"✓ Already exists {os.path.basename(file_path_csv)}")
        return

    # Determine engine
    engine = build_engine(source_file_extension)

    try:
        sheet_name = "1"
        skiprows = 8
        names = ["age", "total_2016_percentage", "male_2016_percentage", "female_2016_percentage",
                 "total_2011_percentage", "male_2011_percentage", "female_2011_percentage",
                 "2016_vs_2011_total", "2016_vs_2011_male", "2016_vs_2011_female"]
        drop_columns = []

        dataframe = pd.read_excel(source_file_path, engine=engine, sheet_name=sheet_name, skiprows=skiprows,
                                  header=0, usecols=list(range(0, len(names))), names=names) \
            .drop(columns=drop_columns, errors="ignore") \
            .head(10) \
            .dropna()

        # Write csv file
        write_csv_file(dataframe, file_path_csv, quiet)
    except Exception as e:
        print(f"✗️ Exception: {str(e)}")

    source_file_name, source_file_extension = os.path.splitext(source_file_path)
    file_path_csv = f"{source_file_name}-eligible-voters-by-age-and-sex-berlin-east.csv"

    # Check if result needs to be generated
    if not clean and os.path.exists(file_path_csv):
        if not quiet:
            print(f"✓ Already exists {os.path.basename(file_path_csv)}")
        return

    # Determine engine
    engine = build_engine(source_file_extension)

    try:
        sheet_name = "1"
        skiprows = 21
        names = ["age", "total_2016_percentage", "male_2016_percentage", "female_2016_percentage",
                 "total_2011_percentage", "male_2011_percentage", "female_2011_percentage",
                 "2016_vs_2011_total", "2016_vs_2011_male", "2016_vs_2011_female"]
        drop_columns = []

        dataframe = pd.read_excel(source_file_path, engine=engine, sheet_name=sheet_name, skiprows=skiprows,
                                  header=0, usecols=list(range(0, len(names))), names=names) \
            .drop(columns=drop_columns, errors="ignore") \
            .head(10) \
            .dropna()

        # Write csv file
        write_csv_file(dataframe, file_path_csv, quiet)
    except Exception as e:
        print(f"✗️ Exception: {str(e)}")

    source_file_name, source_file_extension = os.path.splitext(source_file_path)
    file_path_csv = f"{source_file_name}-eligible-voters-by-age-and-sex-berlin-west.csv"

    # Check if result needs to be generated
    if not clean and os.path.exists(file_path_csv):
        if not quiet:
            print(f"✓ Already exists {os.path.basename(file_path_csv)}")
        return

    # Determine engine
    engine = build_engine(source_file_extension)

    try:
        sheet_name = "1"
        skiprows = 33
        names = ["age", "total_2016_percentage", "male_2016_percentage", "female_2016_percentage",
                 "total_2011_percentage", "male_2011_percentage", "female_2011_percentage",
                 "2016_vs_2011_total", "2016_vs_2011_male", "2016_vs_2011_female"]
        drop_columns = []

        dataframe = pd.read_excel(source_file_path, engine=engine, sheet_name=sheet_name, skiprows=skiprows,
                                  header=0, usecols=list(range(0, len(names))), names=names) \
            .drop(columns=drop_columns, errors="ignore") \
            .head(10) \
            .dropna()

        # Write csv file
        write_csv_file(dataframe, file_path_csv, quiet)
    except Exception as e:
        print(f"✗️ Exception: {str(e)}")


def convert_file_to_csv_2016_electoral_statistics_2(source_path, source_file_path, clean=False,
                                                    quiet=False):
    source_file_name, source_file_extension = os.path.splitext(source_file_path)
    file_path_csv = f"{source_file_name}-ballot-paper-recipients-by-age-and-sex.csv"

    # Check if result needs to be generated
    if not clean and os.path.exists(file_path_csv):
        if not quiet:
            print(f"✓ Already exists {os.path.basename(file_path_csv)}")
        return

    # Determine engine
    engine = build_engine(source_file_extension)

    try:
        sheet_name = "2"
        skiprows = 8
        names = ["age", "total_2016_percentage", "male_2016_percentage", "female_2016_percentage",
                 "total_2011_percentage", "male_2011_percentage", "female_2011_percentage",
                 "2016_vs_2011_total", "2016_vs_2011_male", "2016_vs_2011_female"]
        drop_columns = []

        dataframe = pd.read_excel(source_file_path, engine=engine, sheet_name=sheet_name, skiprows=skiprows,
                                  header=0, usecols=list(range(0, len(names))), names=names) \
            .drop(columns=drop_columns, errors="ignore") \
            .head(10) \
            .dropna()

        # Write csv file
        write_csv_file(dataframe, file_path_csv, quiet)
    except Exception as e:
        print(f"✗️ Exception: {str(e)}")

    source_file_name, source_file_extension = os.path.splitext(source_file_path)
    file_path_csv = f"{source_file_name}-ballot-paper-recipients-by-age-and-sex-berlin-east.csv"

    # Check if result needs to be generated
    if not clean and os.path.exists(file_path_csv):
        if not quiet:
            print(f"✓ Already exists {os.path.basename(file_path_csv)}")
        return

    # Determine engine
    engine = build_engine(source_file_extension)

    try:
        sheet_name = "2"
        skiprows = 20
        names = ["age", "total_2016_percentage", "male_2016_percentage", "female_2016_percentage",
                 "total_2011_percentage", "male_2011_percentage", "female_2011_percentage",
                 "2016_vs_2011_total", "2016_vs_2011_male", "2016_vs_2011_female"]
        drop_columns = []

        dataframe = pd.read_excel(source_file_path, engine=engine, sheet_name=sheet_name, skiprows=skiprows,
                                  header=0, usecols=list(range(0, len(names))), names=names) \
            .drop(columns=drop_columns, errors="ignore") \
            .head(10) \
            .dropna()

        # Write csv file
        write_csv_file(dataframe, file_path_csv, quiet)
    except Exception as e:
        print(f"✗️ Exception: {str(e)}")

    source_file_name, source_file_extension = os.path.splitext(source_file_path)
    file_path_csv = f"{source_file_name}-ballot-paper-recipients-by-age-and-sex-berlin-west.csv"

    # Check if result needs to be generated
    if not clean and os.path.exists(file_path_csv):
        if not quiet:
            print(f"✓ Already exists {os.path.basename(file_path_csv)}")
        return

    # Determine engine
    engine = build_engine(source_file_extension)

    try:
        sheet_name = "2"
        skiprows = 33
        names = ["age", "total_2016_percentage", "male_2016_percentage", "female_2016_percentage",
                 "total_2011_percentage", "male_2011_percentage", "female_2011_percentage",
                 "2016_vs_2011_total", "2016_vs_2011_male", "2016_vs_2011_female"]
        drop_columns = []

        dataframe = pd.read_excel(source_file_path, engine=engine, sheet_name=sheet_name, skiprows=skiprows,
                                  header=0, usecols=list(range(0, len(names))), names=names) \
            .drop(columns=drop_columns, errors="ignore") \
            .head(10) \
            .dropna()

        # Write csv file
        write_csv_file(dataframe, file_path_csv, quiet)
    except Exception as e:
        print(f"✗️ Exception: {str(e)}")


def convert_file_to_csv_2016_electoral_statistics_3(source_path, source_file_path, clean=False,
                                                    quiet=False):
    source_file_name, source_file_extension = os.path.splitext(source_file_path)
    file_path_csv = f"{source_file_name}-voter-turnout-by-age-and-sex.csv"

    # Check if result needs to be generated
    if not clean and os.path.exists(file_path_csv):
        if not quiet:
            print(f"✓ Already exists {os.path.basename(file_path_csv)}")
        return

    # Determine engine
    engine = build_engine(source_file_extension)

    try:
        sheet_name = "3"
        skiprows = 8
        names = ["age", "total_2016_percentage", "male_2016_percentage", "female_2016_percentage",
                 "total_2011_percentage", "male_2011_percentage", "female_2011_percentage",
                 "2016_vs_2011_total", "2016_vs_2011_male", "2016_vs_2011_female"]
        drop_columns = []

        dataframe = pd.read_excel(source_file_path, engine=engine, sheet_name=sheet_name, skiprows=skiprows,
                                  header=0, usecols=list(range(0, len(names))), names=names) \
            .drop(columns=drop_columns, errors="ignore") \
            .head(10) \
            .dropna()

        # Write csv file
        write_csv_file(dataframe, file_path_csv, quiet)
    except Exception as e:
        print(f"✗️ Exception: {str(e)}")

    source_file_name, source_file_extension = os.path.splitext(source_file_path)
    file_path_csv = f"{source_file_name}-voter-turnout-by-age-and-sex-berlin-east.csv"

    # Check if result needs to be generated
    if not clean and os.path.exists(file_path_csv):
        if not quiet:
            print(f"✓ Already exists {os.path.basename(file_path_csv)}")
        return

    # Determine engine
    engine = build_engine(source_file_extension)

    try:
        sheet_name = "3"
        skiprows = 21
        names = ["age", "total_2016_percentage", "male_2016_percentage", "female_2016_percentage",
                 "total_2011_percentage", "male_2011_percentage", "female_2011_percentage",
                 "2016_vs_2011_total", "2016_vs_2011_male", "2016_vs_2011_female"]
        drop_columns = []

        dataframe = pd.read_excel(source_file_path, engine=engine, sheet_name=sheet_name, skiprows=skiprows,
                                  header=0, usecols=list(range(0, len(names))), names=names) \
            .drop(columns=drop_columns, errors="ignore") \
            .head(10) \
            .dropna()

        # Write csv file
        write_csv_file(dataframe, file_path_csv, quiet)
    except Exception as e:
        print(f"✗️ Exception: {str(e)}")

    source_file_name, source_file_extension = os.path.splitext(source_file_path)
    file_path_csv = f"{source_file_name}-voter-turnout-by-age-and-sex-berlin-west.csv"

    # Check if result needs to be generated
    if not clean and os.path.exists(file_path_csv):
        if not quiet:
            print(f"✓ Already exists {os.path.basename(file_path_csv)}")
        return

    # Determine engine
    engine = build_engine(source_file_extension)

    try:
        sheet_name = "3"
        skiprows = 34
        names = ["age", "total_2016_percentage", "male_2016_percentage", "female_2016_percentage",
                 "total_2011_percentage", "male_2011_percentage", "female_2011_percentage",
                 "2016_vs_2011_total", "2016_vs_2011_male", "2016_vs_2011_female"]
        drop_columns = []

        dataframe = pd.read_excel(source_file_path, engine=engine, sheet_name=sheet_name, skiprows=skiprows,
                                  header=0, usecols=list(range(0, len(names))), names=names) \
            .drop(columns=drop_columns, errors="ignore") \
            .head(10) \
            .dropna()

        # Write csv file
        write_csv_file(dataframe, file_path_csv, quiet)
    except Exception as e:
        print(f"✗️ Exception: {str(e)}")


def convert_file_to_csv_2016_structural_data_1_1(source_path, source_file_path, clean=False, quiet=False):
    source_file_name, source_file_extension = os.path.splitext(source_file_path)
    file_path_csv = f"{source_file_name}-structural-data-1-1.csv"

    # Check if result needs to be generated
    if not clean and os.path.exists(file_path_csv):
        if not quiet:
            print(f"✓ Already exists {os.path.basename(file_path_csv)}")
        return

    # Determine engine
    engine = build_engine(source_file_extension)

    try:
        sheet_name = "1.1"
        skiprows = 6
        names = ["feature", "berlin_elections_2011", "berlin_elections_2011_percentage",
                 "european_elections_2014", "european_elections_2014_percentage",
                 "federal_election_2013", "federal_election_2013_percentage"]
        drop_columns = []

        dataframe = pd.read_excel(source_file_path, engine=engine, sheet_name=sheet_name, skiprows=skiprows,
                                  header=0, usecols=list(range(0, len(names))), names=names) \
            .drop(columns=drop_columns, errors="ignore") \
            .dropna()

        # Write csv file
        write_csv_file(dataframe, file_path_csv, quiet)
    except Exception as e:
        print(f"✗️ Exception: {str(e)}")


def convert_file_to_csv_2016_structural_data_1_2(source_path, source_file_path, clean=False, quiet=False):
    source_file_name, source_file_extension = os.path.splitext(source_file_path)
    file_path_csv = f"{source_file_name}-structural-data-1-2-berlin-elections-by-district-first-vote.csv"

    # Check if result needs to be generated
    if not clean and os.path.exists(file_path_csv):
        if not quiet:
            print(f"✓ Already exists {os.path.basename(file_path_csv)}")
        return

    # Determine engine
    engine = build_engine(source_file_extension)

    try:
        sheet_name = "1.2+1.3"
        skiprows = 7
        names = ["district", "eligible_voters", "voters", "voter_turnout", "valid_votes",
                 "SPD", "CDU", "GRÜNE", "DIE LINKE", "PIRATEN"]
        drop_columns = []

        dataframe = pd.read_excel(source_file_path, engine=engine, sheet_name=sheet_name, skiprows=skiprows,
                                  header=0, usecols=list(range(0, len(names))), names=names) \
            .drop(columns=drop_columns, errors="ignore") \
            .head(12) \
            .dropna()

        # Write csv file
        write_csv_file(dataframe, file_path_csv, quiet)
    except Exception as e:
        print(f"✗️ Exception: {str(e)}")


def convert_file_to_csv_2016_structural_data_1_3(source_path, source_file_path, clean=False, quiet=False):
    source_file_name, source_file_extension = os.path.splitext(source_file_path)
    file_path_csv = f"{source_file_name}-structural-data-1-3-berlin-elections-by-district-second-vote.csv"

    # Check if result needs to be generated
    if not clean and os.path.exists(file_path_csv):
        if not quiet:
            print(f"✓ Already exists {os.path.basename(file_path_csv)}")
        return

    # Determine engine
    engine = build_engine(source_file_extension)

    try:
        sheet_name = "1.2+1.3"
        skiprows = 27
        names = ["constituency_association", "eligible_voters", "voters", "voter_turnout", "valid_votes",
                 "SPD", "CDU", "GRÜNE", "DIE LINKE", "PIRATEN", "NPD", "FDP", "others"]
        drop_columns = []

        dataframe = pd.read_excel(source_file_path, engine=engine, sheet_name=sheet_name, skiprows=skiprows,
                                  header=0, usecols=list(range(0, len(names))), names=names) \
            .drop(columns=drop_columns, errors="ignore") \
            .head(12) \
            .dropna()

        # Write csv file
        write_csv_file(dataframe, file_path_csv, quiet)
    except Exception as e:
        print(f"✗️ Exception: {str(e)}")


def convert_file_to_csv_2016_structural_data_1_4(source_path, source_file_path, clean=False, quiet=False):
    source_file_name, source_file_extension = os.path.splitext(source_file_path)
    file_path_csv = f"{source_file_name}-structural-data-1-4-berlin-election-2011-by-district.csv"

    # Check if result needs to be generated
    if not clean and os.path.exists(file_path_csv):
        if not quiet:
            print(f"✓ Already exists {os.path.basename(file_path_csv)}")
        return

    # Determine engine
    engine = build_engine(source_file_extension)

    try:
        sheet_name = "1.4+1.5"
        skiprows = 7
        names = ["district", "mandates",
                 "mandates_SPD", "mandates_CDU", "mandates_GRÜNE", "mandates_DIE LINKE", "mandates_PIRATEN",
                 "surplus_mandates_SPD", "surplus_mandates_CDU",
                 "equalizing_mandate_CDU", "equalizing_mandate_GRÜNE", "equalizing_mandate_DIE LINKE",
                 "equalizing_mandate_PIRATEN"]
        drop_columns = []

        dataframe = pd.read_excel(source_file_path, engine=engine, sheet_name=sheet_name, skiprows=skiprows,
                                  header=0, usecols=list(range(0, len(names))), names=names) \
            .drop(columns=drop_columns, errors="ignore") \
            .head(12) \
            .dropna()

        # Write csv file
        write_csv_file(dataframe, file_path_csv, quiet)
    except Exception as e:
        print(f"✗️ Exception: {str(e)}")


def convert_file_to_csv_2016_structural_data_1_5(source_path, source_file_path, clean=False, quiet=False):
    source_file_name, source_file_extension = os.path.splitext(source_file_path)
    file_path_csv = f"{source_file_name}-structural-data-1-5-berlin-elections-since-1990-second-vote.csv"

    # Check if result needs to be generated
    if not clean and os.path.exists(file_path_csv):
        if not quiet:
            print(f"✓ Already exists {os.path.basename(file_path_csv)}")
        return

    # Determine engine
    engine = build_engine(source_file_extension)

    try:
        sheet_name = "1.4+1.5"
        skiprows = 33
        skipcols = 14
        names = ["year", "SPD", "CDU", "DIE LINKE", "GRÜNE", "FDP", "PIRATEN"]
        drop_columns = []

        dataframe = pd.read_excel(source_file_path, engine=engine, sheet_name=sheet_name, skiprows=skiprows,
                                  header=None, usecols=list(range(skipcols, skipcols + len(names))), names=names) \
            .drop(columns=drop_columns, errors="ignore")

        # Write csv file
        write_csv_file(dataframe, file_path_csv, quiet)
    except Exception as e:
        print(f"✗️ Exception: {str(e)}")


def convert_file_to_csv_2016_structural_data_1_6(source_path, source_file_path, clean=False, quiet=False):
    source_file_name, source_file_extension = os.path.splitext(source_file_path)

    for file_path_csv_suffix, skiprows, head in [
        (f"01-mitte.csv", 8, 7),
        (f"02-friedrichshain-kreuzberg.csv", 18, 5),
        (f"03-pankow.csv", 26, 9),
        (f"04-charlottenburg-wilmersdorf.csv", 38, 7),
        (f"05-spandau.csv", 48, 5),
        (f"06-steglitz-zehlendorf.csv", 56, 7),
        (f"07-tempelhof-schöneberg.csv", 66, 7),
        (f"08-neukölln.csv", 76, 7),
        (f"09-treptow-köpenick.csv", 86, 6),
        (f"10-marzahn-hellersdorf.csv", 95, 6),
        (f"11-lichtenberg.csv", 104, 6),
        (f"12-reinickendorf.csv", 113, 6),
    ]:
        file_path_csv = f"{source_file_name}-structural-data-1-6-berlin-election-2011-by-district-second-vote-{file_path_csv_suffix}"

        # Check if result needs to be generated
        if not clean and os.path.exists(file_path_csv):
            if not quiet:
                print(f"✓ Already exists {os.path.basename(file_path_csv)}")
            continue

        # Determine engine
        engine = build_engine(source_file_extension)

        try:
            sheet_name = "1.6"
            names = ["constituency_association", "eligible_voters", "voters", "voter_turnout", "valid_votes",
                     "SPD", "CDU", "GRÜNE", "DIE LINKE", "PIRATEN", "NPD", "FDP", "others"]
            drop_columns = []

            dataframe = pd.read_excel(source_file_path, engine=engine, sheet_name=sheet_name, skiprows=skiprows,
                                      header=None, usecols=list(range(0, len(names))), names=names) \
                .drop(columns=drop_columns, errors="ignore") \
                .head(head) \
                .dropna()

            # Write csv file
            write_csv_file(dataframe, file_path_csv, quiet)
        except Exception as e:
            print(f"✗️ Exception: {str(e)}")


def convert_file_to_csv_2016_structural_data_1_7(source_path, source_file_path, clean=False, quiet=False):
    source_file_name, source_file_extension = os.path.splitext(source_file_path)
    file_path_csv = f"{source_file_name}-structural-data-1-7-federal-elections-2013-by-district-second-vote.csv"

    # Check if result needs to be generated
    if not clean and os.path.exists(file_path_csv):
        if not quiet:
            print(f"✓ Already exists {os.path.basename(file_path_csv)}")
        return

    # Determine engine
    engine = build_engine(source_file_extension)

    try:
        sheet_name = "1.7"
        skiprows = 6
        names = ["district", "eligible_voters", "voters", "voter_turnout", "valid_votes",
                 "SPD", "CDU", "GRÜNE", "DIE LINKE", "PIRATEN", "NPD", "FDP", "AfD", "others"]
        drop_columns = []

        dataframe = pd.read_excel(source_file_path, engine=engine, sheet_name=sheet_name, skiprows=skiprows,
                                  header=None, usecols=list(range(0, len(names))), names=names) \
            .drop(columns=drop_columns, errors="ignore") \
            .head(12) \
            .dropna()

        # Write csv file
        write_csv_file(dataframe, file_path_csv, quiet)
    except Exception as e:
        print(f"✗️ Exception: {str(e)}")


def convert_file_to_csv_2016_structural_data_1_8(source_path, source_file_path, clean=False, quiet=False):
    source_file_name, source_file_extension = os.path.splitext(source_file_path)

    for file_path_csv_suffix, skiprows, head in [
        (f"01-mitte.csv", 7, 7),
        (f"02-friedrichshain-kreuzberg.csv", 17, 5),
        (f"03-pankow.csv", 25, 9),
        (f"04-charlottenburg-wilmersdorf.csv", 37, 7),
        (f"05-spandau.csv", 47, 5),
        (f"06-steglitz-zehlendorf.csv", 55, 7),
        (f"07-tempelhof-schöneberg.csv", 65, 7),
        (f"08-neukölln.csv", 75, 7),
        (f"09-treptow-köpenick.csv", 85, 6),
        (f"10-marzahn-hellersdorf.csv", 94, 6),
        (f"11-lichtenberg.csv", 103, 6),
        (f"12-reinickendorf.csv", 112, 6),
    ]:
        file_path_csv = f"{source_file_name}-structural-data-1-8-federal-elections-2013-by-district-second-vote-{file_path_csv_suffix}"

        # Check if result needs to be generated
        if not clean and os.path.exists(file_path_csv):
            if not quiet:
                print(f"✓ Already exists {os.path.basename(file_path_csv)}")
            continue

        # Determine engine
        engine = build_engine(source_file_extension)

        try:
            sheet_name = "1.8"
            names = ["constituency_association", "eligible_voters", "voters", "voter_turnout", "valid_votes",
                     "SPD", "CDU", "GRÜNE", "DIE LINKE", "PIRATEN", "NPD", "FDP", "AfD", "others"]
            drop_columns = []

            dataframe = pd.read_excel(source_file_path, engine=engine, sheet_name=sheet_name, skiprows=skiprows,
                                      header=None, usecols=list(range(0, len(names))), names=names) \
                .drop(columns=drop_columns, errors="ignore") \
                .head(head) \
                .dropna()

            # Write csv file
            write_csv_file(dataframe, file_path_csv, quiet)
        except Exception as e:
            print(f"✗️ Exception: {str(e)}")


def convert_file_to_csv_2016_structural_data_2_1(source_path, source_file_path, clean=False, quiet=False):
    source_file_name, source_file_extension = os.path.splitext(source_file_path)
    file_path_csv = f"{source_file_name}-structural-data-2-1-registered-inhabitants-december-2015.csv"

    # Check if result needs to be generated
    if not clean and os.path.exists(file_path_csv):
        if not quiet:
            print(f"✓ Already exists {os.path.basename(file_path_csv)}")
        return

    # Determine engine
    engine = build_engine(source_file_extension)

    try:
        sheet_name = "2.1"
        skiprows = 9
        names = ["district", "total", "foreigners", "foreigners_percentage", "foreigners_from_eu_percentage",
                 "germans_total", "germans_below_6", "germans_below_6_percentage",
                 "germans_between_6_and_18", "germans_between_6_and_18_percentage",
                 "germans_between_18_and_65", "germans_between_18_and_65_percentage",
                 "germans_above_65", "germans_above_65_percentage",
                 ]
        drop_columns = []

        dataframe = pd.read_excel(source_file_path, engine=engine, sheet_name=sheet_name, skiprows=skiprows,
                                  header=None, usecols=list(range(0, len(names))), names=names) \
            .drop(columns=drop_columns, errors="ignore") \
            .head(12) \
            .dropna()

        # Write csv file
        write_csv_file(dataframe, file_path_csv, quiet)
    except Exception as e:
        print(f"✗️ Exception: {str(e)}")

#
# Helpers
#

def build_engine(source_file_extension):
    return "openpyxl" if source_file_extension == ".xlsx" else None


def write_csv_file(dataframe, file_path, quiet):
    if dataframe.shape[0] > 0:
        dataframe.to_csv(file_path, index=False)
        if not quiet:
            print(f"✓ Convert {os.path.basename(file_path)}")
    else:
        if not quiet:
            print(dataframe.head())
            print(f"✗️ Empty {os.path.basename(file_path)}")
