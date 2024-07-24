import os
import shutil

from lib.tracking_decorator import TrackingDecorator


@TrackingDecorator.track_time
def copy_data(source_path, results_path, clean=False, quiet=False):
    # Iterate over files
    for subdir, dirs, files in sorted(os.walk(source_path)):

        # Make results path
        subdir = subdir.replace(f"{source_path}/", "")
        os.makedirs(os.path.join(results_path, subdir), exist_ok=True)

        for source_file_name in sorted(files):
            results_file_name = get_results_file_name(subdir, source_file_name)

            source_file_path = os.path.join(source_path, subdir, source_file_name)
            results_file_path = os.path.join(results_path, subdir, results_file_name)

            # Check if file needs to be copied
            if clean or not os.path.exists(results_file_path):
                shutil.copyfile(source_file_path, results_file_path)

                if not quiet:
                    print(f"✓ Copy {source_file_name}")
            else:
                print(f"✓ Already exists {source_file_name}")


def get_results_file_name(subdir, source_file_name):
    # European elections 2024

    if source_file_name == "DL_BE_EU2024.xlsx":
        return "berlin-election-results-european-election-2024-electoral-district-results.xlsx"
    if source_file_name == "DL_BE_2_EU2024.xlsx":
        return "berlin-election-results-european-election-2024-electoral-constituencies.xlsx"
    if source_file_name == "SB_B07-05-01_2024j05_BE.xlsx":
        return "berlin-election-results-european-election-2024-structural-data.xlsx"
    if source_file_name == "DL_BE_EU2024_EU2019.xlsx":
        return "berlin-election-results-european-election-2024-converted-results-european-election-2019.xlsx"
    if source_file_name == "DL_BE_EU_2024_Strukturdaten.xlsx":
        return "berlin-election-results-european-election-2024-structural-data-by-constituency.xlsx"
    if source_file_name == "Europawahlen_Lange-Reihe_2024_Berlin-Brandenburg.xlsx":
        return "berlin-election-results-european-election-2024-long-time-series.xlsx"

    # European elections 2019

    if source_file_name == "DL_BE_EU2019.xlsx":
        return "berlin-election-results-european-election-2019-electoral-district-results.xlsx"
    if source_file_name == "DL_BE_2_EU2019.xlsx":
        return "berlin-election-results-european-election-2019-constituencies.xlsx"
    if source_file_name == "SB_B07-05-01_2019j05_BE.xlsx":
        return "berlin-election-results-european-election-2019-structural-data.xlsx"
    if source_file_name == "DL_BE_EU2019_BT2017.xlsx":
        return "berlin-election-results-european-election-2019-converted-results-european-election-2017.xlsx"
    if source_file_name == "DL_BE_EU2019_EU2014.xlsx":
        return "berlin-election-results-european-election-2019-converted-results-european-election-2014.xlsx"
    if source_file_name == "DL_BE_EU2019_Strukturdaten.xlsx":
        return "berlin-election-results-european-election-2019-structural-data-by-constituency.xlsx"
    if source_file_name == "SB_B07-05-05_2019j05_BE.xlsx":
        return "berlin-election-results-european-election-2019-electoral-statistics.xlsx"
    if source_file_name == "europawahlen-lange-reihe-2019.xlsx":
        return "berlin-election-results-european-election-2019-long-time-series.xlsx"

    # Federal election 2024

    if source_file_name == "DL_BE_BU2024.xlsx":
        return "berlin-election-results-federal-election-2024-constituency-results.xlsx"
    if source_file_name == "SB_B07-01-01_2024j04_BE.xlsx":
        return "berlin-election-results-federal-election-2024-structural-data.xlsx"
    if source_file_name == "DL_BE_BT2024_BT2017.xlsx":
        return "berlin-election-results-federal-election-2024-converted-results-federal-election-2017.xlsx"
    if source_file_name == "DL_BE_BT2024_AH2016.xlsx":
        return "berlin-election-results-federal-election-2024-converted-results-berlin-elections-2016.xlsx"
    if source_file_name == "DL_BE_BTW2024_Strukturdaten_Wiederholung.xlsx":
        return "berlin-election-results-federal-election-2024-structural-data-by-constituency.xlsx"
    if source_file_name == "SB_B07-01-05_2021j04_BE.xlsx":
        return "berlin-election-results-federal-election-2024-electoral-statistics.xlsx"
    if "2024" in subdir and source_file_name == "bundestagswahlen-lange-reihe.xlsx":
        return "berlin-election-results-federal-election-2024-long-time-series.xlsx"

    # Federal election 2017

    if source_file_name == "DL_BE_EE_WB_BU2017.xlsx":
        return "berlin-election-results-federal-election-2017-constituency-results.xlsx"
    if source_file_name == "SB_B07-01-01_2017j04_BE.xlsx":
        return "berlin-election-results-federal-election-2017-structural-data.xlsx"
    if source_file_name == "DL_BE_BT2017_BT2013.xlsx":
        return "berlin-election-results-federal-election-2017-converted-results-federal-election-2013.xlsx"
    if source_file_name == "DL_BE_BT2017_AH2016.xlsx":
        return "berlin-election-results-federal-election-2017-converted-results-berlin-elections-2016.xlsx"
    if source_file_name == "SB_B07-01-05_2017j04_BE.xlsx":
        return "berlin-election-results-federal-election-2017-electoral-statistics.xlsx"
    if "2017" in subdir and source_file_name == "bundestagswahlen-lange-reihe.xlsx":
        return "berlin-election-results-federal-election-2017-long-time-series.xlsx"

    # Berlin elections 2023

    if source_file_name == "DL_BE_AGHBVV2023.xlsx":
        return "berlin-election-results-berlin-election-2023-constituency-results.xlsx"
    if source_file_name == "SB_B07-02-03_2023j05_BE.xlsx":
        return "berlin-election-results-berlin-election-2023-results.xlsx"
    if source_file_name == "SB_B07-02-01_2023j05_BE.xlsx":
        return "berlin-election-results-berlin-election-2023-structural-data.xlsx"
    if source_file_name == "DL_BE_AGH2023_BT2017.xlsx":
        return "berlin-election-results-berlin-election-2023-converted-results-federal-election-2017.xlsx"
    if source_file_name == "DL_BE_AGH2023_AH2016.xlsx":
        return "berlin-election-results-berlin-election-2023-converted-results-berlin-elections-2016.xlsx"
    if source_file_name == "SB_B07-02-05_2023j05_BE.xlsx":
        return "berlin-election-results-berlin-election-2023-electoral-statistics.xlsx"
    if source_file_name == "abgeordnetenhauswahlen-lange-reihe-2023-berlin.xlsx":
        return "berlin-election-results-berlin-election-2023-long-time-series.xlsx"

    # Berlin elections 2016

    if source_file_name == "DL_BE_AGHBVV2016.xlsx":
        return "berlin-election-results-berlin-election-2016-constituency-results.xlsx"
    if source_file_name == "SB_B07-02-01_2016j05_BE.xlsx":
        return "berlin-election-results-berlin-election-2016-structural-data.xlsx"
    if source_file_name == "DL_BE_AH2016_BT2013.xlsx":
        return "berlin-election-results-berlin-election-2016-converted-results-federal-election-2013.xlsx"
    if source_file_name == "DL_BE_AH2016_AH2011.xlsx":
        return "berlin-election-results-berlin-election-2016-converted-results-berlin-elections-2011.xlsx"
    if source_file_name == "SB_B07-02-05_2016j05_BE.xlsx":
        return "berlin-election-results-berlin-election-2016-electoral-statistics.xlsx"
    if source_file_name == "abgeordnetenhauswahlen-lange-reihe-2023-berlin.xlsx":
        return "berlin-election-results-berlin-election-2016-long-time-series.xlsx"

    # Referendums

    if source_file_name == "DL_BE_VE2023.xlsx":
        return "berlin-election-results-referendum-berlin-2030-klimaneutral.xlsx"
    if source_file_name == "DL_BE_VE2021.xlsx":
        return "berlin-election-results-referendum-deutsche-wohnen-und-co-enteignen.xlsx"
    if source_file_name == "DL_BE_EE_WB_VE2017.xlsx":
        return "berlin-election-results-referendum-on-the-continued-operation-of-tegel-airport.xlsx"
    if source_file_name == "DL_BE_VE2014.xlsx":
        return "berlin-election-results-referendum-on-the-preservation-of-tempelhofer-feld.xlsx"
    if source_file_name == "DL_BE_VE2013.xlsx":
        return "berlin-election-results-referendum-on-the-remunicipalisation-of-berlins-energy-supply.xlsx"
    if source_file_name == "DL_BE_VE2011.xlsx":
        return "berlin-election-results-referendum-on-the-disclosure-of-the-partial-privatisation-contracts-at-berliner-wasserbetriebe.xlsx"
    if source_file_name == "DL_BE_VE2009.xlsx":
        return "berlin-election-results-referendum-on-the-introduction-of-the-compulsory-elective-subject-ethics-religion.xlsx"
    if source_file_name == "DL_BE_VE2008.xlsx":
        return "berlin-election-results-referendum-tempelhof-bleibt-verkehrsflughafen.xlsx"

    else:
        return source_file_name
