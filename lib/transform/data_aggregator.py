import os

import pandas as pd

from config.data_transformation_gold_loader import DataTransformation
from tracking_decorator import TrackingDecorator


@TrackingDecorator.track_time
def aggregate_data(
    data_transformation: DataTransformation,
    source_path,
    results_path,
    clean=False,
    quiet=False,
):
    already_exists, converted, exception = 0, 0, 0

    for input_port in data_transformation.input_ports or []:
        for file in input_port.files or []:
            source_file_path = os.path.join(
                source_path, input_port.id, file.source_file_name
            )
            target_file_path = os.path.join(
                results_path, input_port.id, file.target_file_name
            )

            if not clean and os.path.exists(target_file_path):
                already_exists += 1
                not quiet and print(f"✓ Already exists {file.target_file_name}")
                continue

            try:
                with open(source_file_path, "r") as csv_file:
                    # Read csv file
                    dataframe = pd.read_csv(csv_file, dtype=str, keep_default_na=False)

                    names = file.names

                    # Apply trim
                    dataframe = dataframe.applymap(
                        lambda col: col.strip() if isinstance(col, str) else col
                    )

                    # Apply data type
                    dataframe = dataframe.astype(
                        {
                            name.name: name.type
                            for name in names
                            if name.name in dataframe.columns and name.action == "keep"
                        },
                        errors="ignore",
                    )

                    # Apply filter
                    dataframe = dataframe.filter(
                        items=[name.name for name in names if name.action == "keep"]
                    )

                    # Apply aggregation
                    if file.aggregate_by is not None:
                        if file.aggregate_by != "total":
                            dataframe = dataframe.apply(pd.to_numeric, errors="ignore")
                            dataframe = dataframe.groupby(
                                file.aggregate_by, as_index=False
                            ).sum()
                        else:
                            dataframe = dataframe.apply(pd.to_numeric, errors="ignore")
                            dataframe = pd.DataFrame(dataframe.sum()).transpose()
                            dataframe["id"] = 0
                            dataframe.insert(0, "id", dataframe.pop("id"))

                    # Apply zfill
                    dataframe = (
                        dataframe[
                            [name.name for name in names if name.action == "keep"]
                        ]
                        .astype(str)
                        .apply(
                            lambda col: col.str.zfill(
                                next(
                                    name.zfill if name.zfill is not None else 0
                                    for name in names
                                    if name.name == col.name
                                )
                            )
                        )
                    )

                    # Apply copy
                    for name in [name for name in names if name.action == "copy"]:
                        dataframe[name.name] = dataframe[name.copy]
                        dataframe.insert(0, name.name, dataframe.pop(name.name))

                    # Apply concatenation
                    for name in [
                        name for name in names if name.action == "concatenation"
                    ]:
                        dataframe[name.name] = dataframe[name.concat].agg(
                            "".join, axis=1
                        )
                        dataframe.insert(0, name.name, dataframe.pop(name.name))

                    # Apply fraction
                    for name in [name for name in names if name.action == "percentage"]:
                        dataframe[name.name] = (
                            dataframe[name.numerator]
                            .astype(float)
                            .divide(dataframe[name.denominator].astype(float))
                            .multiply(100)
                            .fillna(0)
                        )

                    os.makedirs(os.path.dirname(target_file_path), exist_ok=True)
                    dataframe.to_csv(target_file_path, index=False)
                    converted += 1

                    not quiet and print(
                        f"✓ Convert {os.path.basename(target_file_path)}"
                    )
            except Exception as e:
                exception += 1
                print(f"✗️ Exception: {str(e)}")
    print(
        f"aggregate_data finished with already_exists: {already_exists}, converted: {converted}, exception: {exception}"
    )
