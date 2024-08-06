import os
from dataclasses import asdict

import yaml

from config.data_product_manifest_loader import DataProductManifest, Port, Metadata
from lib.tracking_decorator import TrackingDecorator


class IndentDumper(yaml.Dumper):
    def increase_indent(self, flow=False, indentless=False):
        return super(IndentDumper, self).increase_indent(flow, False)


@TrackingDecorator.track_time
def update_data_product_manifest(data_product_manifest: DataProductManifest, config_path, data_paths, file_endings=()):
    data_product_manifest_path = os.path.join(config_path, "data-product.yml")

    data_product_manifest.output_ports = None

    for data_path in data_paths:
        for subdir, dirs, files in sorted(os.walk(data_path)):
            if [file for file in files if file.endswith(file_endings)]:
                id = subdir.replace(f"{data_path}/", "")

                repo_organization = data_product_manifest.metadata.owner.lower().replace(" ", "-")
                repo = f"open-lifeworlds-data-product-{data_product_manifest.id}"

                output_port = Port(
                    id=id,
                    metadata=Metadata(
                        name=id.replace("-", " ").title(),
                        owner=data_product_manifest.metadata.owner,
                        description="description",
                        url=f"https://github.com/{repo_organization}/{repo}/tree/main/data/{os.path.basename(data_path)}/{id}",
                        license=data_product_manifest.metadata.license,
                        updated=None,
                        schema=None,
                    ),
                    files=[
                        f"https://raw.githubusercontent.com/{repo_organization}/{repo}/main/data/{os.path.basename(data_path)}/{id}/{file}"
                        for file in sorted([file for file in files if file.endswith(file_endings)])
                    ]
                )

                if data_product_manifest.output_ports is None:
                    data_product_manifest.output_ports = []

                data_product_manifest.output_ports.append(output_port)

    def represent_none(self, _):
        return self.represent_scalar('tag:yaml.org,2002:null', '')

    yaml.add_representer(type(None), represent_none)

    with open(data_product_manifest_path, "w") as file:
        yaml.dump(asdict(data_product_manifest), file,
                  sort_keys=False, default_flow_style=False, Dumper=IndentDumper,
                  allow_unicode=True, width=float("inf"), explicit_start=True)
