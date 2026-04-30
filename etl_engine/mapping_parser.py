import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class TransformationConfig:
    name: str
    type: str
    fields: list = field(default_factory=list)


@dataclass
class MappingConfig:
    name: str
    sources: list = field(default_factory=list)
    targets: list = field(default_factory=list)
    transformations: list = field(default_factory=list)


def parse_repository(xml_path: str) -> list:
    path = Path(xml_path)
    if not path.exists():
        raise FileNotFoundError(f"Repository XML not found: {xml_path}")

    tree = ET.parse(xml_path)
    root = tree.getroot()
    mappings = []

    for folder in root.iter('FOLDER'):
        for mapping_elem in folder.iter('MAPPING'):
            mc = MappingConfig(name=mapping_elem.get('NAME', ''))

            for inst in mapping_elem.iter('INSTANCE'):
                inst_type = inst.get('TYPE', '')
                inst_name = inst.get('NAME', '')
                if inst_type == 'SOURCE':
                    mc.sources.append(inst_name)
                elif inst_type == 'TARGET':
                    mc.targets.append(inst_name)
                else:
                    tc = TransformationConfig(
                        name=inst_name,
                        type=inst.get('TRANSFORMATION_TYPE', inst_type),
                    )
                    mc.transformations.append(tc)

            for xform in mapping_elem.iter('TRANSFORMATION'):
                tc = TransformationConfig(
                    name=xform.get('NAME', ''),
                    type=xform.get('TYPE', ''),
                    fields=[tf.get('NAME', '') for tf in xform.iter('TRANSFORMFIELD')],
                )
                mc.transformations.append(tc)

            if mc.name:
                mappings.append(mc)

    if not mappings:
        for mapping_elem in root.iter('MAPPING'):
            mc = MappingConfig(name=mapping_elem.get('NAME', ''))
            for inst in mapping_elem.iter('INSTANCE'):
                inst_type = inst.get('TYPE', '')
                inst_name = inst.get('NAME', '')
                if inst_type == 'SOURCE':
                    mc.sources.append(inst_name)
                elif inst_type == 'TARGET':
                    mc.targets.append(inst_name)
            if mc.name:
                mappings.append(mc)

    return mappings


def get_mapping_names(xml_path: str) -> list:
    mappings = parse_repository(xml_path)
    return [m.name for m in mappings]
