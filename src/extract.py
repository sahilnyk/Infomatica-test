"""
Extract Informatica repository mappings for forecast processing.
Parses Informatica PowerCenter XML repository exports to extract metadata.
"""

import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field, asdict
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


@dataclass
class SourceField:
    """Represents a source field definition."""
    name: str
    datatype: str
    length: int
    nullable: str
    fieldnumber: int
    keytype: str = "NOT A KEY"
    precision: Optional[int] = None
    scale: Optional[int] = None
    description: str = ""
    businessname: str = ""


@dataclass
class SourceDefinition:
    """Represents a source definition."""
    name: str
    databasetype: str
    description: str
    fields: List[SourceField] = field(default_factory=list)
    dbdname: str = ""
    objectversion: str = "1"


@dataclass
class TargetField:
    """Represents a target field definition."""
    name: str
    datatype: str
    length: int
    nullable: str
    fieldnumber: int
    keytype: str = "NOT A KEY"
    precision: Optional[int] = None
    scale: Optional[int] = None
    description: str = ""


@dataclass
class TargetDefinition:
    """Represents a target definition."""
    name: str
    databasetype: str
    description: str
    fields: List[TargetField] = field(default_factory=list)
    tablename: str = ""
    objectversion: str = "1"


@dataclass
class TransformationField:
    """Represents a transformation field."""
    name: str
    datatype: str
    precision: int
    scale: int
    expression: str = ""
    porttype: str = "INPUT/OUTPUT"


@dataclass
class Transformation:
    """Represents a transformation."""
    name: str
    type: str
    description: str
    fields: List[TransformationField] = field(default_factory=list)
    objectversion: str = "1"


@dataclass
class MappingConnector:
    """Represents a connector between mapping components."""
    frominstance: str
    fromfield: str
    toinstance: str
    tofield: str


@dataclass
class Mapping:
    """Represents a complete mapping."""
    name: str
    description: str
    sources: List[str] = field(default_factory=list)
    targets: List[str] = field(default_factory=list)
    transformations: List[str] = field(default_factory=list)
    connectors: List[MappingConnector] = field(default_factory=list)
    isvalid: str = "YES"
    objectversion: str = "1"


@dataclass
class RepositoryMetadata:
    """Complete repository metadata."""
    repository_name: str
    repository_version: str
    folder_name: str
    sources: Dict[str, SourceDefinition] = field(default_factory=dict)
    targets: Dict[str, TargetDefinition] = field(default_factory=dict)
    transformations: Dict[str, Transformation] = field(default_factory=dict)
    mappings: Dict[str, Mapping] = field(default_factory=dict)


class InformaticaRepositoryExtractor:
    """Extracts metadata from Informatica PowerCenter repository XML."""

    def __init__(self, xml_path: str):
        """
        Initialize extractor with XML file path.
        
        Args:
            xml_path: Path to Informatica repository XML file
        """
        self.xml_path = Path(xml_path)
        self.tree: Optional[ET.ElementTree] = None
        self.root: Optional[ET.Element] = None
        
    def parse_xml(self) -> None:
        """Parse the XML file."""
        try:
            logger.info(f"Parsing XML file: {self.xml_path}")
            self.tree = ET.parse(self.xml_path)
            self.root = self.tree.getroot()
            logger.info("XML parsing completed successfully")
        except ET.ParseError as e:
            logger.error(f"XML parsing error: {e}")
            raise
        except FileNotFoundError as e:
            logger.error(f"File not found: {self.xml_path}")
            raise

    def extract_repository_info(self) -> Dict[str, str]:
        """Extract repository-level information."""
        if self.root is None:
            raise ValueError("XML not parsed. Call parse_xml() first.")
        
        repo_elem = self.root.find('.//REPOSITORY')
        if repo_elem is None:
            raise ValueError("REPOSITORY element not found in XML")
        
        return {
            'name': repo_elem.get('NAME', ''),
            'version': repo_elem.get('VERSION', ''),
            'codepage': repo_elem.get('CODEPAGE', ''),
            'databasetype': repo_elem.get('DATABASETYPE', '')
        }

    def extract_folder_info(self) -> Dict[str, str]:
        """Extract folder-level information."""
        if self.root is None:
            raise ValueError("XML not parsed. Call parse_xml() first.")
        
        folder_elem = self.root.find('.//FOLDER')
        if folder_elem is None:
            raise ValueError("FOLDER element not found in XML")
        
        return {
            'name': folder_elem.get('NAME', ''),
            'owner': folder_elem.get('OWNER', ''),
            'description': folder_elem.get('DESCRIPTION', ''),
            'version': folder_elem.get('VERSION', '1')
        }

    def extract_sources(self) -> Dict[str, SourceDefinition]:
        """Extract all source definitions."""
        if self.root is None:
            raise ValueError("XML not parsed. Call parse_xml() first.")
        
        sources = {}
        source_elements = self.root.findall('.//SOURCE')
        
        logger.info(f"Found {len(source_elements)} source definitions")
        
        for source_elem in source_elements:
            source_name = source_elem.get('NAME', '')
            
            source_def = SourceDefinition(
                name=source_name,
                databasetype=source_elem.get('DATABASETYPE', ''),
                description=source_elem.get('DESCRIPTION', ''),
                dbdname=source_elem.get('DBDNAME', ''),
                objectversion=source_elem.get('OBJECTVERSION', '1')
            )
            
            # Extract fields
            for field_elem in source_elem.findall('SOURCEFIELD'):
                field = SourceField(
                    name=field_elem.get('NAME', ''),
                    datatype=field_elem.get('DATATYPE', ''),
                    length=int(field_elem.get('LENGTH', '0')),
                    nullable=field_elem.get('NULLABLE', 'NULL'),
                    fieldnumber=int(field_elem.get('FIELDNUMBER', '0')),
                    keytype=field_elem.get('KEYTYPE', 'NOT A KEY'),
                    precision=int(field_elem.get('PRECISION', '0')) if field_elem.get('PRECISION') else None,
                    scale=int(field_elem.get('SCALE', '0')) if field_elem.get('SCALE') else None,
                    description=field_elem.get('DESCRIPTION', ''),
                    businessname=field_elem.get('BUSINESSNAME', '')
                )
                source_def.fields.append(field)
            
            sources[source_name] = source_def
            logger.debug(f"Extracted source: {source_name} with {len(source_def.fields)} fields")
        
        return sources

    def extract_targets(self) -> Dict[str, TargetDefinition]:
        """Extract all target definitions."""
        if self.root is None:
            raise ValueError("XML not parsed. Call parse_xml() first.")
        
        targets = {}
        target_elements = self.root.findall('.//TARGET')
        
        logger.info(f"Found {len(target_elements)} target definitions")
        
        for target_elem in target_elements:
            target_name = target_elem.get('NAME', '')
            
            target_def = TargetDefinition(
                name=target_name,
                databasetype=target_elem.get('DATABASETYPE', ''),
                description=target_elem.get('DESCRIPTION', ''),
                tablename=target_elem.get('TABLENAME', ''),
                objectversion=target_elem.get('OBJECTVERSION', '1')
            )
            
            # Extract fields
            for field_elem in target_elem.findall('TARGETFIELD'):
                field = TargetField(
                    name=field_elem.get('NAME', ''),
                    datatype=field_elem.get('DATATYPE', ''),
                    length=int(field_elem.get('LENGTH', '0')),
                    nullable=field_elem.get('NULLABLE', 'NULL'),
                    fieldnumber=int(field_elem.get('FIELDNUMBER', '0')),
                    keytype=field_elem.get('KEYTYPE', 'NOT A KEY'),
                    precision=int(field_elem.get('PRECISION', '0')) if field_elem.get('PRECISION') else None,
                    scale=int(field_elem.get('SCALE', '0')) if field_elem.get('SCALE') else None,
                    description=field_elem.get('DESCRIPTION', '')
                )
                target_def.fields.append(field)
            
            targets[target_name] = target_def
            logger.debug(f"Extracted target: {target_name} with {len(target_def.fields)} fields")
        
        return targets

    def extract_transformations(self) -> Dict[str, Transformation]:
        """Extract all transformation definitions."""
        if self.root is None:
            raise ValueError("XML not parsed. Call parse_xml() first.")
        
        transformations = {}
        trans_elements = self.root.findall('.//TRANSFORMATION')
        
        logger.info(f"Found {len(trans_elements)} transformations")
        
        for trans_elem in trans_elements:
            trans_name = trans_elem.get('NAME', '')
            
            transformation = Transformation(
                name=trans_name,
                type=trans_elem.get('TYPE', ''),
                description=trans_elem.get('DESCRIPTION', ''),
                objectversion=trans_elem.get('OBJECTVERSION', '1')
            )
            
            # Extract transformation fields
            for field_elem in trans_elem.findall('TRANSFORMFIELD'):
                field = TransformationField(
                    name=field_elem.get('NAME', ''),
                    datatype=field_elem.get('DATATYPE', ''),
                    precision=int(field_elem.get('PRECISION', '0')),
                    scale=int(field_elem.get('SCALE', '0')),
                    expression=field_elem.get('EXPRESSION', ''),
                    porttype=field_elem.get('PORTTYPE', 'INPUT/OUTPUT')
                )
                transformation.fields.append(field)
            
            transformations[trans_name] = transformation
            logger.debug(f"Extracted transformation: {trans_name} ({transformation.type})")
        
        return transformations

    def extract_mappings(self) -> Dict[str, Mapping]:
        """Extract all mapping definitions."""
        if self.root is None:
            raise ValueError("XML not parsed. Call parse_xml() first.")
        
        mappings = {}
        mapping_elements = self.root.findall('.//MAPPING')
        
        logger.info(f"Found {len(mapping_elements)} mappings")
        
        for mapping_elem in mapping_elements:
            mapping_name = mapping_elem.get('NAME', '')
            
            mapping = Mapping(
                name=mapping_name,
                description=mapping_elem.get('DESCRIPTION', ''),
                isvalid=mapping_elem.get('ISVALID', 'YES'),
                objectversion=mapping_elem.get('OBJECTVERSION', '1')
            )
            
            # Extract source instances
            for source_inst in mapping_elem.findall('.//INSTANCE[@TRANSFORMATION_TYPE="Source Definition"]'):
                mapping.sources.append(source_inst.get('TRANSFORMATION_NAME', ''))
            
            # Extract target instances
            for target_inst in mapping_elem.findall('.//INSTANCE[@TRANSFORMATION_TYPE="Target Definition"]'):
                mapping.targets.append(target_inst.get('TRANSFORMATION_NAME', ''))
            
            # Extract transformation instances
            for trans_inst in mapping_elem.findall('.//INSTANCE'):
                trans_type = trans_inst.get('TRANSFORMATION_TYPE', '')
                if trans_type not in ['Source Definition', 'Target Definition']:
                    mapping.transformations.append(trans_inst.get('TRANSFORMATION_NAME', ''))
            
            # Extract connectors
            for connector_elem in mapping_elem.findall('.//CONNECTOR'):
                connector = MappingConnector(
                    frominstance=connector_elem.get('FROMINSTANCE', ''),
                    fromfield=connector_elem.get('FROMFIELD', ''),
                    toinstance=connector_elem.get('TOINSTANCE', ''),
                    tofield=connector_elem.get('TOFIELD', '')
                )
                mapping.connectors.append(connector)
            
            mappings[mapping_name] = mapping
            logger.debug(f"Extracted mapping: {mapping_name}")
        
        return mappings

    def extract_all(self) -> RepositoryMetadata:
        """Extract all metadata from the repository."""
        self.parse_xml()
        
        repo_info = self.extract_repository_info()
        folder_info = self.extract_folder_info()
        
        metadata = RepositoryMetadata(
            repository_name=repo_info['name'],
            repository_version=repo_info['version'],
            folder_name=folder_info['name'],
            sources=self.extract_sources(),
            targets=self.extract_targets(),
            transformations=self.extract_transformations(),
            mappings=self.extract_mappings()
        )
        
        logger.info(f"Extraction complete: {len(metadata.sources)} sources, "
                   f"{len(metadata.targets)} targets, "
                   f"{len(metadata.transformations)} transformations, "
                   f"{len(metadata.mappings)} mappings")
        
        return metadata

    def to_dict(self, metadata: RepositoryMetadata) -> Dict[str, Any]:
        """Convert metadata to dictionary format."""
        return {
            'repository_name': metadata.repository_name,
            'repository_version': metadata.repository_version,
            'folder_name': metadata.folder_name,
            'sources': {name: asdict(src) for name, src in metadata.sources.items()},
            'targets': {name: asdict(tgt) for name, tgt in metadata.targets.items()},
            'transformations': {name: asdict(trans) for name, trans in metadata.transformations.items()},
            'mappings': {name: asdict(map) for name, map in metadata.mappings.items()}
        }