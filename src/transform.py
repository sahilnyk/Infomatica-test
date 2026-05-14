"""
Transform Informatica metadata into NiFi-compatible format.
Converts Informatica mappings to NiFi processor configurations.
"""

import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from src.extract import RepositoryMetadata, Mapping, SourceDefinition, TargetDefinition, Transformation

logger = logging.getLogger(__name__)


@dataclass
class NiFiProcessor:
    """Represents a NiFi processor configuration."""
    name: str
    type: str
    properties: Dict[str, str] = field(default_factory=dict)
    auto_terminated_relationships: List[str] = field(default_factory=list)
    position_x: int = 0
    position_y: int = 0


@dataclass
class NiFiConnection:
    """Represents a connection between NiFi processors."""
    source_name: str
    destination_name: str
    relationships: List[str] = field(default_factory=lambda: ['success'])


@dataclass
class NiFiProcessGroup:
    """Represents a NiFi process group."""
    name: str
    processors: List[NiFiProcessor] = field(default_factory=list)
    connections: List[NiFiConnection] = field(default_factory=list)
    description: str = ""


class InformaticaToNiFiTransformer:
    """Transforms Informatica metadata to NiFi configurations."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize transformer with configuration.
        
        Args:
            config: Configuration dictionary with transformation settings
        """
        self.config = config
        self.processor_spacing = config.get('processor_spacing', 200)
        self.initial_x = config.get('initial_x', 100)
        self.initial_y = config.get('initial_y', 100)

    def transform_source_to_processor(
        self, 
        source: SourceDefinition, 
        position: int
    ) -> NiFiProcessor:
        """
        Transform Informatica source to NiFi processor.
        
        Args:
            source: Source definition
            position: Position index for layout
            
        Returns:
            NiFi processor configuration
        """
        processor_type = self._map_source_type(source.databasetype)
        
        properties = {
            'source-name': source.name,
            'description': source.description,
            'database-type': source.databasetype
        }
        
        if source.databasetype == 'Flat File':
            properties.update({
                'File to Process': '${source.file.path}',
                'Record Reader': 'CSVReader',
                'Schema Access Strategy': 'Use String Fields From Header'
            })
        elif source.databasetype in ['Oracle', 'SQL Server', 'DB2']:
            properties.update({
                'Database Connection Pooling Service': '${db.connection.pool}',
                'SQL select query': self._generate_select_query(source),
                'Max Wait Time': '0 seconds'
            })
        
        return NiFiProcessor(
            name=f"Read_{source.name}",
            type=processor_type,
            properties=properties,
            auto_terminated_relationships=['failure'],
            position_x=self.initial_x,
            position_y=self.initial_y + (position * self.processor_spacing)
        )

    def transform_target_to_processor(
        self, 
        target: TargetDefinition, 
        position: int
    ) -> NiFiProcessor:
        """
        Transform Informatica target to NiFi processor.
        
        Args:
            target: Target definition
            position: Position index for layout
            
        Returns:
            NiFi processor configuration
        """
        processor_type = self._map_target_type(target.databasetype)
        
        properties = {
            'target-name': target.name,
            'description': target.description,
            'database-type': target.databasetype
        }
        
        if target.databasetype == 'Flat File':
            properties.update({
                'Directory': '${target.output.directory}',
                'Record Writer': 'CSVRecordSetWriter',
                'Include Header Line': 'true'
            })
        elif target.databasetype in ['Oracle', 'SQL Server', 'DB2']:
            properties.update({
                'Database Connection Pooling Service': '${db.connection.pool}',
                'Statement Type': 'INSERT',
                'Table Name': target.tablename or target.name,
                'Translate Field Names': 'true'
            })
        
        return NiFiProcessor(
            name=f"Write_{target.name}",
            type=processor_type,
            properties=properties,
            auto_terminated_relationships=['failure', 'retry'],
            position_x=self.initial_x + (self.processor_spacing * 4),
            position_y=self.initial_y + (position * self.processor_spacing)
        )

    def transform_transformation_to_processor(
        self, 
        transformation: Transformation, 
        position: int
    ) -> Optional[NiFiProcessor]:
        """
        Transform Informatica transformation to NiFi processor.
        
        Args:
            transformation: Transformation definition
            position: Position index for layout
            
        Returns:
            NiFi processor configuration or None if not mappable
        """
        processor_type = self._map_transformation_type(transformation.type)
        
        if processor_type is None:
            logger.warning(f"Transformation type {transformation.type} not supported")
            return None
        
        properties = {
            'transformation-name': transformation.name,
            'transformation-type': transformation.type,
            'description': transformation.description
        }
        
        if transformation.type == 'Expression':
            properties['Record Reader'] = 'JsonTreeReader'
            properties['Record Writer'] = 'JsonRecordSetWriter'
            
            # Build expressions from transformation fields
            expressions = []
            for field in transformation.fields:
                if field.expression:
                    expressions.append(f"{field.name}={field.expression}")
            
            if expressions:
                properties['expressions'] = '; '.join(expressions)
        
        elif transformation.type == 'Filter':
            properties['Filter Expression'] = self._extract_filter_expression(transformation)
        
        elif transformation.type == 'Aggregator':
            properties['Correlation Attribute Name'] = 'group_key'
            properties['Aggregation Strategy'] = 'Custom'
        
        elif transformation.type == 'Joiner':
            properties['Join Type'] = 'INNER'
            properties['Join Strategy'] = 'Merge'
        
        elif transformation.type == 'Lookup':
            properties['Lookup Service'] = '${lookup.service}'
            properties['Lookup Key'] = self._extract_lookup_key(transformation)
        
        return NiFiProcessor(
            name=f"Transform_{transformation.name}",
            type=processor_type,
            properties=properties,
            auto_terminated_relationships=['failure', 'unmatched'],
            position_x=self.initial_x + (self.processor_spacing * 2),
            position_y=self.initial_y + (position * self.processor_spacing)
        )

    def transform_mapping_to_process_group(
        self, 
        mapping: Mapping, 
        metadata: RepositoryMetadata
    ) -> NiFiProcessGroup:
        """
        Transform complete Informatica mapping to NiFi process group.
        
        Args:
            mapping: Mapping definition
            metadata: Complete repository metadata
            
        Returns:
            NiFi process group configuration
        """
        process_group = NiFiProcessGroup(
            name=mapping.name,
            description=mapping.description
        )
        
        processor_map = {}
        position = 0
        
        # Transform sources
        for source_name in mapping.sources:
            if source_name in metadata.sources:
                processor = self.transform_source_to_processor(
                    metadata.sources[source_name], 
                    position
                )
                process_group.processors.append(processor)
                processor_map[source_name] = processor.name
                position += 1
        
        # Transform transformations
        for trans_name in mapping.transformations:
            if trans_name in metadata.transformations:
                processor = self.transform_transformation_to_processor(
                    metadata.transformations[trans_name], 
                    position
                )
                if processor:
                    process_group.processors.append(processor)
                    processor_map[trans_name] = processor.name
                    position += 1
        
        # Transform targets
        for target_name in mapping.targets:
            if target_name in metadata.targets:
                processor = self.transform_target_to_processor(
                    metadata.targets[target_name], 
                    position
                )
                process_group.processors.append(processor)
                processor_map[target_name] = processor.name
                position += 1
        
        # Transform connectors to connections
        for connector in mapping.connectors:
            source_proc = processor_map.get(connector.frominstance)
            dest_proc = processor_map.get(connector.toinstance)
            
            if source_proc and dest_proc:
                connection = NiFiConnection(
                    source_name=source_proc,
                    destination_name=dest_proc,
                    relationships=['success']
                )
                process_group.connections.append(connection)
        
        logger.info(f"Transformed mapping {mapping.name} to process group with "
                   f"{len(process_group.processors)} processors and "
                   f"{len(process_group.connections)} connections")
        
        return process_group

    def transform_all_mappings(
        self, 
        metadata: RepositoryMetadata
    ) -> List[NiFiProcessGroup]:
        """
        Transform all mappings to NiFi process groups.
        
        Args:
            metadata: Complete repository metadata
            
        Returns:
            List of NiFi process groups
        """
        process_groups = []
        
        for mapping_name, mapping in metadata.mappings.items():
            try:
                process_group = self.transform_mapping_to_process_group(mapping, metadata)
                process_groups.append(process_group)
            except Exception as e:
                logger.error(f"Error transforming mapping {mapping_name}: {e}")
        
        logger.info(f"Transformed {len(process_groups)} mappings to process groups")
        return process_groups

    def _map_source_type(self, db_type: str) -> str:
        """Map Informatica source type to NiFi processor type."""
        type_mapping = {
            'Flat File': 'org.apache.nifi.processors.standard.GetFile',
            'Oracle': 'org.apache.nifi.processors.standard.ExecuteSQL',
            'SQL Server': 'org.apache.nifi.processors.standard.ExecuteSQL',
            'DB2': 'org.apache.nifi.processors.standard.ExecuteSQL',
            'Teradata': 'org.apache.nifi.processors.standard.ExecuteSQL'
        }
        return type_mapping.get(db_type, 'org.apache.nifi.processors.standard.GetFile')

    def _map_target_type(self, db_type: str) -> str:
        """Map Informatica target type to NiFi processor type."""
        type_mapping = {
            'Flat File': 'org.apache.nifi.processors.standard.PutFile',
            'Oracle': 'org.apache.nifi.processors.standard.PutDatabaseRecord',
            'SQL Server': 'org.apache.nifi.processors.standard.PutDatabaseRecord',
            'DB2': 'org.apache.nifi.processors.standard.PutDatabaseRecord',
            'Teradata': 'org.apache.nifi.processors.standard.PutDatabaseRecord'
        }
        return type_mapping.get(db_type, 'org.apache.nifi.processors.standard.PutFile')

    def _map_transformation_type(self, trans_type: str) -> Optional[str]:
        """Map Informatica transformation type to NiFi processor type."""
        type_mapping = {
            'Expression': 'org.apache.nifi.processors.standard.UpdateRecord',
            'Filter': 'org.apache.nifi.processors.standard.QueryRecord',
            'Aggregator': 'org.apache.nifi.processors.standard.PartitionRecord',
            'Joiner': 'org.apache.nifi.processors.standard.JoinEnrichment',
            'Lookup': 'org.apache.nifi.processors.standard.LookupRecord',
            'Sorter': 'org.apache.nifi.processors.standard.PartitionRecord',
            'Router': 'org.apache.nifi.processors.standard.RouteOnAttribute'
        }
        return type_mapping.get(trans_type)

    def _generate_select_query(self, source: SourceDefinition) -> str:
        """Generate SQL SELECT query from source definition."""
        field_names = [field.name for field in source.fields]
        return f"SELECT {', '.join(field_names)} FROM {source.name}"

    def _extract_filter_expression(self, transformation: Transformation) -> str:
        """Extract filter expression from transformation."""
        for field in transformation.fields:
            if field.expression and 'WHERE' in field.expression.upper():
                return field.expression
        return "true"

    def _extract_lookup_key(self, transformation: Transformation) -> str:
        """Extract lookup key from transformation."""
        for field in transformation.fields:
            if field.porttype == 'INPUT':
                return field.name
        return "id"