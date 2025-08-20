import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Any, Tuple
from robot.api import logger


class DB2Comparison:
    def __init__(self):
        self.results = {}

    def compare_schemas(self, source_schema: List[Dict], target_schema: List[Dict],
                        table_name: str) -> Dict[str, Any]:
        """Compare source and target table schemas"""
        comparison_results = {
            'table_name': table_name,
            'column_order_match': True,
            'column_count_match': True,
            'column_details': [],
            'mismatches': []
        }

        # Convert to DataFrame for easier comparison
        source_df = pd.DataFrame(source_schema)
        target_df = pd.DataFrame(target_schema)

        # Column count validation
        if len(source_df) != len(target_df):
            comparison_results['column_count_match'] = False
            comparison_results['mismatches'].append(
                f"Column count mismatch: Source={len(source_df)}, Target={len(target_df)}"
            )

        # Column order and details validation
        for idx, (source_col, target_col) in enumerate(zip(source_schema, target_schema)):
            col_comparison = {
                'position': idx + 1,
                'source_column': source_col['COLNAME'],
                'target_column': target_col['COLNAME'],
                'name_match': source_col['COLNAME'] == target_col['COLNAME'],
                'type_match': source_col['TYPENAME'] == target_col['TYPENAME'],
                'length_match': source_col['LENGTH'] == target_col['LENGTH'],
                'scale_match': source_col['SCALE'] == target_col['SCALE'],
                'nullable_match': source_col['NULLS'] == target_col['NULLS']
            }

            if not all([col_comparison['name_match'], col_comparison['type_match']]):
                comparison_results['column_order_match'] = False

            comparison_results['column_details'].append(col_comparison)

        return comparison_results

    def compare_row_counts(self, source_count: int, target_count: int) -> Dict[str, Any]:
        """Compare row counts between source and target"""
        return {
            'source_count': source_count,
            'target_count': target_count,
            'match': source_count == target_count,
            'difference': abs(source_count - target_count)
        }

    def compare_aggregates(self, source_data: List[Dict], target_data: List[Dict],
                           numeric_columns: List[str], categorical_columns: List[str],
                           datetime_columns: List[str]) -> Dict[str, Any]:
        """Compare aggregate statistics"""
        source_df = pd.DataFrame(source_data)
        target_df = pd.DataFrame(target_data)

        results = {}

        # Numerical columns aggregation
        for col in numeric_columns:
            if col in source_df.columns and col in target_df.columns:
                source_agg = {
                    'mean': source_df[col].mean(),
                    'sum': source_df[col].sum(),
                    'min': source_df[col].min(),
                    'max': source_df[col].max(),
                    'count': source_df[col].count()
                }

                target_agg = {
                    'mean': target_df[col].mean(),
                    'sum': target_df[col].sum(),
                    'min': target_df[col].min(),
                    'max': target_df[col].max(),
                    'count': target_df[col].count()
                }

                results[col] = {
                    'source': source_agg,
                    'target': target_agg,
                    'matches': all([
                        abs(source_agg['mean'] - target_agg['mean']) < 0.01,
                        abs(source_agg['sum'] - target_agg['sum']) < 0.01,
                        source_agg['min'] == target_agg['min'],
                        source_agg['max'] == target_agg['max'],
                        source_agg['count'] == target_agg['count']
                    ])
                }

        return results

    def extract_ddl_metadata(self, schema_data: List[Dict]) -> Dict[str, Any]:
        """Extract DDL metadata from schema information"""
        metadata = {
            'columns': [],
            'primary_keys': [],
            'foreign_keys': [],
            'indexes': [],
            'constraints': []
        }

        for col in schema_data:
            column_meta = {
                'name': col.get('COLNAME'),
                'type': col.get('TYPENAME'),
                'length': col.get('LENGTH'),
                'scale': col.get('SCALE'),
                'nullable': col.get('NULLS'),
                'default_value': col.get('DEFAULT'),
                'position': col.get('COLNO')
            }
            metadata['columns'].append(column_meta)

        return metadata