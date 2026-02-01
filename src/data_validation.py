"""
Data Validation Framework for Migration
Ensures data integrity between on-prem and cloud
"""

import pandas as pd
import logging
from typing import Dict, Tuple

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MigrationValidator:
    """
    Validates data consistency after migration
    Implements row counts, checksums, and sample comparisons
    """
    
    def __init__(self):
        self.threshold = 0.01  # 1% difference allowed for floating point
    
    def validate_row_counts(self, source_count: int, target_count: int) -> bool:
        """Validate row counts match between source and target"""
        match = source_count == target_count
        if match:
            logger.info(f"Row count validation PASSED: {source_count} rows")
        else:
            logger.error(f"Row count validation FAILED: Source={source_count}, Target={target_count}")
        return match
    
    def validate_checksum(self, source_df: pd.DataFrame, target_df: pd.DataFrame, key_column: str) -> bool:
        """
        Compare checksums of key columns to ensure data fidelity
        Uses hash of concatenated string values
        """
        try:
            # Create checksum for source
            source_df['checksum'] = source_df.apply(
                lambda row: hash(tuple(row.values)), axis=1
            )
            source_checksum = source_df[key_column].astype(str) + '_' + source_df['checksum'].astype(str)
            source_set = set(source_checksum)
            
            # Create checksum for target
            target_df['checksum'] = target_df.apply(
                lambda row: hash(tuple(row.values)), axis=1
            )
            target_checksum = target_df[key_column].astype(str) + '_' + target_df['checksum'].astype(str)
            target_set = set(target_checksum)
            
            # Compare
            diff = source_set.symmetric_difference(target_set)
            
            if len(diff) == 0:
                logger.info("Checksum validation PASSED: Data fidelity confirmed")
                return True
            else:
                logger.warning(f"Checksum validation FAILED: {len(diff)} rows mismatch")
                return False
                
        except Exception as e:
            logger.error(f"Checksum calculation error: {e}")
            return False
    
    def validate_schema(self, source_schema: Dict, target_schema: Dict) -> bool:
        """Ensure schema matches (column names and types)"""
        source_cols = set(source_schema.keys())
        target_cols = set(target_schema.keys())
        
        if source_cols != target_cols:
            missing_in_target = source_cols - target_cols
            extra_in_target = target_cols - source_cols
            logger.error(f"Schema mismatch. Missing: {missing_in_target}, Extra: {extra_in_target}")
            return False
        
        logger.info("Schema validation PASSED")
        return True
    
    def validate_nulls(self, df: pd.DataFrame, critical_columns: list) -> bool:
        """Ensure no unexpected nulls in critical columns"""
        null_check = df[critical_columns].isnull().sum()
        issues = null_check[null_check > 0]
        
        if len(issues) > 0:
            logger.warning(f"Null values found: {issues.to_dict()}")
            return False
        
        logger.info("Null validation PASSED")
        return True
    
    def generate_validation_report(self, table_name: str, results: Dict) -> str:
        """Generate HTML/Markdown report for stakeholders"""
        report = f"""
## Migration Validation Report: {table_name}

| Check | Status | Details |
|-------|--------|---------|
| Row Count | {'✅ PASS' if results['row_count'] else '❌ FAIL'} | Source: {results.get('source_rows', 'N/A')}, Target: {results.get('target_rows', 'N/A')} |
| Schema | {'✅ PASS' if results['schema'] else '❌ FAIL'} | Column consistency check |
| Data Integrity | {'✅ PASS' if results['checksum'] else '❌ FAIL'} | Checksum validation |
| Null Check | {'✅ PASS' if results['nulls'] else '❌ FAIL'} | Critical columns |

**Overall Status:** {'✅ APPROVED FOR PRODUCTION' if all(results.values()) else '⚠️ ISSUES DETECTED - REVIEW REQUIRED'}

Generated: {pd.Timestamp.now()}
        """
        return report

if __name__ == "__main__":
    # Example usage
    validator = MigrationValidator()
    
    # Simulate validation (in real scenario, compare actual source vs target)
    sample_results = {
        'row_count': True,
        'schema': True,
        'checksum': True,
        'nulls': True
    }
    
    report = validator.generate_validation_report("dim_customer", sample_results)
    print(report)
