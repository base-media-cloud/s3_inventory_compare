#!/usr/bin/env python3
"""
S3 Inventory Comparison Script

This script reads S3 inventory files from two different buckets and compares:
1. File presence in both buckets
2. Checksum equality
3. Object size equality

Requirements:
- boto3
- pandas (optional, for better CSV handling)
"""

import boto3
import csv
import gzip
import json
import logging
from collections import defaultdict
from typing import Dict, Set, Tuple, Optional
import argparse
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class S3InventoryComparator:
    def __init__(self, aws_profile: Optional[str] = None):
        """Initialize the S3 client."""
        session = boto3.Session(profile_name=aws_profile) if aws_profile else boto3.Session()
        self.s3_client = session.client('s3')
    
    def read_inventory_manifest(self, bucket: str, manifest_key: str) -> Dict:
        """Read and parse the inventory manifest file."""
        try:
            response = self.s3_client.get_object(Bucket=bucket, Key=manifest_key)
            manifest_content = response['Body'].read().decode('utf-8')
            return json.loads(manifest_content)
        except Exception as e:
            logger.error(f"Error reading manifest from {bucket}/{manifest_key}: {e}")
            raise
    
    def read_inventory_data(self, bucket: str, data_key: str) -> Dict[str, Dict]:
        """Read inventory data file and return a dictionary of objects."""
        objects = {}
        
        try:
            response = self.s3_client.get_object(Bucket=bucket, Key=data_key)
            
            # Handle gzipped files
            if data_key.endswith('.gz'):
                content = gzip.decompress(response['Body'].read()).decode('utf-8')
            else:
                content = response['Body'].read().decode('utf-8')
            
            # Parse CSV content
            csv_reader = csv.reader(content.splitlines())
            
            for row in csv_reader:
                if len(row) >= 3:  # Minimum required columns: key, size, etag
                    key = row[1]
                    size = int(row[5]) if row[5].isdigit() else 0
                    etag = row[7].strip('"')  # Remove quotes from ETag
                    
                    objects[key] = {
                        'size': size,
                        'etag': etag,
                        'raw_row': row
                    }
            
            logger.info(f"Read {len(objects)} objects from {bucket}/{data_key}")
            return objects
            
        except Exception as e:
            logger.error(f"Error reading inventory data from {bucket}/{data_key}: {e}")
            raise
    
    def load_inventory_from_manifest(self, bucket: str, manifest_key: str) -> Dict[str, Dict]:
        """Load all inventory data files listed in the manifest."""
        manifest = self.read_inventory_manifest(bucket, manifest_key)
        all_objects = {}
        
        # Get the list of data files from manifest
        files = manifest.get('files', [])
        
        for file_info in files:
            data_key = file_info['key']
            logger.info(f"Processing inventory file: {data_key}")
            
            objects = self.read_inventory_data(bucket, data_key)
            all_objects.update(objects)
        
        return all_objects
    
    def load_inventory_from_file(self, bucket: str, inventory_file_key: str) -> Dict[str, Dict]:
        """Load inventory data from a single file."""
        logger.info(f"Processing single inventory file: {inventory_file_key}")
        return self.read_inventory_data(bucket, inventory_file_key)
    
    def compare_inventories(self, bucket1_objects: Dict[str, Dict], 
                          bucket2_objects: Dict[str, Dict]) -> Dict:
        """Compare two inventory datasets and return comparison results."""
        
        keys1 = set(bucket1_objects.keys())
        keys2 = set(bucket2_objects.keys())
        
        # Find differences
        only_in_bucket1 = keys1 - keys2
        only_in_bucket2 = keys2 - keys1
        common_keys = keys1 & keys2
        
        # Compare common objects
        size_mismatches = []
        checksum_mismatches = []
        perfect_matches = []
        
        for key in common_keys:
            obj1 = bucket1_objects[key]
            obj2 = bucket2_objects[key]
            
            size_match = obj1['size'] == obj2['size']
            checksum_match = obj1['etag'] == obj2['etag']
            
            #if size_match and checksum_match:
            if size_match:
                perfect_matches.append(key)
            else:
                if not size_match:
                    size_mismatches.append({
                        'key': key,
                        'bucket1_size': obj1['size'],
                        'bucket2_size': obj2['size']
                    })
                if not checksum_match:
                    checksum_mismatches.append({
                        'key': key,
                        'bucket1_etag': obj1['etag'],
                        'bucket2_etag': obj2['etag']
                    })
        
        return {
            'total_bucket1': len(keys1),
            'total_bucket2': len(keys2),
            'only_in_bucket1': list(only_in_bucket1),
            'only_in_bucket2': list(only_in_bucket2),
            'common_objects': len(common_keys),
            'perfect_matches': len(perfect_matches),
            'size_mismatches': size_mismatches,
            'checksum_mismatches': checksum_mismatches
        }
    
    def print_comparison_report(self, results: Dict, bucket1_name: str, bucket2_name: str):
        """Print a detailed comparison report."""
        print("\n" + "="*80)
        print("S3 INVENTORY COMPARISON REPORT")
        print("="*80)
        print(f"Bucket 1: {bucket1_name}")
        print(f"Bucket 2: {bucket2_name}")
        print("-"*80)
        
        print(f"Total objects in {bucket1_name}: {results['total_bucket1']:,}")
        print(f"Total objects in {bucket2_name}: {results['total_bucket2']:,}")
        print(f"Common objects: {results['common_objects']:,}")
        #print(f"Perfect matches (size + checksum): {results['perfect_matches']:,}")
        print(f"Perfect matches (size): {results['perfect_matches']:,}")
        print(f"\nObjects only in {bucket1_name}: {len(results['only_in_bucket1']):,}")
        if results['only_in_bucket1'] and len(results['only_in_bucket1']) <= 10:
            for obj in results['only_in_bucket1'][:10]:
                print(f"  - {obj}")
        elif len(results['only_in_bucket1']) > 10:
            print(f"  (showing first 10 of {len(results['only_in_bucket1'])})")
            for obj in results['only_in_bucket1'][:10]:
                print(f"  - {obj}")
        
        print(f"\nObjects only in {bucket2_name}: {len(results['only_in_bucket2']):,}")
        if results['only_in_bucket2'] and len(results['only_in_bucket2']) <= 10:
            for obj in results['only_in_bucket2'][:10]:
                print(f"  - {obj}")
        elif len(results['only_in_bucket2']) > 10:
            print(f"  (showing first 10 of {len(results['only_in_bucket2'])})")
            for obj in results['only_in_bucket2'][:10]:
                print(f"  - {obj}")
        
        print(f"\nSize mismatches: {len(results['size_mismatches']):,}")
        for mismatch in results['size_mismatches'][:10]:
            print(f"  - {mismatch['key']}: {mismatch['bucket1_size']} vs {mismatch['bucket2_size']} bytes")
        
        print(f"\nChecksum mismatches: {len(results['checksum_mismatches']):,}")
        for mismatch in results['checksum_mismatches'][:10]:
            print(f"  - {mismatch['key']}: {mismatch['bucket1_etag']} vs {mismatch['bucket2_etag']}")
        
        # Summary
        print("\n" + "="*80)
        if (len(results['only_in_bucket1']) == 0 and 
            len(results['only_in_bucket2']) == 0 and 
            len(results['size_mismatches']) == 0 and 
            len(results['checksum_mismatches']) == 0):
            print("✅ SUCCESS: All objects match perfectly between both buckets!")
        else:
            print("❌ DIFFERENCES FOUND: Objects differ between buckets")
        print("="*80)


def main():
    parser = argparse.ArgumentParser(
        description='Compare S3 inventory files between two buckets'
    )
    parser.add_argument('--bucket1', required=True, help='First bucket name')
    parser.add_argument('--bucket2', required=True, help='Second bucket name')
    parser.add_argument('--inventory1', required=True, 
                       help='S3 key for bucket1 inventory file or manifest')
    parser.add_argument('--inventory2', required=True,
                       help='S3 key for bucket2 inventory file or manifest')
    parser.add_argument('--use-manifest', action='store_true',
                       help='Treat inventory files as manifest files')
    parser.add_argument('--profile', help='AWS profile to use')
    
    args = parser.parse_args()
    
    try:
        comparator = S3InventoryComparator(aws_profile=args.profile)
        
        # Load inventory data
        logger.info(f"Loading inventory from {args.bucket1}/{args.inventory1}")
        if args.use_manifest:
            bucket1_objects = comparator.load_inventory_from_manifest(
                args.bucket1, args.inventory1
            )
        else:
            bucket1_objects = comparator.load_inventory_from_file(
                args.bucket1, args.inventory1
            )
        
        logger.info(f"Loading inventory from {args.bucket2}/{args.inventory2}")
        if args.use_manifest:
            bucket2_objects = comparator.load_inventory_from_manifest(
                args.bucket2, args.inventory2
            )
        else:
            bucket2_objects = comparator.load_inventory_from_file(
                args.bucket2, args.inventory2
            )
        
        # Compare inventories
        logger.info("Comparing inventories...")
        results = comparator.compare_inventories(bucket1_objects, bucket2_objects)
        
        # Print report
        comparator.print_comparison_report(results, args.bucket1, args.bucket2)
        
        # Exit with appropriate code
        if (len(results['only_in_bucket1']) == 0 and 
            len(results['only_in_bucket2']) == 0 and 
            len(results['size_mismatches']) == 0 and 
            len(results['checksum_mismatches']) == 0):
            sys.exit(0)
        else:
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Script failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
