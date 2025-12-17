#!/usr/bin/env python3
"""
Fix corrupted dates in CSV files based on sequential order.
The script infers correct years from the sequence pattern.

Example:
  0967-12-04 → should be based on sequence position
  2966-08-06 → 1966-08-06 based on sequence
  3965-12-07 → 1965-12-07 based on sequence  
  4968-01-03 → 1972-12-30 (before 1973 in sequence)
"""

import os
import re
from datetime import datetime

BASE_DIR = "/Users/s0m13i5/walmart/kafka-db2-projects/precious_metals_data/macrotrends_organized"

def is_valid_year(year_int):
    """Check if year is in valid range."""
    return 1800 <= year_int <= 2100

def parse_date(date_str):
    """Parse a date string, return (year, month, day) or None if invalid format."""
    if not date_str or len(date_str) < 10:
        return None
    match = re.match(r'^(\d{4})-(\d{2})-(\d{2})$', date_str)
    if not match:
        return None
    return int(match.group(1)), int(match.group(2)), int(match.group(3))

def format_date(year, month, day):
    """Format date as YYYY-MM-DD."""
    return f"{year:04d}-{month:02d}-{day:02d}"

def process_csv(filepath):
    """Process a single CSV file and fix dates based on sequence."""
    fixed_count = 0
    
    # Read all lines
    with open(filepath, 'r', encoding='utf-8') as f:
        lines = [line.rstrip('\n') for line in f]
    
    if len(lines) < 2:
        return 0
    
    header = lines[0]
    data_lines = lines[1:]
    
    # Parse all dates and values
    records = []
    for line in data_lines:
        parts = line.split(',')
        if len(parts) >= 2:
            date_str = parts[0]
            value = ','.join(parts[1:])
            parsed = parse_date(date_str)
            records.append({
                'original_date': date_str,
                'parsed': parsed,
                'value': value,
                'fixed_date': None
            })
    
    # Find first valid year to establish sequence direction
    first_valid_idx = None
    first_valid_year = None
    for i, rec in enumerate(records):
        if rec['parsed'] and is_valid_year(rec['parsed'][0]):
            first_valid_idx = i
            first_valid_year = rec['parsed'][0]
            break
    
    if first_valid_idx is None:
        # No valid dates found, can't fix
        return 0
    
    # Find sequence pattern by looking at valid consecutive years
    valid_years = []
    valid_indices = []
    for i, rec in enumerate(records):
        if rec['parsed'] and is_valid_year(rec['parsed'][0]):
            valid_years.append(rec['parsed'][0])
            valid_indices.append(i)
    
    # Determine if sequence is ascending or descending
    if len(valid_years) >= 2:
        ascending = valid_years[-1] > valid_years[0]
    else:
        ascending = True  # Default assumption
    
    # Strategy: For each corrupted date, use the CLOSEST valid anchor
    # and work from there (backwards if anchor is after, forwards if before)
    # This ensures 4968 right before 1973 becomes 1972
    
    for i, rec in enumerate(records):
        if rec['parsed'] and is_valid_year(rec['parsed'][0]):
            # Valid date, keep it
            rec['fixed_date'] = rec['original_date']
        else:
            # Invalid date - find nearest valid anchor
            prev_valid_idx = None
            prev_valid_year = None
            next_valid_idx = None
            next_valid_year = None
            
            for j in range(i - 1, -1, -1):
                if records[j]['parsed'] and is_valid_year(records[j]['parsed'][0]):
                    prev_valid_idx = j
                    prev_valid_year = records[j]['parsed'][0]
                    break
            
            for j in range(i + 1, len(records)):
                if records[j]['parsed'] and is_valid_year(records[j]['parsed'][0]):
                    next_valid_idx = j
                    next_valid_year = records[j]['parsed'][0]
                    break
            
            # Choose the CLOSEST anchor and work from there
            inferred_year = None
            
            dist_to_prev = (i - prev_valid_idx) if prev_valid_idx is not None else float('inf')
            dist_to_next = (next_valid_idx - i) if next_valid_idx is not None else float('inf')
            
            if dist_to_next <= dist_to_prev and next_valid_year is not None:
                # Work backwards from next valid year
                steps_to_next = next_valid_idx - i
                if ascending:
                    inferred_year = next_valid_year - steps_to_next
                else:
                    inferred_year = next_valid_year + steps_to_next
            elif prev_valid_year is not None:
                # Work forwards from previous valid year
                steps_from_prev = i - prev_valid_idx
                if ascending:
                    inferred_year = prev_valid_year + steps_from_prev
                else:
                    inferred_year = prev_valid_year - steps_from_prev
            
            if inferred_year and is_valid_year(inferred_year):
                # Use the month/day from original if valid, otherwise use 12-30
                if rec['parsed']:
                    _, month, day = rec['parsed']
                    if not (1 <= month <= 12):
                        month = 12
                    if not (1 <= day <= 31):
                        day = 30
                else:
                    month, day = 12, 30
                
                rec['fixed_date'] = format_date(inferred_year, month, day)
                fixed_count += 1
            else:
                # Can't fix, keep original
                rec['fixed_date'] = rec['original_date']
    
    # Write back
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(header + '\n')
        for rec in records:
            f.write(f"{rec['fixed_date']},{rec['value']}\n")
    
    return fixed_count

def main():
    print("=" * 60)
    print("Fixing Corrupted Dates in CSV Files")
    print("=" * 60)
    
    total_fixed = 0
    files_fixed = 0
    
    for section in sorted(os.listdir(BASE_DIR)):
        section_path = os.path.join(BASE_DIR, section)
        if not os.path.isdir(section_path):
            continue
        
        print(f"\n{section}:")
        section_fixed = 0
        
        for csv_file in sorted(os.listdir(section_path)):
            if not csv_file.endswith('.csv'):
                continue
            
            csv_path = os.path.join(section_path, csv_file)
            fixed = process_csv(csv_path)
            
            if fixed > 0:
                print(f"  ✓ {csv_file}: {fixed} dates fixed")
                total_fixed += fixed
                files_fixed += 1
                section_fixed += fixed
        
        if section_fixed == 0:
            print("  (no fixes needed)")
    
    print("\n" + "=" * 60)
    print(f"COMPLETE! Fixed {total_fixed} dates in {files_fixed} files")
    print("=" * 60)

if __name__ == "__main__":
    main()
