#!/usr/bin/env python3
"""
Script to rewrite storedproc.test file according to specified rules:
1. Remove trailing delimiter from delimiter statements
2. Convert 'let $message' to comments
3. Remove '--source *.inc' statements
4. Execute SQL statements in MySQL and add --error directive if needed
"""

import re
import sys
import pymysql
from pymysql import MySQLError

def rewrite_storedproc_test(input_file, output_file):
    """
    Rewrite the storedproc.test file according to the rules.
    
    Args:
        input_file: Path to input file
        output_file: Path to output file
    """
    # Connect to MySQL
    try:
        conn = pymysql.connect(
            host='127.0.0.1',
            port=3306,
            user='root',
            password='',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True
        )
        # Create a cursor that will be reused throughout the script
        cursor = conn.cursor()
        print("Connected to MySQL at 127.0.0.1:3306")
    except Exception as e:
        print(f"Warning: Could not connect to MySQL: {e}")
        print("Continuing without MySQL validation...")
        conn = None
        cursor = None
        conn = None
        cursor = None
    
    with open(input_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    modified_lines = []
    current_delimiter = ';'
    sql_buffer = []
    i = 0
    
    def is_sql_statement(line):
        """Check if line looks like a SQL statement"""
        stripped = line.strip()
        if not stripped or stripped.startswith('--') or stripped.startswith('#'):
            return False
        # Common SQL keywords
        sql_keywords = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 
                       'ALTER', 'TRUNCATE', 'GRANT', 'REVOKE', 'SET', 'USE',
                       'CALL', 'SHOW', 'DESCRIBE', 'DESC', 'EXPLAIN']
        upper_stripped = stripped.upper()
        return any(upper_stripped.startswith(kw) for kw in sql_keywords)
    
    def execute_sql(sql, delimiter):
        """Execute SQL and return error code if any"""
        if not cursor:
            return None
        
        # Remove the delimiter at the end if present
        sql = sql.strip()
        if sql.endswith(delimiter):
            sql = sql[:-len(delimiter)].strip()
        
        try:
            cursor.execute(sql)
            return None
        except MySQLError as e:
            # Extract error code
            error_code = e.args[0]
            error_name = None
            
            # Common MySQL error codes to names
            error_map = {
                1046: 'ER_NO_DB_ERROR',
                1054: 'ER_BAD_FIELD_ERROR',
                1146: 'ER_NO_SUCH_TABLE',
                1064: 'ER_PARSE_ERROR',
                1062: 'ER_DUP_ENTRY',
                1364: 'ER_NO_DEFAULT_FOR_FIELD',
                1048: 'ER_BAD_NULL_ERROR',
                1452: 'ER_NO_REFERENCED_ROW_2',
                1451: 'ER_ROW_IS_REFERENCED_2',
                1406: 'ER_DATA_TOO_LONG',
                1264: 'ER_WARN_DATA_OUT_OF_RANGE',
                1690: 'ER_DATA_OUT_OF_RANGE',
            }
            
            error_name = error_map.get(error_code, str(error_code))
            return error_name
        except Exception as e:
            print(f"Unexpected error executing SQL: {e}")
            return None
    
    while i < len(lines):
        line = lines[i]
        
        # Rule 3: Remove '--source *.inc' statements
        if re.match(r'^\s*--source\s+\S+\.inc\s*$', line):
            i += 1
            continue
        
        # Rule 1: Fix delimiter statements
        if re.match(r'^\s*delimiter\s+', line, re.IGNORECASE):
            # First, fix the line format
            # Remove trailing ;// -> ; or //; -> //
            original_line = line
            line = re.sub(r'delimiter\s+(;)//\s*$', r'delimiter \1\n', line, flags=re.IGNORECASE)
            line = re.sub(r'delimiter\s+(//);\s*$', r'delimiter \1\n', line, flags=re.IGNORECASE)
            
            # Update current delimiter based on the fixed line
            delimiter_match = re.search(r'delimiter\s+(\S+)', line, re.IGNORECASE)
            if delimiter_match:
                current_delimiter = delimiter_match.group(1)
            
            modified_lines.append(line)
            i += 1
            continue
        
        # Rule 2: Convert 'let $message' to comments
        message_match = re.match(r'^\s*let\s+\$message\s*=\s*["\'](.+?)["\']\s*;?\s*$', line, re.IGNORECASE)
        if message_match:
            modified_lines.append(f'# {message_match.group(1)}\n')
            i += 1
            continue
        else:
            message_match = re.match(r'^\s*let\s+\$message\s*=\s*(.+?)\s*;?\s*$', line, re.IGNORECASE)
            if message_match:
                modified_lines.append(f'# {message_match.group(1)}\n')
                i += 1
                continue
        
        # Rule 4: Execute SQL and add --error if needed
        if is_sql_statement(line):
            # Collect the complete SQL statement (may span multiple lines)
            sql_buffer = [line]
            j = i + 1
            
            # Check if current line already contains the delimiter
            combined = ''.join(sql_buffer)
            if current_delimiter not in combined:
                # Keep reading until we find the delimiter
                while j < len(lines):
                    next_line = lines[j]
                    # Don't include delimiter statements in SQL buffer
                    if re.match(r'^\s*delimiter\s+', next_line, re.IGNORECASE):
                        break
                    sql_buffer.append(next_line)
                    combined = ''.join(sql_buffer)
                    if current_delimiter in combined:
                        j += 1  # Move past the line containing the delimiter
                        break
                    j += 1
            
            # Check if previous line has --error directive
            has_error_directive = False
            if modified_lines:
                prev_line = modified_lines[-1].strip()
                if prev_line.startswith('--error'):
                    has_error_directive = True
            
            # Execute SQL if no error directive exists
            if not has_error_directive and conn:
                sql = ''.join(sql_buffer)
                error = execute_sql(sql, current_delimiter)
                
                if error:
                    # Add --error directive before the SQL
                    modified_lines.append(f'--error {error}\n')
            
            # Add all the SQL lines
            for k in range(len(sql_buffer)):
                modified_lines.append(sql_buffer[k])
            
            i = j
            continue
        
        # Default: just append the line
        modified_lines.append(line)
        i += 1
    
    if cursor:
        cursor.close()
    if conn:
        conn.close()
        print("MySQL connection closed")
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.writelines(modified_lines)
    
    print(f"Successfully rewrote {input_file} to {output_file}")
    print(f"Original lines: {len(lines)}")
    print(f"Modified lines: {len(modified_lines)}")
    print(f"Lines removed: {len(lines) - len(modified_lines)}")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        input_file = 'storedproc.test'
        output_file = 'storedproc.test.rewritten'
    elif len(sys.argv) == 2:
        input_file = sys.argv[1]
        output_file = input_file + '.rewritten'
    else:
        input_file = sys.argv[1]
        output_file = sys.argv[2]
    
    try:
        rewrite_storedproc_test(input_file, output_file)
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)