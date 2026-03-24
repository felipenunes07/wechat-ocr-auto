import sys
import os

sys.path.insert(0, os.path.abspath('.'))

try:
    import gspread
    gc = gspread.service_account(filename='google_service_account.json')
    sh = gc.open_by_key('14_O2qiXu-TYAhz2LeG0wEmjxxaNDpyU16CQiuo0Wf8c')
    ws = sh.worksheet('Página1')
    rows = ws.get_all_values()
    
    with open('.runtime/test_sheet.txt', 'w', encoding='utf-8') as f:
        f.write(f"Total Rows: {len(rows)}\n")
        f.write(f"Last 5 rows:\n")
        for idx, row in enumerate(rows[-5:], start=max(1, len(rows)-4)):
            f.write(f"Row {idx}: {row}\n")
            
        empty_count = sum(1 for r in rows[1:] if not any(x.strip() for x in r))
        f.write(f"Completely empty rows above bottom: {empty_count}\n")
except Exception as e:
    with open('.runtime/test_sheet.txt', 'w', encoding='utf-8') as f:
        f.write(f"ERROR: {str(e)}\n")
