import csv
from ..config import supabase

async def upload_characters_csv_to_supabase(csv_path, table_name="characters_export"):
    if not supabase:
        return
    try:
        rows = []
        with open(csv_path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)
        if not rows:
            print("[SUPABASE] characters.csv has no rows. Skipping.")
            return
        supabase.table(table_name).upsert(rows, on_conflict=["name"]).execute()
        print(f"[SUPABASE] Upserted {len(rows)} rows into '{table_name}'.")
    except Exception as e:
        print("[SUPABASE] Error uploading CSV:", e)