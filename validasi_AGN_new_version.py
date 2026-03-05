import pandas as pd
import re
import os
from tqdm import tqdm

# ==========================================
# STEP 1: FUNGSI VALIDASI MODULAR (UNIT)
# ==========================================

def validate_not_blank(series):
    return series.isna() | (series.astype(str).str.strip() == "")

def validate_alphanumeric(series):
    return series.astype(str).str.match(r'^[a-zA-Z0-9]+$')

# ==========================================
# STEP 2: PROSES UTAMA
# ==========================================

def run_validation():
    folder_path = os.path.dirname(os.path.abspath(__file__))
    agn_files = [f for f in os.listdir(folder_path) if f.startswith('AGN') and f.endswith(('.xlsx', '.xls'))]

    if not agn_files:
        print("Tidak ada file AGN ditemukan.")
        return

    input_file = os.path.join(folder_path, agn_files[0])
    print(f"Membaca file: {os.path.basename(input_file)}...")
    
    df = pd.read_excel(input_file, dtype=str)
    
    for col in df.columns:
        df[col] = df[col].astype(str).str.strip().replace(['nan', 'NaN', 'None'], '')

    error_list = []

    def add_error(condition, message):
        error_series = pd.Series("", index=df.index)
        error_series[condition] = message
        error_list.append(error_series)

    print("Menerapkan aturan validasi...")
    
    validations = [
        ("idPelapor kosong", validate_not_blank(df['idPelapor'])),
        ("periodeLaporan kosong", validate_not_blank(df['periodeLaporan'])),
        ("periodeData kosong", validate_not_blank(df['periodeData'])),
        ("noAgunan kosong", validate_not_blank(df['noAgunan'])),
        ("jenisAgunan kosong", validate_not_blank(df['jenisAgunan'])),
        ("nilaiAgunan kosong", validate_not_blank(df['nilaiAgunan'])),
    ]
    
    for message, condition in tqdm(validations, desc="Validasi AGN", colour="green"):
        add_error(condition, message)
        
    # ==========================================
    # STEP 3: MENGGABUNGKAN HASIL
    # ==========================================
    print("Menggabungkan hasil validasi...")
    temp_err_df = pd.concat(error_list, axis=1)
    
    # Inisialisasi tqdm untuk pandas dan gabungkan string per baris
    tqdm.pandas(desc="Menggabungkan pesan error")
    df['hasil_validasi'] = temp_err_df.progress_apply(lambda x: "; ".join(filter(None, x)), axis=1)
    df['hasil_validasi'] = df['hasil_validasi'].replace("", "VALID")

    # Simpan
    output_file = os.path.join(folder_path, f'hasil_validasi_{os.path.basename(input_file)}')
    df.to_excel(output_file, index=False)
    print(f"Selesai! File disimpan di: {output_file}")

if __name__ == "__main__":
    run_validation()