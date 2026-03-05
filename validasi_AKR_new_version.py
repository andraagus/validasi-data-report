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

def validate_values_not_in_set(series, valid_values_set):
    """Mengecek apakah nilai dalam series TIDAK ada di dalam set yang disediakan."""
    return ~series.isin(valid_values_set)

# ==========================================
# STEP 2: PROSES UTAMA
# ==========================================

def run_validation():
    folder_path = os.path.dirname(os.path.abspath(__file__))

    # Cari file AKR untuk divalidasi
    akr_files = [f for f in os.listdir(folder_path) if f.startswith('AKR') and f.endswith(('.xlsx', '.xls'))]
    if not akr_files:
        print("Tidak ada file AKR ditemukan.")
        return
    input_file_akr = os.path.join(folder_path, akr_files[0])
    
    # Cari file referensi KRP
    krp_files = [f for f in os.listdir(folder_path) if f.startswith('KRP') and f.endswith(('.xlsx', '.xls'))]
    if not krp_files:
        print("Tidak ada file KRP yang ditemukan untuk validasi nomorRekening.")
        return
    input_file_krp = os.path.join(folder_path, krp_files[0])

    # Cari file referensi AGN
    agn_files = [f for f in os.listdir(folder_path) if f.startswith('AGN') and f.endswith(('.xlsx', '.xls'))]
    if not agn_files:
        print("Tidak ada file AGN yang ditemukan untuk validasi noAgunan.")
        return
    input_file_agn = os.path.join(folder_path, agn_files[0])

    # Baca semua file yang diperlukan
    print(f"Membaca file validasi: {os.path.basename(input_file_akr)}...")
    df_akr = pd.read_excel(input_file_akr, dtype=str)
    
    print(f"Membaca file referensi KRP: {os.path.basename(input_file_krp)}...")
    df_krp = pd.read_excel(input_file_krp, dtype=str, usecols=['nomorRekening'])
    valid_rekening_set = set(df_krp['nomorRekening'].astype(str).str.strip())
    
    print(f"Membaca file referensi AGN: {os.path.basename(input_file_agn)}...")
    df_agn = pd.read_excel(input_file_agn, dtype=str, usecols=['noAgunan'])
    valid_agunan_set = set(df_agn['noAgunan'].astype(str).str.strip())
    
    for col in df_akr.columns:
        df_akr[col] = df_akr[col].astype(str).str.strip().replace(['nan', 'NaN', 'None'], '')

    error_list = []

    def add_error(condition, message):
        error_series = pd.Series("", index=df_akr.index)
        error_series[condition] = message
        error_list.append(error_series)

    print("Menerapkan aturan validasi...")
    
    # Pre-calculate blank checks
    is_rek_blank = validate_not_blank(df_akr['nomorRekening'])
    is_agunan_blank = validate_not_blank(df_akr['noAgunan'])

    validations = [
        ("idPelapor kosong", validate_not_blank(df_akr['idPelapor'])),
        ("periodeLaporan kosong", validate_not_blank(df_akr['periodeLaporan'])),
        ("periodeData kosong", validate_not_blank(df_akr['periodeData'])),
        ("nomorRekening kosong", is_rek_blank),
        ("nomorRekening tidak ditemukan di file KRP", validate_values_not_in_set(df_akr['nomorRekening'], valid_rekening_set) & ~is_rek_blank),
        ("noAgunan kosong", is_agunan_blank),
        ("noAgunan tidak ditemukan di file AGN", validate_values_not_in_set(df_akr['noAgunan'], valid_agunan_set) & ~is_agunan_blank)
    ]
    
    for message, condition in tqdm(validations, desc="Validasi AKR", colour="green"):
        add_error(condition, message)
        
    # ==========================================
    # STEP 3: MENGGABUNGKAN HASIL
    # ==========================================
    print("Menggabungkan hasil validasi...")
    temp_err_df = pd.concat(error_list, axis=1)
    
    # Inisialisasi tqdm untuk pandas dan gabungkan string per baris
    tqdm.pandas(desc="Menggabungkan pesan error")
    df_akr['hasil_validasi'] = temp_err_df.progress_apply(lambda x: "; ".join(filter(None, x)), axis=1)
    df_akr['hasil_validasi'] = df_akr['hasil_validasi'].replace("", "VALID")

    # Simpan
    output_file_akr = os.path.join(folder_path, f'hasil_validasi_{os.path.basename(input_file_akr)}')
    df_akr.to_excel(output_file_akr, index=False)
    print(f"Selesai! File disimpan di: {output_file_akr}")

if __name__ == "__main__":
    run_validation()