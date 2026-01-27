import pandas as pd
import re
import os
from tqdm import tqdm

# ==========================================
# STEP 1: FUNGSI VALIDASI MODULAR (UNIT)
# ==========================================
# Fungsi-fungsi ini sekarang bekerja pada satu kolom sekaligus (Vectorized)

def check_blank(series):
    return series.isna() | (series.astype(str).str.strip() == "")

def validate_alphanumeric(series):
    # Menggunakan regex vektorisasi pandas
    return series.astype(str).str.match(r'^[a-zA-Z0-9]+$')

def validate_numeric_only(series):
    return series.astype(str).str.match(r'^\d+$')

def validate_not_equal(series, value):
    return series.astype(str) != value
def validate_not_blank(series):
    return series.isna() | (series.astype(str).str.strip() == "")

# ==========================================
# STEP 2: PROSES UTAMA
# ==========================================

def run_validation():
    folder_path = os.path.dirname(os.path.abspath(__file__))
    krp_files = [f for f in os.listdir(folder_path) if 'KRP' in f and f.endswith(('.xlsx', '.xls'))]

    if not krp_files:
        print("Tidak ada file KRP ditemukan.")
        return

    input_file = os.path.join(folder_path, krp_files[0])
    print(f"Membaca file: {input_file}...")
    
    # Tetap baca sebagai string
    df = pd.read_excel(input_file, dtype=str)
    
    # Bersihkan spasi
    for col in df.columns:
        df[col] = df[col].astype(str).str.strip().replace(['nan', 'NaN', 'None'], '')

    # Dictionary untuk menampung pesan error sementara
    # Kita gunakan list of series agar bisa digabungkan di akhir
    error_log = pd.DataFrame(index=df.index)
    error_list = []

    print("Melakukan validasi vectorized (Sangat Cepat)...")

    # Helper untuk menambah error ke list
    def add_error(condition, message):
        # condition: True jika ERROR
        error_series = pd.Series("", index=df.index)
        error_series[condition] = message
        error_list.append(error_series)

    # --- Validasi Kolom ---
    add_error(validate_not_blank(df['idPelapor']), "idPelapor kosong")
    
    add_error(df['periodeLaporan'] != 'M', "periodeLaporan harus 'M'")
    
    # --- Validasi nomorRekening ---
    add_error(validate_not_blank(df['nomorRekening']), "nomorRekening kosong")
    add_error(~validate_alphanumeric(df['nomorRekening']) & (df['nomorRekening'] != ""), "nomorRekening harus alphanumeric")
    add_error(df['nomorRekening'].duplicated(keep=False) & (df['nomorRekening'] != ""), "nomorRekening duplikat")

    # --- Validasi Tanggal (Gunakan variabel lokal untuk perbandingan) ---
    tgl_awal = pd.to_datetime(df['tanggalAkadAwal'], format='%Y-%m-%d', errors='coerce')
    tgl_akhir = pd.to_datetime(df['tanggalAkadAkhir'], format='%Y-%m-%d', errors='coerce')
    
    add_error(tgl_awal.isna() & (df['tanggalAkadAwal'] != ""), "tanggalAkadAwal format salah")
    add_error((tgl_akhir < tgl_awal) & tgl_awal.notna() & tgl_akhir.notna(), "tanggalAkadAkhir < tanggalAkadAwal")

    # --- Validasi Kolom Wajib (Batch) ---
    wajib_kolom = ['idDebitur', 'jenisKreditPembiayaan', 'nomorAkadAwal', 'plafonAwal']
    for col in wajib_kolom:
        add_error(validate_not_blank(df[col]), f"{col} kosong")

    # --- Validasi jenisValuta ---
    add_error((df['jenisValuta'] != 'IDR') & (df['jenisValuta'] != ""), "jenisValuta harus IDR")
    # ==========================================
    # STEP 3: MENGGABUNGKAN HASIL
    # ==========================================
    print("Menggabungkan hasil validasi...")
    
    # Menggabungkan semua series error dengan separator "; "
    # Kita filter string kosong agar tidak ada double semicolon
    def join_errors(row):
        return "; ".join([e for e in row if e != ""])

    # Konversi list series menjadi dataframe untuk diproses baris demi baris hanya untuk join string
    temp_err_df = pd.concat(error_list, axis=1)
    
    # Hasil akhir
    df['hasil_validasi'] = temp_err_df.apply(lambda x: "; ".join(filter(None, x)), axis=1)
    df['hasil_validasi'] = df['hasil_validasi'].replace("", "VALID")

    # Simpan
    output_file = os.path.join(folder_path, f'hasil_validasi_{os.path.basename(input_file)}')
    df.to_excel(output_file, index=False)
    print(f"Selesai! File disimpan di: {output_file}")

if __name__ == "__main__":
    run_validation()