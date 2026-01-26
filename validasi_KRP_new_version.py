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

def check_alphanumeric(series):
    # Menggunakan regex vektorisasi pandas
    return series.astype(str).str.match(r'^[a-zA-Z0-9]+$')

def check_numeric_only(series):
    return series.astype(str).str.match(r'^\d+$')

def check_not_equal(series, value):
    return series.astype(str) != value

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
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

    # Dictionary untuk menampung pesan error sementara
    # Kita gunakan list of series agar bisa digabungkan di akhir
    error_log = pd.DataFrame(index=df.index)
    error_list = []

    print("Melakukan validasi vectorized (Sangat Cepat)...")

    # --- Validasi idPelapor ---
    mask = check_blank(df['idPelapor'])
    error_list.append(pd.Series(index=df.index, data="idPelapor kosong").where(mask, ""))

    # --- Validasi nomorRekening ---
    mask_blank = check_blank(df['nomorRekening'])
    mask_alpha = ~check_alphanumeric(df['nomorRekening']) & ~mask_blank
    mask_dup = df['nomorRekening'].duplicated(keep=False)
    
    error_list.append(pd.Series("nomorRekening kosong").where(mask_blank, ""))
    error_list.append(pd.Series("nomorRekening harus alphanumeric").where(mask_alpha, ""))
    error_list.append(pd.Series("nomorRekening duplikat").where(mask_dup, ""))

    # --- Validasi Periode Laporan ---
    mask_period = check_not_equal(df['periodeLaporan'], "M")
    error_list.append(pd.Series("periodeLaporan harus 'M'").where(mask_period, ""))

    # --- Validasi Tanggal (Vectorized Conversion) ---
    tgl_awal = pd.to_datetime(df['tanggalAkadAwal'], format='%Y-%m-%d', errors='coerce')
    tgl_akhir = pd.to_datetime(df['tanggalAkadAkhir'], format='%Y-%m-%d', errors='coerce')
    
    mask_tgl_awal = tgl_awal.isna()
    mask_tgl_urut = (tgl_akhir < tgl_awal) & tgl_awal.notna() & tgl_akhir.notna()
    
    error_list.append(pd.Series("tanggalAkadAwal format salah").where(mask_tgl_awal, ""))
    error_list.append(pd.Series("tanggalAkadAkhir < tanggalAkadAwal").where(mask_tgl_urut, ""))

    # --- Validasi Kolom Wajib Lainnya (Massal) ---
    wajib_kolom = [
        'idDebitur', 'jenisKreditPembiayaan', 'nomorAkadAwal', 'plafonAwal', 'kualitas'
    ]
    for col in wajib_kolom:
        if col in df.columns:
            mask = check_blank(df[col])
            error_list.append(pd.Series(f"{col} kosong").where(mask, ""))

    # --- Validasi jenisValuta ---
    mask_valuta = (df['jenisValuta'] != 'IDR') & ~check_blank(df['jenisValuta'])
    error_list.append(pd.Series("jenisValuta harus IDR").where(mask_valuta, ""))

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