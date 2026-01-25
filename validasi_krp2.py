import pandas as pd
import re
import os
from datetime import datetime
from tqdm import tqdm

# ============================
# Cari File yang Mengandung 'KRP'
# ============================

folder_path = os.path.dirname(os.path.abspath(__file__))
krp_files = [f for f in os.listdir(folder_path) if 'KRP' in f and f.endswith(('.xlsx', '.xls'))]

if not krp_files:
    raise FileNotFoundError("Tidak ada file yang mengandung 'KRP' ditemukan di folder.")

input_file = os.path.join(folder_path, krp_files[0])
print(f"Membaca file: {input_file}")

# PENTING: baca SEMUA kolom sebagai string supaya data tidak berubah
df = pd.read_excel(input_file, dtype=str)

# ============================
# Preprocessing untuk Percepatan
# ============================

# 1️⃣ Strip semua string di awal
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

# 2️⃣ Tandai duplikat nomorRekening sekali saja
df['is_duplicated_nomorRekening'] = df['nomorRekening'].duplicated()

# 3️⃣ Pre-parse semua kolom tanggal ke datetime sekali saja
tanggal_cols = [
    'tanggalAkadAwal', 'tanggalAkadAkhir',
    'tanggalAwal', 'tanggalMulai', 'tanggalJatuhTempo'
]
for col in tanggal_cols:
    df[f'{col}_parsed'] = pd.to_datetime(df[col], format='%Y-%m-%d', errors='coerce')

# 4️⃣ Compile regex di awal
ALPHANUMERIC_RE = re.compile(r'^[a-zA-Z0-9]+$')
INVALID_CHAR_RE = re.compile(r'[a-zA-Z\s!@#$%^&*()_+?]')
NUMERIC_ONLY_RE = re.compile(r'^\d+$')

# ============================
# Fungsi Validasi Per Baris
# ============================

def validate_row(row):
    errors = []

    def is_blank(value):
        return pd.isna(value) or str(value).strip() == ""

    def is_alphanumeric(value):
        return bool(ALPHANUMERIC_RE.fullmatch(str(value)))

    def contains_invalid_char(value):
        return bool(INVALID_CHAR_RE.search(str(value)))

    def is_not_numeric_only(value):
        return not NUMERIC_ONLY_RE.fullmatch(str(value))

    # ====================
    # Validasi per kolom
    # ====================

    if is_blank(row['idPelapor']):
        errors.append("idPelapor kosong")

    if is_blank(row['periodeLaporan']):
        errors.append("periodeLaporan kosong")
    elif str(row['periodeLaporan']) != "M":
        errors.append("periodeLaporan harus 'M'")

    if is_blank(row['periodeData']):
        errors.append("periodeData kosong")

    if is_blank(row['nomorRekening']):
        errors.append("nomorRekening kosong")
    elif not is_alphanumeric(row['nomorRekening']):
        errors.append("nomorRekening harus alfanumeric")

    if row['is_duplicated_nomorRekening']:
        errors.append("nomorRekening duplikat")

    if is_blank(row['idDebitur']):
        errors.append("idDebitur kosong")

    if is_blank(row['jenisKreditPembiayaan']):
        errors.append("jenisKreditPembiayaan kosong")

    if is_blank(row['nomorAkadAwal']):
        errors.append("nomorAkadAwal kosong")

    tgl_awal = row['tanggalAkadAwal_parsed']
    tgl_akhir = row['tanggalAkadAkhir_parsed']

    if pd.isnull(tgl_awal):
        errors.append("tanggalAkadAwal kosong atau format salah")
    elif contains_invalid_char(str(row['tanggalAkadAwal'])) or is_not_numeric_only(str(row['tanggalAkadAwal']).replace("-", "")):
        errors.append("tanggalAkadAwal mengandung karakter tidak valid")

    if is_blank(row['nomorAkadAkhir']):
        errors.append("nomorAkadAkhir kosong")

    if pd.isnull(tgl_akhir):
        errors.append("tanggalAkadAkhir kosong atau format salah")
    elif not pd.isnull(tgl_awal) and tgl_akhir < tgl_awal:
        errors.append("tanggalAkadAkhir lebih muda dari tanggalAkadAwal")

    # Validasi tanggal lainnya
    for field in ['tanggalAwal', 'tanggalMulai', 'tanggalJatuhTempo']:
        tgl = row[f'{field}_parsed']
        if pd.isnull(tgl):
            errors.append(f"{field} kosong atau format salah")

    # Kolom wajib lain
    wajib_kolom = [
        'kategoriUsahaDebitur', 'kategoriPortofolio', 'sifatKreditPembiayaan',
        'jenisPenggunaan', 'orientasiPenggunaan', 'jenisValuta',
        'klasifikasiAsetKeuangan', 'kreditProgramPemerintah', 'sektorEkonomi',
        'lokasiPenggunaan', 'jenisSukuBungaImbalan', 'sukuBungaPersentaseImbalanBulanLaporan',
        'kualitas', 'plafonAwal'
    ]

    for kolom in wajib_kolom:
        if is_blank(row.get(kolom, "")):
            errors.append(f"{kolom} kosong")

    if not is_blank(row['jenisValuta']) and row['jenisValuta'] != 'IDR':
        errors.append("jenisValuta harus 'IDR'")

    return "VALID" if not errors else "; ".join(errors)

# ============================
# Proses dan Simpan Hasil
# ============================

tqdm.pandas()
df['hasil_validasi'] = df.progress_apply(validate_row, axis=1)

output_file = os.path.join(folder_path, f'hasil_validasi_{os.path.basename(input_file)}')
df.to_excel(output_file, index=False)

print(f"Validasi selesai. File disimpan sebagai '{output_file}'.")
