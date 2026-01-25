import pandas as pd
import re
from datetime import datetime

# Load data
df = pd.read_excel('Test Validasi Data.xlsx')

# ============================
# Fungsi Validasi Per Kolom
# ============================

def is_valid_nama(nama):
    if pd.isna(nama) or str(nama).strip() == "":
        return False, "Nama kosong atau hanya spasi"
    if re.search(r'\d', str(nama)):
        return False, "Nama mengandung angka"
    if re.search(r'[!@#$%^&*()_+?]', str(nama)):
        return False, "Nama mengandung karakter spesial"
    return True, ""

def is_valid_tanggal(tanggal):
    if pd.isna(tanggal) or str(tanggal).strip() == "":
        return False, "Tanggal lahir kosong"
    try:
        datetime.strptime(str(tanggal), "%Y-%m-%d")
    except ValueError:
        return False, "Format tanggal tidak valid (seharusnya yyyy-mm-dd)"
    return True, ""

def is_valid_no_telp(telp):
    if pd.isna(telp) or str(telp).strip() == "":
        return False, "No telp kosong"
    if re.search(r'[a-zA-Z\s!@#$%^&*()_+?]', str(telp)):
        return False, "No telp mengandung huruf, spasi, atau karakter spesial"
    if not re.fullmatch(r'\d+', str(telp)):
        return False, "No telp tidak berupa angka murni"
    return True, ""

# ============================
# Validasi & Gabungkan Hasil
# ============================

def hasil_validasi_baris(row):
    errors = []

    valid_nama, err_nama = is_valid_nama(row['nama'])
    if not valid_nama:
        errors.append(f"nama: {err_nama}")

    valid_tanggal, err_tanggal = is_valid_tanggal(row['tanggal lahir'])
    if not valid_tanggal:
        errors.append(f"tanggal lahir: {err_tanggal}")

    valid_telp, err_telp = is_valid_no_telp(row['no telp'])
    if not valid_telp:
        errors.append(f"no telp: {err_telp}")

    return "VALID" if not errors else "; ".join(errors)

# Tambahkan kolom hasil_validasi
df['hasil_validasi'] = df.apply(hasil_validasi_baris, axis=1)

# Simpan hasilnya
df.to_excel('hasil_validasi.xlsx', index=False)

print("Validasi selesai. File hasil disimpan sebagai 'hasil_validasi.xlsx'.")
