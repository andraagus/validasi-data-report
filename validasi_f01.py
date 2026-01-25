import pandas as pd
import re
import os
import time
from datetime import datetime

# ============================
# Cari File Input
# ============================
folder_path = os.path.dirname(os.path.abspath(__file__))
input_files = [f for f in os.listdir(folder_path) if 'F01' in f and f.endswith(('.xlsx', '.xls'))]

if not input_files:
    raise FileNotFoundError("Tidak ada file yang mengandung 'F01' ditemukan di folder ini.")

input_file = os.path.join(folder_path, input_files[0])
print(f"Membaca file: {input_file}")

# ============================
# Membaca File
# ============================
dtype_setting = {
    'Kode Sektor Ekonomi': str,
    'Kode Kab. / Kota Lokasi Proyek': str,
    'Plafon Awal': str,
    'Plafon': str,
    'Realisasi': str,
    'Denda': str,
    'Baki Debet': str,
    'Kode Kualitas Kredit': str,
    'Kode Sebab Macet': str,
    'Kode Kantor Cabang': str,
    'Kode Kondisi': str
}

df = pd.read_excel(input_file, dtype=dtype_setting)

# ============================
# Precompute duplicated No Rekening
# ============================
norek_counts = df['No Rekening Fasilitas'].value_counts()
duplicate_norek = set(norek_counts[norek_counts > 1].index)

# ============================
# Fungsi Validasi
# ============================
def is_blank(val):
    return pd.isna(val) or str(val).strip() == ""

def is_valid_date(val):
    try:
        datetime.strptime(str(val), "%Y-%m-%d")
        return True
    except:
        return False

def contains_alpha(val):
    return bool(re.search(r'[a-zA-Z]', str(val)))

def contains_special_char(val):
    return bool(re.search(r'[!@#$%^&*()_+?]', str(val)))

def only_numbers(val):
    return bool(re.fullmatch(r'\d+', str(val).strip()))

def no_space(val):
    return ' ' not in str(val)

def must_be_idr(val):
    return str(val).strip().upper() == 'IDR'

def must_be_472(val):
    return str(val).strip() == '472'

def must_be_001(val):
    return str(val).strip() == '001'

def length_equals(val, n):
    return len(str(val).strip()) == n

# ============================
# Fungsi Validasi Per Baris
# ============================
def validate_row(row):
    errors = []

    # Flag Detail
    if is_blank(row['Flag Detail']) or str(row['Flag Detail']).strip() != 'D':
        errors.append('Flag Detail harus D dan tidak kosong')

    # No Rekening Fasilitas
    norek = str(row['No Rekening Fasilitas']).strip()
    if is_blank(norek):
        errors.append('No Rekening Fasilitas kosong')
    elif contains_special_char(norek) or not no_space(norek):
        errors.append('No Rekening Fasilitas mengandung karakter tidak valid atau spasi')
    elif norek in duplicate_norek:
        errors.append('No Rekening Fasilitas duplikat')

    # No CIF Debitur
    if is_blank(row['No CIF Debitur']) or not no_space(row['No CIF Debitur']):
        errors.append('No CIF Debitur kosong atau mengandung spasi')

    # Kolom wajib
    wajib_blank = [
        'Kode Sifat Kredit', 'Kode Jenis Kredit', 'Kode Akad Kredit', 'No Akad Awal',
        'No Akad Akhir', 'Freq Perpanjangan Fasilitas Kredit',
        'Kode Kategori Debitur', 'Kode Jenis Penggunaan', 'Kode Orientasi Penggunaan',
        'Kode Sektor Ekonomi', 'Kode Kab. / Kota Lokasi Proyek', 'Kode Valuta',
        'Suku Bunga atau Imbalan', 'Jenis Suku Bunga', 'Kredit  Program Pemerintah',
        'Sumber Dana', 'Plafon Awal', 'Plafon', 'Realisasi', 'Denda', 'Baki Debet',
        'Kode Kualitas Kredit', 'Tunggakan Pokok', 'Tunggakan Bunga', 'Jmlh Hari Tunggakan',
        'Frekuensi Tunggakan', 'Frekuensi Restrukturisasi', 'Kode Kondisi',
        'Kode Kantor Cabang', 'Operasi Data'
    ]
    for kolom in wajib_blank:
        if is_blank(row.get(kolom, '')):
            errors.append(f'{kolom} kosong')

    # Validasi tanggal
    def validasi_tanggal(kolom, min_tgl=None):
        val = row.get(kolom, '')
        if is_blank(val):
            errors.append(f'{kolom} kosong')
            return
        if not is_valid_date(val):
            errors.append(f'{kolom} format tanggal salah')
            return
        if contains_alpha(val) or contains_special_char(val) or ' ' in str(val):
            errors.append(f'{kolom} mengandung karakter tidak valid')
            return
        if only_numbers(val):
            errors.append(f'{kolom} hanya angka saja')
            return
        tgl = datetime.strptime(str(val), "%Y-%m-%d")
        if min_tgl and tgl < min_tgl:
            errors.append(f'{kolom} lebih muda dari batas')

    validasi_tanggal('Tanggal Akad Awal')
    tgl_awal = datetime.strptime(str(row['Tanggal Akad Awal']), "%Y-%m-%d") if is_valid_date(row['Tanggal Akad Awal']) else None
    validasi_tanggal('Tanggal Akad Akhir', min_tgl=tgl_awal)
    validasi_tanggal('Tanggal Awal Kredit')
    validasi_tanggal('Tanggal Mulai')
    tgl_mulai = datetime.strptime(str(row['Tanggal Mulai']), "%Y-%m-%d") if is_valid_date(row['Tanggal Mulai']) else None
    validasi_tanggal('Tanggal Jatuh Tempo', min_tgl=tgl_mulai)

    if not is_blank(row['Kode Sektor Ekonomi']) and not length_equals(row['Kode Sektor Ekonomi'], 6):
        errors.append('Kode Sektor Ekonomi harus 6 digit')
    if not is_blank(row['Kode Kab. / Kota Lokasi Proyek']) and not length_equals(row['Kode Kab. / Kota Lokasi Proyek'], 4):
        errors.append('Kode Kab. / Kota Lokasi Proyek harus 4 digit')

    # Value tertentu
    if not is_blank(row['Kode Valuta']) and not must_be_idr(row['Kode Valuta']):
        errors.append('Kode Valuta harus IDR')
    if not is_blank(row['Sumber Dana']) and not must_be_472(row['Sumber Dana']):
        errors.append('Sumber Dana harus 472')
    if not is_blank(row['Kode Kantor Cabang']) and not must_be_001(row['Kode Kantor Cabang']):
        errors.append('Kode Kantor Cabang harus 001')

    # Angka tanpa spasi
    angka_kolom = ['Plafon Awal', 'Plafon', 'Realisasi', 'Denda', 'Baki Debet', 'Kode Kualitas Kredit']
    for kolom in angka_kolom:
        val = str(row.get(kolom, '')).strip()
        if not only_numbers(val) or ' ' in val:
            errors.append(f'{kolom} harus angka tanpa spasi')

    # Tanggal Macet & Kode Sebab Macet
    if str(row.get('Kode Kualitas Kredit', '')).strip() == '5' and str(row.get('Kode Kondisi', '')).strip() == '00':
        validasi_tanggal('Tanggal Macet')
        if is_blank(row.get('Kode Sebab Macet', '')) or str(row.get('Kode Sebab Macet', '')).strip() != '99':
            errors.append('Kode Sebab Macet harus 99 saat Kode Kualitas Kredit = 5 dan Kode Kondisi = 00')

    # Tanggal Kondisi
    if not is_blank(row.get('Kode Kondisi', '')) and str(row.get('Kode Kondisi')).strip() != '00':
        validasi_tanggal('Tanggal Kondisi')

    # ----------------------------
    # VALIDASI BARU
    # ----------------------------
    try:
        plafon_awal = int(float(str(row.get('Plafon Awal', '0')).strip()))
        if plafon_awal <= 0:
            errors.append('Plafon Awal harus lebih besar dari 0')
    except:
        errors.append('Plafon Awal harus berupa angka')

    try:
        pokok = int(float(str(row.get('Tunggakan Pokok', '0')).strip()))
        bunga = int(float(str(row.get('Tunggakan Bunga', '0')).strip()))
        hari  = int(float(str(row.get('Jmlh Hari Tunggakan', '0')).strip()))
        if pokok == 0 and bunga == 0 and hari != 0:
            errors.append('Jika Tunggakan Pokok dan Tunggakan Bunga = 0 maka Jmlh Hari Tunggakan harus 0')
    except:
        errors.append('Tunggakan Pokok, Tunggakan Bunga, dan Jmlh Hari Tunggakan harus berupa angka')

    # Jenis Penggunaan vs Kategori Debitur
    kode_jenis = str(row.get('Kode Jenis Penggunaan', '')).strip()
    kode_kategori = str(row.get('Kode Kategori Debitur', '')).strip().upper()
    if kode_jenis == '3' and kode_kategori in ['UM', 'UK', 'UT']:
        errors.append('Jika Kode Jenis Penggunaan = 3 maka Kode Kategori Debitur tidak boleh UM, UK, atau UT')

    # Kode Kondisi = 02 → Kode Kualitas Kredit = 1
    kode_kondisi = str(row.get('Kode Kondisi', '')).strip()
    kualitas_kredit = str(row.get('Kode Kualitas Kredit', '')).strip()
    if kode_kondisi == '02' and kualitas_kredit != '1':
        errors.append('Jika Kode Kondisi = 02 maka Kode Kualitas Kredit harus 1')

    return "VALID" if not errors else "; ".join(errors)


# ============================
# Eksekusi Validasi
# ============================
start_time = time.time()
df['hasil_validasi'] = df.apply(validate_row, axis=1)
end_time = time.time()
elapsed_time = end_time - start_time

# ============================
# Simpan Hasil
# ============================
output_file = os.path.join(folder_path, f"hasil_validasi_{os.path.basename(input_file)}")
df.to_excel(output_file, index=False)

print(f"Validasi selesai. File hasil: '{output_file}'")
print(f"Waktu proses: {elapsed_time:.2f} detik")
