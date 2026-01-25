import pandas as pd
import re
import os
from datetime import datetime
from tqdm import tqdm

# ============================
# Cari File yang Mengandung 'KRP'
# ============================

# Dapatkan path folder script ini
folder_path = os.path.dirname(os.path.abspath(__file__))

# Cari file yang mengandung 'KRP' di nama filenya
krp_files = [f for f in os.listdir(folder_path) if 'KRP' in f and f.endswith(('.xlsx', '.xls'))]

if not krp_files:
    raise FileNotFoundError("Tidak ada file yang mengandung 'KRP' ditemukan di folder.")

# Gunakan file pertama yang ditemukan
input_file = os.path.join(folder_path, krp_files[0])
print(f"Membaca file: {input_file}")

# Load data
df = pd.read_excel(input_file)

# ============================
# Fungsi Validasi Per Kolom
# ============================

def is_blank(value):
    return pd.isna(value) or str(value).strip() == ""

def is_alphanumeric(value):
    return bool(re.fullmatch(r'[a-zA-Z0-9]+', str(value)))

def is_valid_date_format(value):
    try:
        datetime.strptime(str(value), "%Y-%m-%d")
        return True
    except:
        return False

def contains_invalid_char(value):
    return bool(re.search(r'[a-zA-Z\s!@#$%^&*()_+?]', str(value)))

def is_not_numeric_only(value):
    return not re.fullmatch(r'\d+', str(value))

# ============================
# Validasi Per Baris
# ============================

def validate_row(row):
    errors = []

    if is_blank(row['idPelapor']):
        errors.append("idPelapor kosong")

    if is_blank(row['periodeLaporan']):
        errors.append("periodeLaporan kosong")
    elif str(row['periodeLaporan']).strip() != "M":
        errors.append("periodeLaporan harus 'M'")
    if is_blank(row['periodeData']):
        errors.append("periodeData kosong")
    if is_blank(row['nomorRekening']):
        errors.append("nomorRekening kosong")
    elif not is_alphanumeric(row['nomorRekening']):
        errors.append("nomorRekening harus alfanumeric")

    if df['nomorRekening'].duplicated().iloc[row.name]:
        errors.append("nomorRekening duplikat")

    if is_blank(row['idDebitur']):
        errors.append("idDebitur kosong")

    if is_blank(row['jenisKreditPembiayaan']):
        errors.append("jenisKreditPembiayaan kosong")

    if is_blank(row['nomorAkadAwal']):
        errors.append("nomorAkadAwal kosong")

    tgl_awal, tgl_akhir = None, None

    if is_blank(row['tanggalAkadAwal']):
        errors.append("tanggalAkadAwal kosong")
    elif not is_valid_date_format(row['tanggalAkadAwal']):
        errors.append("tanggalAkadAwal format salah")
    elif contains_invalid_char(row['tanggalAkadAwal']) or is_not_numeric_only(str(row['tanggalAkadAwal']).replace("-", "")):
        errors.append("tanggalAkadAwal mengandung karakter tidak valid")
    else:
        tgl_awal = datetime.strptime(str(row['tanggalAkadAwal']), "%Y-%m-%d")

    if is_blank(row['nomorAkadAkhir']):
        errors.append("nomorAkadAkhir kosong")

    if is_blank(row['tanggalAkadAkhir']):
        errors.append("tanggalAkadAkhir kosong")
    elif not is_valid_date_format(row['tanggalAkadAkhir']):
        errors.append("tanggalAkadAkhir format salah")
    else:
        tgl_akhir = datetime.strptime(str(row['tanggalAkadAkhir']), "%Y-%m-%d")
        if tgl_awal and tgl_akhir < tgl_awal:
            errors.append("tanggalAkadAkhir lebih muda dari tanggalAkadAwal")

    def valid_tanggal_berurutan(field):
        val = row.get(field, "")
        if is_blank(val):
            errors.append(f"{field} kosong")
        elif not is_valid_date_format(val):
            errors.append(f"{field} format salah")
        else:
            try:
                tgl = datetime.strptime(str(val), "%Y-%m-%d")
                # if tgl_awal and tgl < tgl_awal:
                #     errors.append(f"{field} lebih muda dari tanggalAkadAwal")
                # if tgl_akhir and tgl < tgl_akhir:
                #     errors.append(f"{field} lebih muda dari tanggalAkadAkhir")
            except:
                errors.append(f"{field} gagal diproses")

    for kolom in ['tanggalAwal', 'tanggalMulai', 'tanggalJatuhTempo']:
        valid_tanggal_berurutan(kolom)

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
df['hasil_validasi'] = df.apply(validate_row, axis=1)

# Nama file output berdasarkan input file
output_file = os.path.join(folder_path, f'hasil_validasi_{os.path.basename(input_file)}')
df.to_excel(output_file, index=False)

print(f"Validasi selesai. File disimpan sebagai '{output_file}'.")
