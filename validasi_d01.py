import pandas as pd
import re
import os
import time
from datetime import datetime

# ============================
# Cari File Input
# ============================
folder_path = os.path.dirname(os.path.abspath(__file__))
input_files = [f for f in os.listdir(folder_path) if 'D01' in f and f.endswith(('.xlsx', '.xls'))]

if not input_files:
    raise FileNotFoundError("Tidak ada file yang mengandung 'D01' ditemukan di folder ini.")

input_file = os.path.join(folder_path, input_files[0])
print(f"Membaca file: {input_file}")

# ============================
# Baca File Excel
# ============================
dtype_setting = {
    'No Identitas': str,
    'No Pokok Wajib Pajak': str,
    'Kode Pekerjaan': str,
    'Penghasilan Kotor Per-Tahun': str,
    'Jmlh Tanggungan': str,
    'Kode Kantor Cabang': str,
    'Kode Kabupaten atau Kota': str,
    'Kode Pos': str,
    'No Telepon': str,
    'No Telepon Seluler': str,
    'Kode Bidang Usaha Tempat Bekerja': str,
    'Jmlh Tanggungan': int

}


df = pd.read_excel(input_file, dtype=dtype_setting)

# ============================
# Fungsi Validasi Umum
# ============================
def is_blank(val):
    return pd.isna(val) or str(val).strip() == ""

def only_letters(val):
    return bool(re.fullmatch(r'[A-Za-z]+', str(val).strip()))

def only_digits(val, length=None):
    digits_only = bool(re.fullmatch(r'\d+', str(val).strip()))
    if length:
        return digits_only and len(str(val).strip()) == length
    return digits_only

def no_space(val):
    return ' ' not in str(val)

def no_special_chars(val):
    return not re.search(r'[^\w]', str(val))

def is_valid_date(val):
    try:
        datetime.strptime(val, "%Y-%m-%d")
        return True
    except ValueError:
        return False

def is_valid_email(val):
    return bool(re.fullmatch(r"[^@]+@[^@]+\.[^@]+", str(val).strip()))

# ============================
# Fungsi Validasi Per Baris
# ============================
def validate_row(row):
    errors = []

    # Validasi "Flag Detail"
    flag = str(row.get('Flag Detail', '')).strip()
    if is_blank(flag) or flag != 'D':
        errors.append("Flag Detail harus 'D' dan tidak kosong")

    # Validasi "Nomor CIF"
    cif = str(row.get('Nomor CIF', '')).strip()
    if is_blank(cif):
        errors.append("Nomor CIF kosong")

    # Validasi "Jenis Identitas"
    jenis_id = str(row.get('Jenis Identitas', '')).strip()
    if is_blank(jenis_id) or not only_digits(jenis_id):
        errors.append("Jenis Identitas harus numerik dan tidak kosong")

    # Validasi "Nomor Identitas"
    no_id = str(row.get('No Identitas', '')).strip()
    if is_blank(no_id) or not no_space(no_id) or not no_special_chars(no_id):
        errors.append("Nomor Identitas tidak boleh kosong, mengandung spasi, atau karakter spesial")

    # Validasi "Nama Lengkap"
    nama = str(row.get('Nama Lengkap', '')).strip()
    if is_blank(nama) or not only_letters(nama.replace(" ", "")):
        errors.append("Nama Lengkap tidak boleh kosong dan harus alfabet")

    # Validasi "Alamat"
    alamat = str(row.get('Alamat', '')).strip()
    if is_blank(alamat):
        errors.append("Alamat tidak boleh kosong")

    # Validasi "Tanggal Lahir"
    tgl_lahir = row.get('Tanggal Lahir', '')
    if is_blank(tgl_lahir) or not is_valid_date(tgl_lahir):
        errors.append("Tanggal Lahir kosong atau format salah (YYYY-MM-DD)")

    # Validasi "Email"
    email = str(row.get('Email', '')).strip()
    if is_valid_email(email):
        errors.append("Email tidak valid")

        # Validasi "Kode Negara Domisili"
    negara = str(row.get('Kode Negara Domisili', '')).strip()
    if is_blank(negara) or negara != 'ID':
        errors.append("Kode Negara Domisili harus 'ID' dan tidak kosong")

    # Validasi "Kode Pekerjaan"
    kode_pekerjaan = str(row.get('Kode Pekerjaan', '')).strip()
    if is_blank(kode_pekerjaan) or not only_digits(kode_pekerjaan, 3):
        errors.append("Kode Pekerjaan harus 3 digit dan tidak kosong")

    # Validasi "Tempat Bekerja"
    tempat_bekerja = str(row.get('Tempat Bekerja', '')).strip()
    if is_blank(tempat_bekerja) or len(tempat_bekerja) > 50:
        errors.append("Tempat Bekerja tidak boleh kosong dan maksimal 50 karakter")

    # Validasi "Kode Bidang Usaha Tempat Bekerja"
    kode_bidang = str(row.get('Kode Bidang Usaha Tempat Bekerja', '')).strip()
    if is_blank(kode_bidang) or not only_digits(kode_bidang, 6):
        errors.append("Kode Bidang Usaha Tempat Bekerja harus 6 digit dan tidak kosong")

    # Validasi "Alamat Tempat Bekerja"
    alamat_kerja = str(row.get('Alamat Tempat Bekerja', '')).strip()
    if is_blank(alamat_kerja) or len(alamat_kerja) > 150:
        errors.append("Alamat Tempat Bekerja tidak boleh kosong dan maksimal 150 karakter")

    # Validasi "Penghasilan Kotor Per-Tahun"
    penghasilan = str(row.get('Penghasilan Kotor Per-Tahun', '')).strip()
    if is_blank(penghasilan) or not only_digits(penghasilan) or len(penghasilan) > 12:
        errors.append("Penghasilan Kotor Per-Tahun harus numerik, tidak kosong, dan maksimal 12 digit")

    # Validasi "Kode Sumber Penghasilan"
    sumber_penghasilan = str(row.get('Kode Sumber Penghasilan', '')).strip()
    if is_blank(sumber_penghasilan) or not only_digits(sumber_penghasilan, 1):
        errors.append("Kode Sumber Penghasilan harus 1 digit numerik dan tidak kosong")

    # Validasi "Jmlh Tanggungan"
    tanggungan = str(row.get('Jmlh Tanggungan', '')).strip()
    if is_blank(tanggungan) or not only_digits(tanggungan):
        errors.append("Jmlh Tanggungan harus numerik dan tidak kosong")

    # Validasi "Kode Hub. dengan Pelapor"
    hubungan = str(row.get('Kode Hub. dengan Pelapor', '')).strip()
    if is_blank(hubungan) or hubungan != 'N':
        errors.append("Kode Hub. dengan Pelapor harus 'N' dan tidak kosong")

    # Validasi "Kode Golongan Debitur"
    golongan = str(row.get('Kode Golongan Debitur', '')).strip()
    if is_blank(golongan):
        errors.append("Kode Golongan Debitur tidak boleh kosong")

    # Validasi "Status Perkawinan Debitur"
    status_kawin = str(row.get('Status Perkawinan Debitur', '')).strip()
    if is_blank(status_kawin) or not only_digits(status_kawin):
        errors.append("Status Perkawinan Debitur harus numerik dan tidak kosong")

    # Validasi "No Identitas Pasangan"
    no_id_pasangan = str(row.get('No Identitas Pasangan', '')).strip()
    if not no_space(no_id_pasangan) or not no_special_chars(no_id_pasangan):
        errors.append("No Identitas Pasangan tidak boleh mengandung spasi atau karakter spesial")

    # Validasi "Nama Pasangan"
    nama_pasangan = str(row.get('Nama Pasangan', '')).strip()
    if re.search(r'[!@#$%^&*()_+?\-]', nama_pasangan):
        errors.append("Nama Pasangan tidak boleh mengandung karakter spesial (!@#$%^&*()_+?-)")

    # # Validasi "Tanggal Lahir Pasangan"
    # tgl_lahir_pasangan = str(row.get('Tanggal Lahir Pasangan', '')).strip()
    # if (re.search(r'[A-Za-z]', tgl_lahir_pasangan) or
    #     re.fullmatch(r'\d+', tgl_lahir_pasangan) or
    #     ' ' in tgl_lahir_pasangan or
    #     re.search(r'[!@#$%^&*()_+?-]', tgl_lahir_pasangan) or
    #     not is_valid_date(tgl_lahir_pasangan)):
    #     errors.append("Tanggal Lahir Pasangan tidak valid. Format harus yyyy-mm-dd tanpa alfabet, spasi, atau karakter spesial")

    # Validasi "Tanggal Lahir Pasangan"
    # tgl_lahir_pasangan = str(row.get('Tanggal Lahir Pasangan', '')).strip()
    # if tgl_lahir_pasangan:  # hanya validasi jika kolom tidak kosong
    #     if (re.search(r'[A-Za-z]', tgl_lahir_pasangan) or
    #         re.fullmatch(r'\d+', tgl_lahir_pasangan) or
    #         ' ' in tgl_lahir_pasangan or
    #         re.search(r'[!@#$%^&*()_+?]', tgl_lahir_pasangan) or
    #         not is_valid_date(tgl_lahir_pasangan)):
    #         errors.append("Tanggal Lahir Pasangan tidak valid. Format harus yyyy-mm-dd tanpa alfabet, spasi, atau karakter spesial")

    # tgl_lahir_pasangan = str(row.get('Tanggal Lahir Pasangan', '')).strip()
    # if tgl_lahir_pasangan:
    #     if not re.fullmatch(r'\d{4}-\d{2}-\d{2}', tgl_lahir_pasangan) or not is_valid_date(tgl_lahir_pasangan):
    #         errors.append("Tanggal Lahir Pasangan tidak valid. Format harus yyyy-mm-dd")

    nama_pasangan = str(row.get('Nama Pasangan', '')).strip()
    tgl_lahir_pasangan = str(row.get('Tanggal Lahir Pasangan', '')).strip()

    # Hanya lakukan validasi jika Nama Pasangan terisi
    if nama_pasangan:
        if tgl_lahir_pasangan:
            if not re.fullmatch(r'\d{4}-\d{2}-\d{2}', tgl_lahir_pasangan) or not is_valid_date(tgl_lahir_pasangan):
                errors.append("Tanggal Lahir Pasangan tidak valid. Format harus yyyy-mm-dd")
        else:
            errors.append("Tanggal Lahir Pasangan wajib diisi jika Nama Pasangan terisi")

    # Validasi "Perjanjian Pisah Harta"
    pisah_harta = str(row.get('Perjanjian Pisah Harta', '')).strip()
    if is_blank(pisah_harta) or pisah_harta != 'T':
        errors.append("Perjanjian Pisah Harta harus 'T' dan tidak kosong")

    # Validasi "Melanggar BMPK BMPD BMPP"
    melanggar = str(row.get('Melanggar BMPK BMPD BMPP', '')).strip()
    if is_blank(melanggar) or melanggar != 'T':
        errors.append("Melanggar BMPK BMPD BMPP harus 'T' dan tidak kosong")

    # Validasi "Melampaui BMPK BMPD BMPP"
    melampaui = str(row.get('Melampaui BMPK BMPD BMPP', '')).strip()
    if is_blank(melampaui) or melampaui != 'T':
        errors.append("Melampaui BMPK BMPD BMPP harus 'T' dan tidak kosong")

    # Validasi "Nama Gadis Ibu Kandung"
    ibu_kandung = str(row.get('Nama Gadis Ibu Kandung', '')).strip()
    if re.search(r'[!@#$%^&*()_+?-]', ibu_kandung):
        errors.append("Nama Gadis Ibu Kandung tidak boleh mengandung karakter spesial (!@#$%^&*()_+?-)")

    # Validasi "Kode Kantor Cabang"
    kode_kantor = str(row.get('Kode Kantor Cabang', '')).strip()
    if is_blank(kode_kantor) or kode_kantor != '001':
        errors.append("Kode Kantor Cabang harus '001' dan tidak kosong")

    # Validasi "Operasi Data"
    operasi = str(row.get('Operasi Data', '')).strip()
    if is_blank(operasi):
        errors.append("Operasi Data tidak boleh kosong")


    return "VALID" if not errors else "; ".join(errors)

# ============================
# Eksekusi Validasi
# ============================
start_time = time.time()

df["hasil_validasi"] = df.apply(validate_row, axis=1)

end_time = time.time()
elapsed_time = end_time - start_time

# ============================
# Simpan Hasil
# ============================
output_file = os.path.join(folder_path, f"hasil_validasi_{os.path.basename(input_file)}")
df.to_excel(output_file, index=False)

print(f"Validasi selesai. File hasil: '{output_file}'")
print(f"Waktu proses: {elapsed_time:.2f} detik")
