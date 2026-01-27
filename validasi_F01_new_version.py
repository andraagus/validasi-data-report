import pandas as pd
import re
import os
import tqdm

...
#... header file F01
# Flag Detail|No Rekening Fasilitas|No CIF Debitur|Kode Sifat Kredit
# |Kode Jenis Kredit|Kode Akad Kredit|No Akad Awal|Tanggal Akad Awal
# |No Akad Akhir|Tanggal Akad Akhir|Freq Perpanjangan Fasilitas Kredit
# |Tanggal Awal Kredit|Tanggal Mulai|Tanggal Jatuh Tempo|Kode Kategori Debitur
# |Kode Jenis Penggunaan|Kode Orientasi Penggunaan|Kode Sektor Ekonomi|Kode Kab. 
# / Kota Lokasi Proyek|Nilai Proyek|Kode Valuta|Suku Bunga atau Imbalan|Jenis Suku Bunga
# |Kredit  Program Pemerintah|Asal Kredit  Takeover|Sumber Dana|Plafon Awal|Plafon|Realisasi
# |Denda|Baki Debet|Nilai Dlm Mata Uang Asal|Kode Kualitas Kredit|Tanggal Macet|Kode Sebab Macet
# |Tunggakan Pokok|Tunggakan Bunga|Jmlh Hari Tunggakan|Frekuensi Tunggakan|Frekuensi Restrukturisasi
# |Tanggal Restrukturisasi Awal|Tanggal Restrukturisasi Akhir|Kode Cara Restrukturisasi|Kode Kondisi
# |Tanggal Kondisi|Keterangan|Kode Kantor Cabang|Operasi Data

#...

## ============
## Functions
## ============

def validate_not_blank(series):
    return series.apply(lambda x: bool(str(x).strip()))

def validate_alphanumeric(series):
    return series.astype(str).str.match(r'^[a-zA-Z0-9]+$')

def validate_numeric(series):
    return series.astype(str).str.match(r'^[0-9]+$')    

def validate_is_exactly_digits(series, length):
    return series.astype(str).str.len() == length & series.astype(str).str.isdigit()

