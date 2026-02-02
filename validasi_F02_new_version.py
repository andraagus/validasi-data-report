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
# |Kode Jenis Penggunaan|Kode Orientasi Penggunaan|Kode Sektor Ekonomi|Kode Kab./ Kota Lokasi Proyek
# |Nilai Proyek|Kode Valuta|Suku Bunga atau Imbalan|Jenis Suku Bunga
# |Kredit  Program Pemerintah|Asal Kredit  Takeover|Sumber Dana|Plafon Awal|Plafon|Realisasi
# |Denda|Baki Debet|Nilai Dlm Mata Uang Asal|Kode Kualitas Kredit|Tanggal Macet|Kode Sebab Macet
# |Tunggakan Pokok|Tunggakan Bunga|Jmlh Hari Tunggakan|Frekuensi Tunggakan|Frekuensi Restrukturisasi
# |Tanggal Restrukturisasi Awal|Tanggal Restrukturisasi Akhir|Kode Cara Restrukturisasi|Kode Kondisi
# |Tanggal Kondisi|Keterangan|Kode Kantor Cabang|Operasi Data

#...

# ==========================================
# STEP 1: FUNGSI VALIDASI MODULAR (UNIT)
# ==========================================

def validate_not_blank(series):
    return series.isna() | (series.astype(str).str.strip() == "")

def validate_alphanumeric(series):
    return series.astype(str).str.match(r'^[a-zA-Z0-9]+$')

def validate_numeric_only(series):
    return series.astype(str).str.match(r'^\d+$')

def validate_is_exactly_digits(series, length):
    # Vectorized check untuk panjang digit
    return (series.astype(str).str.len() != length) | (~series.astype(str).str.isdigit())

def validate_kode_sifat_kredit(series):
    valid_values = ['1', '2', '3', '4', '5', '6', '7', '8', '9']
    return ~series.isin(valid_values)

# Fungsi-fungsi List (Menerima Series, Mengembalikan Boolean Mask)
def validate_kode_jenis_kredit(series):
    valid_values = ['P11', 'P04', 'P03', 'N01', 'P99', 'P06', 'P10', 'P08', 'P02', 'P05', 'N02', 'N99', 'P01', 'P09', 'P07']
    return ~series.isin(valid_values)

def validate_kode_jenis_penggunaan(series):
    valid_values = ['3', '2', '1']
    return ~series.isin(valid_values)

def validate_jenisSukuBungaImbalan(series):
    valid_values = ['4', '3', '0', '2', '9', '5', '1']
    return ~series.isin(valid_values)

def validate_kode_program_pemerintah(series):
    valid_values = ['90', '24', '25', '10', '22', '30', '23', '21']
    return ~series.isin(valid_values)

def validate_kode_kabupaten(series):   
    valid_values = ['0910', '7205', '1295', '3396', '8231', '0122', '6009', '3496', '3909', '5102', '6391', '8238', 
                    '3616', '5311', '3307', '5406', '0394', '8401', '2301', '8192', '3402', '0908', '7414', '8210', 
                    '0924', '1203', '3109', '0194', '6008', '5308', '6302', '1226', '3494', '3305', '8213', '3397',
                    '7204', '8234', '8222', '8108', '0115', '6191', '0994', '3392', '8233', '3313', '3512', '3217', 
                    '0921', '1208', '5301', '6002', '8105', '5309', '5401', '7413', '0993', '6102', '2306', '7415', 
                    '1217', '7102', '7103', '6402', '0106', '0202', '7411', '1209', '8409', '1214', '1291', '6005', 
                    '0201', '0914', '0904', '5106', '0198', '6202', '3491', '3802', '3892', '7410', '3215', '8224', 
                    '8104', '3609', '6119', '3214', '3509', '8107', '3326', '2307', '1211', '0196', '3399', '3404', 
                    '0112', '6006', '3304', '8201', '5812', '6125', '0121', '5304', '5303', '6291', '6292', '7206', 
                    '7407', '3912', '3497', '0911', '6904', '7405', '3902', '3691', '8212', '3495', '6110', '5808', 
                    '3393', '5413', '0180', '3112', '3505', '6909', '0925', '1292', '1215', '0917', '0916', '3306', 
                    '5811', '8228', '8305', '6911', '5802', '5809', '7421', '7412', '6305', '3905', '1296', '0902', 
                    '3613', '3504', '6294', '3803', '6907', '3707', '0114', '8309', '5409', '3105', '6111', '3323', 
                    '1271', '0926', '6122', '5109', '0395', '6115', '3106', '7417', '6004', '3891', '7408', '1294', 
                    '6213', '3208', '0393', '3391', '6011', '3206', '5402', '5312', '5392', '3406', '6212', '0504', 
                    '1205', '3611', '8191', '6091', '8240', '7108', '6118', '0391', '3205', '0192', '7416', '7191', 
                    '3211', '6210', '7402', '1228', '3293', '3303', '8410', '3203', '6105', '3992', '3204', '3411', 
                    '9999', '5494', '0117', '3907', '8304', '3401', '0103', '0503', '5801', '1207', '3111', '0293', 
                    '3610', '8217', '1216', '6991', '6204', '8226', '8302', '8407', '6990', '8239', '8109', '3213', 
                    '3316', '2304', '0119', '3319', '7403', '8307', '3615', '0903', '8216', '3617', '8202', '3219',
                    '0204', '1206', '0102', '7420', '5813', '1222', '7101', '0195', '3493', '3322', '3216', '0905', 
                    '7419', '3192', '2303', '3412', '0116', '1218', '3703', '6404', '5403', '0291', '6012', '8214', 
                    '0927', '0929', '5306', '3508', '3202', '1213', '3302', '7418', '5305', '3210', '3212', '2305', 
                    '6007', '6916', '3903', '6906', '3409', '3201', '5805', '6112', '5404', '6103', '5101', '0203', 
                    '3804', '5892', '5804', '3317', '3702', '5391', '6406', '7208', '5492', '6908', '1201', '1229', 
                    '0193', '6124', '0915', '7106', '1230', '8103', '3407', '8405', '3310', '7409', '7406', '6113', 
                    '3294', '0292', '3607', '8235', '3697', '3209', '3911', '3101', '2309', '0991', '3991', '0920', 
                    '1225', '6293', '0197', '3614', '3591', '6905', '3318', '3694', '3395', '3910', '6913', '0502', 
                    '3501', '6205', '0591', '0109', '8291', '3325', '0995', '6401', '7192', '6405', '0113', '3606', 
                    '8308', '8218', '1219', '0918', '0907', '0922', '1224', '3291', '3608', '3705', '6192', '8101', 
                    '5493', '0906', '6211', '6207', '8211', '3314', '6010', '0928', '1221', '0919', '7404', '8412', 
                    '3513', '8406', '0110', '2391', '6001', '3309', '3805', '8232', '8221', '5107', '7104', '6121', 
                    '3108', '3324', '8236', '8106', '3301', '6116', '5491', '5405', '5111', '3901', '3408', '3218', 
                    '6903', '8215', '0123', '7207', '3592', '5803', '3104', '0913', '7105', '3410', '3207', '6107', 
                    '5810', '5191', '1297', '1223', '3394', '3906', '6109', '5108', '0392', '6910', '3704', '3701', 
                    '6912', '3107', '3619', '3492', '3315', '7201', '0111', '3191', '1220', '3405', '3321', '7291', 
                    '0901', '6915', '8491', '3510', '1227', '0191', '8227', '0923', '8237', '5410', '8391', '3904', 
                    '8223', '1202', '7203', '2308', '6901', '1298', '1204', '3502', '8102', '3292', '0501', '6301', 
                    '6203', '5192', '5806', '6304', '7401', '6209', '0294', '1212', '5110', '3693', '8306', '5103',
                    '6403', '3311', '8303', '0118', '5411', '3706', '8408', '8403', '5807', '3511', '3618', '6117', 
                    '6114', '5310', '8411', '1293', '6206', '5307', '6101', '0912', '8402', '6914', '8404', '3327', 
                    '5302', '6193', '0996', '0909', '0108', '2302', '7202', '8390', '3801', '3913', '7107', '5104', 
                    '3908', '3308', '5412', '6303', '7491', '5105', '0992', '0396', '6106', '6003', '3403']
    return ~series.isin(valid_values)    

def validate_kode_kategori_debitur(series):
    valid_values = ['UM', 'UK', 'NU', 'UT']
    return ~series.isin(valid_values)

def validate_kode_orientasi_penggunaan(series):
    valid_values = ['1', '2', '3']  
    return ~series.isin(valid_values)

def validate_kode_kualitas_kredit(series):
    valid_values = ['1', '2', '3', '4', '5']
    return ~series.isin(valid_values)

def validate_tanggal_format(series):
    # Cek format tanggal YYYY-MM-DD
    return ~series.astype(str).str.match(r'^\d{4}-\d{2}-\d{2}$')

def validate_kode_kondisi(series):
    valid_values = ['00', '01', '02', '03', '04', '05', '06','07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17']
    return ~series.isin(valid_values)   

def validate_kode_sebab_macet(series):
    valid_values = ['00', '01', '02', '03', '04', '05', '06','07', '08', '09', '10', '11', '99']
    return ~series.isin(valid_values)   

def validate_kode_cara_restrukturisasi(series):
    valid_values = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18','19', '20','21','99']
    return ~series.isin(valid_values)

def validate_sektor_ekonomi(series):
    valid_values = ['001110','001120','001130','001210','001220','001230','001300','002100','002200','002300','002900','003100','003200','003300','003900','004120',
                    '004130','004140','004150','004160','004170','004180','004190','004900','009000','011110','011130','011140','011190','011200','011301','011302',
                    '011303','011309','011400','011500','011600','011909','011930','011940','012201','012209','012300','012400','012500','012610','012620','012690',
                    '012701','012702','012703','012709','012810','012820','012830','012840','012850','012891','012892','012899','012910','012990','013010','013020',
                    '014110','014120','014130','014140','014400','014500','014600','014900','016000','017000','021100','021200','021300','021400','022090','024000',
                    '031111','031119','031121','031129','031190','031210','031290','031300','031400','032101','032102','032109','032201','032202','032209','032300',
                    '032400','032501','032509','032600','050000','060001','060002','071000','072100','072910','072930','072940','072950','072990','073011','073012',
                    '073090','081000','089100','089300','089900','091000','099000','101000','102000','103001','103009','104100','104210','104230','104300','104900',
                    '105000','106100','106200','106300','107100','107200','107300','107400','107610','107630','107710','107900','108000','110000','120100','120900',
                    '131000','139000','141000','142000','143000','151000','152000','161000','162100','162900','170100','170200','170900','181000','182000','191000',
                    '192100','192900','201100','201200','201300','202100','202200','202300','202940','202990','203000','210000','221210','221220','221230','221900',
                    '222000','231000','239200','239301','239302','239400','239600','239900','241000','242060','242090','243100','243200','251000','259300','259900',
                    '261000','262000','263000','264000','265100','265200','266000','267000','269000','271100','271200','272000','273000','274000','275000','279000',
                    '281000','282100','282400','282500','282600','282900','291000','292000','293000','301000','302000','303000','309110','309900','310000','320000',
                    '330000','351001','351002','352000','353000','360000','370000','380000','390000','410111','410112','410113','410114','410115','410119','410120',
                    '410130','410141','410149','410190','421101','421102','421103','421104','421109','422110','422131','422139','422190','429120','429190','431201',
                    '431202','431209','432000','433000','439050','439090','451000','452000','453000','454001','454002','454003','461000','462011','462019',
                    '462020','462040','462050','462060','462071','462079','462080','462091','462092','462093','462094','462095','462099','463110','463141','463142',
                    '463150','463190','463201','463209','463301','463302','463309','464110','464120','464130','464190','464900','465000','466100','466200','466301',
                    '466309','466920','466930','466950','466970','466990','471100','471900','472001','472009','473000','474000','475100','475200','475900','476000',
                    '477100','477200','477300','477400','477700','477800','477900','478100','478200','478300','478400','478600','478700','478800','478920','478940',
                    '478990','479100','479900','491000','492100','492210','492290','493000','494100','494200','494300','494501','494509','501100','501130','501190',
                    '501200','501300','501400','502101','502102','502200','511001','511002','511009','512000','521000','522000','530000','551100','551200','559000',
                    '561001','561009','580000','591000','592000','600000','610001','610002','610009','620100','620200','631110','631120','631210','631220','639100',
                    '639900','641000','649100','649900','650000','661001','661009','662000','681101','681102','681103','681104','681105','681106','681107','681108',
                    '681109','681200','681300','682000','690000','702010','702090','710000','721000','722000','730000','740000','750000','771000','772000','773020',
                    '773030','773040','773050','773060','773070','773090','780000','791110','791120','791200','799000','823000','829000','841000','842000','843000','851000','852000','853000','854000',
                    '855000','861000','862000','869000','870000','900001','900009','910100','910200','930000','941000','942000','949000','950000','960001','960009',
                    '970000','990000']
    return ~series.isin(valid_values)

# ==========================================
# STEP 2: PROSES UTAMA
# ==========================================

def run_validation():
    folder_path = os.path.dirname(os.path.abspath(__file__))
    f02_files = [f for f in os.listdir(folder_path) if 'F02' in f and f.endswith(('.xlsx', '.xls'))]

    if not f02_files:
        print("Tidak ada file F02 ditemukan.")
        return

    input_file = os.path.join(folder_path, f02_files[0])
    print(f"Membaca file: {os.path.basename(input_file)}...")
    
    df = pd.read_excel(input_file, dtype=str)
    
    for col in df.columns:
        df[col] = df[col].astype(str).str.strip().replace(['nan', 'NaN', 'None'], '')

    error_list = []

    def add_error(condition, message):
        error_series = pd.Series("", index=df.index)
        error_series[condition] = message
        error_list.append(error_series)

    print("Proses validasi sedang berjalan...")

    # --- Flag Detail ---#
    add_error(~df['Flag Detail'].isin(['D', 'S']), "flagDetail harus 'D' atau 'S'")
    
    # --- Validasi Dasar ---
    add_error(validate_not_blank(df['No CIF Debitur']), "CIF Debitur kosong")
    add_error(df['Flag Detail'] != 'D', "flagDetail harus 'D'")
    
    #-- Seq Deb. Anggota Joint Account ---
    is_sda_blank = validate_not_blank(df['Seq Debitur Anggota Joint Account'])
    add_error(is_sda_blank, "Seq Debitur Anggota Joint Account kosong")


    # --- nomorRekening ---
    is_rek_blank = validate_not_blank(df['No Rekening Fasilitas'])
    add_error(is_rek_blank, "nomorRekening kosong")
    add_error(~validate_alphanumeric(df['No Rekening Fasilitas']) & ~is_rek_blank, "nomorRekening harus alphanumeric")
    add_error(df['No Rekening Fasilitas'].duplicated(keep=False) & ~is_rek_blank, "nomorRekening duplikat")

    # --- kode sifat kredit ---
    is_jkp_blank = validate_not_blank(df['Kode Sifat Kredit'])
    add_error(is_jkp_blank, "Kode Sifat Kredit kosong")
    add_error(validate_kode_sifat_kredit(df['Kode Sifat Kredit']) & ~is_jkp_blank, "Kode Sifat Kredit tidak valid")


    # --- jenisKreditPembiayaan ---
    is_jkp_blank = validate_not_blank(df['Kode Jenis Kredit'])
    add_error(is_jkp_blank, "Kode Jenis Kredit kosong")
    add_error(validate_kode_jenis_kredit(df['Kode Jenis Kredit']) & ~is_jkp_blank, "Kode Jenis Kredit tidak valid")

    #-- vakidasi kode akad kredit ---
    is_kak_blank = validate_not_blank(df['Kode Akad Kredit'])
    add_error(is_kak_blank, "Kode Akad Kredit kosong")
    add_error(~validate_alphanumeric(df['Kode Akad Kredit']) & ~is_kak_blank, "Kode Akad Kredit harus alphanumeric")

    #-- nomorAkadAwal ---
    is_naa_blank = validate_not_blank(df['No Akad Awal'])
    add_error(is_naa_blank, "nomorAkadAwal kosong")
    

    #-- nomorAkadAkhir ---
    is_nak_blank = validate_not_blank(df['No Akad Akhir'])
    add_error(is_nak_blank, "nomorAkadAkhir kosong")
    

    #-- tanggalAkadAwal ---
    is_taa_blank = validate_not_blank(df['Tanggal Akad Awal'])
    add_error(is_taa_blank, "tanggalAkadAwal kosong")
    add_error(validate_tanggal_format(df['Tanggal Akad Awal']) & ~is_taa_blank, "tanggalAkadAwal format salah")

    #-- tanggalAkadAkhir ---
    is_tak_blank = validate_not_blank(df['Tanggal Akad Akhir'])
    add_error(is_tak_blank, "tanggalAkadAkhir kosong")
    add_error(validate_tanggal_format(df['Tanggal Akad Akhir']) & ~is_tak_blank, "tanggalAkadAkhir format salah")

    #-- Fequensi Perpanjangan Fasilitas Kredit ---
    is_fpfk_blank = validate_not_blank(df['Freq Perpanjangan Fasilitas Kredit'])
    add_error(is_fpfk_blank, "Freq Perpanjangan Fasilitas Kredit kosong")
    add_error(~validate_numeric_only(df['Freq Perpanjangan Fasilitas Kredit']) & ~is_fpfk_blank, "Freq Perpanjangan Fasilitas Kredit harus numeric")

    #-- tanggal awal kredit ---
    is_tak_blank = validate_not_blank(df['Tanggal Awal Kredit'])
    add_error(is_tak_blank, "Tanggal Awal Kredit kosong")
    add_error(validate_tanggal_format(df['Tanggal Awal Kredit']) & ~is_tak_blank, "Tanggal Awal Kredit format salah")

    #-- tanggal mulai ---
    is_tm_blank = validate_not_blank(df['Tanggal Mulai'])
    add_error(is_tm_blank, "Tanggal Mulai kosong")
    add_error(validate_tanggal_format(df['Tanggal Mulai']) & ~is_tm_blank, "Tanggal Mulai format salah")

    #-- tanggal jatuh tempo ---
    is_tjt_blank = validate_not_blank(df['Tanggal Jatuh Tempo'])
    add_error(is_tjt_blank, "Tanggal Jatuh Tempo kosong")
    add_error(validate_tanggal_format(df['Tanggal Jatuh Tempo']) & ~is_tjt_blank, "Tanggal Jatuh Tempo format salah")


    # --- Tanggal & Math ---
    tgl_awal = pd.to_datetime(df['Tanggal Akad Awal'], format='%Y-%m-%d', errors='coerce')
    tgl_akhir = pd.to_datetime(df['Tanggal Akad Akhir'], format='%Y-%m-%d', errors='coerce')
    add_error(tgl_awal.isna() & ~validate_not_blank(df['Tanggal Akad Awal']), "Tanggal Akad Awal format salah")
    add_error((tgl_akhir < tgl_awal) & tgl_awal.notna() & tgl_akhir.notna(), "Tanggal Akad Akhir < Tanggal Akad Awal")

    # --- katego kategori Debitur ---
    is_kud_blank = validate_not_blank(df['Kode Kategori Debitur'])
    add_error(is_kud_blank, "Kode kategori Debitur kosong")
    add_error(validate_kode_kategori_debitur(df['Kode Kategori Debitur']) & ~is_kud_blank, "Kode kategori Debitur tidak valid")

    # --- jenisPenggunaan ---
    is_jp_blank = validate_not_blank(df['Kode Jenis Penggunaan'])
    add_error(is_jp_blank, "Kode Jenis Penggunaan kosong")  
    add_error(validate_kode_jenis_penggunaan(df['Kode Jenis Penggunaan']) & ~is_jp_blank, "Kode Jenis Penggunaan tidak valid")

    # --- orientasiPenggunaan ---
    is_op_blank = validate_not_blank(df['Kode Orientasi Penggunaan'])
    add_error(is_op_blank, "Kode Orientasi Penggunaan kosong")
    add_error(validate_kode_orientasi_penggunaan(df['Kode Orientasi Penggunaan']) & ~is_op_blank, "Kode Orientasi Penggunaan tidak valid")

    # --- kreditProgramPemerintah ---
    is_kpp_blank = validate_not_blank(df['Kredit  Program Pemerintah'])
    add_error(is_kpp_blank, "Kredit  Program Pemerintah kosong")
    add_error(validate_kode_program_pemerintah(df['Kredit  Program Pemerintah']) & ~is_kpp_blank, "Kode Program Pemerintah tidak valid")

    # --- sektorEkonomi ---
    is_se_blank = validate_not_blank(df['Kode Sektor Ekonomi'])
    add_error(is_se_blank, "Kode Sektor Ekonomi kosong")
    # Digits check (vectorized)
    add_error(validate_is_exactly_digits(df['Kode Sektor Ekonomi'], 6) & ~is_se_blank, "Kode Sektor Ekonomi harus 6 digit angka")
    add_error(validate_sektor_ekonomi(df['Kode Sektor Ekonomi']) & ~is_se_blank, "Kode Sektor Ekonomi tidak valid")    


    # --- lokasi Kab/Kota ---
    is_lp_blank = validate_not_blank(df['Kode Kab. / Kota Lokasi Proyek'])
    add_error(is_lp_blank, "Kode Kab./ Kota Lokasi Proyek kosong")
    add_error(validate_is_exactly_digits(df['Kode Kab. / Kota Lokasi Proyek'], 4) & ~is_lp_blank, "Kode Kab./ Kota Lokasi Proyek harus 4 digit angka")
    # List check (panggil langsung tanpa .apply)
    add_error(validate_kode_kabupaten(df['Kode Kab. / Kota Lokasi Proyek']) & ~is_lp_blank, "Kode Kab./ Kota Lokasi Proyek tidak valid")

    # --- Nilai Proyek ---
    is_np_blank = validate_not_blank(df['Nilai Proyek'])
    add_error(~is_np_blank & ~validate_numeric_only(df['Nilai Proyek']), "Nilai Proyek harus numeric")

    #-- kode jenis valuta ---
    is_kjv_blank = validate_not_blank(df['Kode Valuta'])
    add_error(is_kjv_blank, "Kode Valuta kosong")
    add_error(~validate_alphanumeric(df['Kode Valuta']) & ~is_kjv_blank, "Kode Valuta harus alphanumeric")

    # --- jenisSukuBungaImbalan ---
    is_jsbi_blank = validate_not_blank(df['Suku Bunga atau Imbalan'])
    add_error(is_jsbi_blank, "Suku Bunga atau Imbalan kosong")
    
    #-- Jenis Suku Bunga ---
    is_jsb_blank = validate_not_blank(df['Jenis Suku Bunga'])
    add_error(is_jsb_blank, "Jenis Suku Bunga kosong")
    add_error(validate_jenisSukuBungaImbalan(df['Jenis Suku Bunga']) & ~is_jsbi_blank, "Suku Bunga atau Imbalan tidak valid")


    # --- Kode Kualitas Kredit ---
    is_ka_blank = validate_not_blank(df['Kode Kualitas Kredit'])
    add_error(is_ka_blank, "Kode Kualitas Kredit kosong")
    add_error(validate_kode_sifat_kredit(df['Kode Kualitas Kredit']) & ~is_ka_blank, "Kode Kualitas Kredit tidak valid")

    # ==========================================================
    # VALIDASI KONDISIONAL (HANYA JIKA KUALITAS == 5)
    # ==========================================================
    # Buat variabel bantu untuk baris yang kualitasnya 5 (Macet)
    is_macet = (df['Kode Kualitas Kredit'] == '5')

    #---- tanggal Macet ---
    is_tm_blank = validate_not_blank(df['Tanggal Macet'])
    add_error(is_macet & is_tm_blank, "Tanggal Macet kosong")
    add_error(is_macet & ~is_tm_blank & validate_tanggal_format(df['Tanggal Macet']), "Tanggal Macet format salah")

    #-- Kode Sebab Macet ---
    is_ksm_blank = validate_not_blank(df['Kode Sebab Macet'])
    add_error(is_macet & is_ksm_blank, "Kode Sebab Macet kosong")
    add_error(is_macet & ~is_ksm_blank & validate_kode_sebab_macet(df['Kode Sebab Macet']), "Kode Sebab Macet tidak valid")

    #-- Tunggakan Pokok ---
    is_tp_blank = validate_not_blank(df['Tunggakan Pokok'])
    add_error(is_macet & is_tp_blank, "Tunggakan Pokok kosong")    
    add_error(is_macet & ~is_tp_blank & ~validate_numeric_only(df['Tunggakan Pokok']), "Tunggakan Pokok harus numeric")

    #-- Tunggakan Bunga --- 
    is_tb_blank = validate_not_blank(df['Tunggakan Bunga'])
    add_error(is_macet & is_tb_blank, "Tunggakan Bunga kosong")
    add_error(is_macet & ~is_tb_blank & ~validate_numeric_only(df['Tunggakan Bunga']), "Tunggakan Bunga harus numeric")
    #-- Jmlh Hari Tunggakan ---
    is_jht_blank = validate_not_blank(df['Jmlh Hari Tunggakan'])
    add_error(is_macet & is_jht_blank, "Jmlh Hari Tunggakan kosong")
    add_error(is_macet & ~is_jht_blank & ~validate_numeric_only(df['Jmlh Hari Tunggakan']), "Jmlh Hari Tunggakan harus numeric")

    #-- Frekuensi Tunggakan --- 
    is_ft_blank = validate_not_blank(df['Frekuensi Tunggakan'])
    add_error(is_macet & is_ft_blank, "Frekuensi Tunggakan kosong")
    add_error(is_macet & ~is_ft_blank & ~validate_numeric_only(df['Frekuensi Tunggakan']), "Frekuensi Tunggakan harus numeric")
    

    #-- Kode Kondisi ---
    is_kk_blank = validate_not_blank(df['Kode Kondisi'])
    add_error(is_kk_blank, "Kode Kondisi kosong")
    add_error(validate_kode_kondisi(df['Kode Kondisi']) & ~is_kk_blank, "Kode Kondisi tidak valid")

    # ==========================================================
    # VALIDASI KONDISIONAL (HANYA JIKA kode kondisi bukan != "00"
    # ==========================================================
    
    is_tdk_aktif = (df['Kode Kondisi'] != '00')

    #-- Tanggal Kondisi ---
    is_tk_blank = validate_not_blank(df['Tanggal Kondisi'])
    add_error(is_tdk_aktif & is_tk_blank, "Tanggal Kondisi kosong")
    add_error(is_tdk_aktif & ~is_tk_blank & validate_tanggal_format(df['Tanggal Kondisi']), "Tanggal Kondisi format salah")

    #-- Operasi Data ---
    is_od_blank = validate_not_blank(df['Operasi Data'])
    add_error(is_od_blank, "Operasi Data kosong")
    add_error(~df['Operasi Data'].isin(['U', 'C']) & ~is_od_blank, "Operasi Data harus salah satu dari 'U', 'C'")
    

    # --- Validasi Integer (Semua kolom keuangan) ---
    kolom_uang = [
        'Plafon Awal', 'Plafon', 'Realisasi', 'Denda', 'Baki Debet', 'Nilai Dlm Mata Uang Asal', 
        'Tunggakan Pokok', 'Tunggakan Bunga', 'Jmlh Hari Tunggakan', 'Frekuensi Tunggakan', 'Frekuensi Restrukturisasi'        
    ]
    
    for col in kolom_uang:
        is_blank = validate_not_blank(df[col])
        # Cek tipe data integer (Strict)
        add_error(~validate_numeric_only(df[col]) & ~is_blank, f"Missmatch data type in Column {col}, expected integer")

    # ==========================================
    # STEP 3: MENGGABUNGKAN HASIL
    # ==========================================
    print("Menggabungkan hasil validasi...")
    temp_err_df = pd.concat(error_list, axis=1)
    
    # Join string per baris
    df['hasil_validasi'] = temp_err_df.apply(lambda x: "; ".join(filter(None, x)), axis=1)
    df['hasil_validasi'] = df['hasil_validasi'].replace("", "VALID")

    # Simpan
    output_file = os.path.join(folder_path, f'hasil_validasi_{os.path.basename(input_file)}')
    df.to_excel(output_file, index=False)
    print(f"Selesai! File disimpan di: {output_file}")

if __name__ == "__main__":
    run_validation()