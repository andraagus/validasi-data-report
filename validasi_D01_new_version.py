import pandas as pd
import re
import os
import tqdm



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

def validate_duplicate_entries(df, subset_cols):
    duplicate_mask = df.duplicated(subset=subset_cols, keep=False)
    return duplicate_mask

def validate_no_nik(series):
    return ~series.astype(str).str.match(r'^\d{16}$')

def validate_special_characters(series):
    return series.astype(str).str.contains(r'[^a-zA-Z0-9\s]', na=False)

def validate_numeric_only(series):
    return series.astype(str).str.match(r'^\d+$')

def valdiate_kode_status_pendidikan(series):
    valid_values = ['00', '01', '02', '03', '04', '05', '06', '99']
    return ~series.isin(valid_values)

def validate_jenis_kelamin(series):
    valid_values = ['P', 'L']
    return ~series.isin(valid_values)

def validate_minimal_karakter(series, min_length):
    return series.astype(str).str.len() < min_length

def validate_maximal_karakter(series, max_length):
    return series.astype(str).str.len() > max_length

def validate_kode_pos(series):
    return ~series.astype(str).str.match(r'^\d{5}$')

def validate_email_format(series):
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return ~series.astype(str).str.match(email_pattern)

def validate_kode_negara_domisili(series):
    valid_values = ['ID', 'AS', 'AU', 'BN', 'CN', 'FR', 'DE', 'HK', 'IN', 'JP', 'KR', 'MY', 'NZ', 'PH', 'SG', 'TH', 'GB']
    return ~series.isin(valid_values)

def validate_kode_pekerjaan(series):    
    valid_values = ['001', '002', '003', '004', '005', '006', '007', '008', '009', '010', 
                    '011', '012', '013', '014', '015', '016', '017', '018', '019', '020', 
                    '021', '022', '023', '024', '025', '026', '027', '028', '029', '030',
                    '031', '032', '033', '034', '035', '036', '037', '099']
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

def validate_exact_length(series, length):
    return series.astype(str).str.len() != length

def validate_kode_sumber_penghasilan(series):
    valid_values = ['1', '2', '3']
    return ~series.isin(valid_values)

def validate_kode_hubungan_dengan_pelapor(series):
    valid_values = ['T1', 'T2', 'T3', 'T4', 'T9', 'N',]
    return ~series.isin(valid_values)   

def validate_kode_golongan_debitur(series):
    valid_values = ['S14', 'S24BL']
    return ~series.isin(valid_values)

def validate_kode_status_perkawinan(series):
    valid_values = ['1', '2', '3']
    return ~series.isin(valid_values)

def validate_perjanjian_pisah_harta(series):
    valid_values = ['Y', 'T']
    return ~series.isin(valid_values)

def validate_melanggar_bmpk_bmpd_bmpp(series):
    valid_values = ['Y', 'T', 'N']
    return ~series.isin(valid_values)

def validate_melampaui_bmpk_bmpd_bmpp(series):
    valid_values = ['Y', 'T', 'N']
    return ~series.isin(valid_values)

def validate_operasi_data(series):
    valid_values = ['C', 'U', 'N']
    return ~series.isin(valid_values)


# ==========================================
# STEP 2: PROSES UTAMA VALIDASI
# ==========================================

def run_validation():
    folder_path = os.path.dirname(os.path.abspath(__file__))
    d01_files = [f for f in os.listdir(folder_path) if 'D01' in f and f.endswith(('.xlsx', '.xls'))]

    if not d01_files:
        print("Tidak ada file D01 ditemukan.")
        return

    input_file = os.path.join(folder_path, d01_files[0])
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

    #---Validate_Flag Detail_---#
    add_error(validate_not_blank(df['Flag Detail']), "Flag Detail kosong")
    add_error(df['Flag Detail'] != 'D', "flagDetail harus 'D'")

    #---Validate_Nomor CIF_---#
    add_error(validate_not_blank(df['Nomor CIF']), "Nomor CIF kosong")
    add_error(validate_duplicate_entries(df, ['Nomor CIF']), "Nomor CIF duplikat")

    #---Validate_Jenis Identitas_---#
    add_error(validate_not_blank(df['Jenis Identitas']), "Jenis Identitas kosong")
    add_error(~df['Jenis Identitas'].isin(['1', '2']), "Jenis Identitas tidak valid")

    #---Validate_No Identitas_---#
    add_error(validate_not_blank(df['No Identitas']), "No Identitas kosong")
    add_error(validate_no_nik(df['No Identitas']), "No Identitas tidak valid (harus 16 digit)")
    add_error(validate_numeric_only(df['No Identitas']), "No Identitas harus numerik")

    #---Validate_Nama Sesuai Identitas_---#
    add_error(validate_not_blank(df['Nama Sesuai Identitas']), "Nama Sesuai Identitas kosong")
    add_error(validate_special_characters(df['Nama Sesuai Identitas']), "Nama Sesuai Identitas mengandung karakter khusus")

    #---Validate_Nama Lengkap_---#
    add_error(validate_not_blank(df['Nama Lengkap']), "Nama Lengkap kosong")
    add_error(validate_special_characters(df['Nama Lengkap']), "Nama Lengkap mengandung karakter khusus")

    #---Validate_Kode Status Pendidikan_---#
    add_error(validate_not_blank(df['Kode Status Pendidikan']), "Kode Status Pendidikan kosong")
    add_error(valdiate_kode_status_pendidikan(df['Kode Status Pendidikan']), "Kode Status Pendidikan tidak valid")

    #---Validate_Jenis Kelamin_---#
    add_error(validate_not_blank(df['Jenis Kelamin']), "Jenis Kelamin kosong")
    add_error(validate_jenis_kelamin(df['Jenis Kelamin']), "Jenis Kelamin tidak valid")

    #---Validate_Tempat Lahir_---#
    add_error(validate_not_blank(df['Tempat Lahir']), "Tempat Lahir kosong")
    add_error(validate_special_characters(df['Tempat Lahir']), "Tempat Lahir mengandung karakter khusus")

    #---Validate_Tanggal Lahir_---#
    add_error(validate_not_blank(df['Tanggal Lahir']), "Tanggal Lahir kosong")
    add_error(validate_tanggal_format(df['Tanggal Lahir']), "Tanggal Lahir tidak sesuai format YYYY-MM-DD")

    #---Validate_No Pokok Wajib Pajak_---#
    is_npwp_not_blank = ~validate_not_blank(df['No Pokok Wajib Pajak'])
    add_error(is_npwp_not_blank & (~df['No Pokok Wajib Pajak'].astype(str).str.match(r'^\d{15}$')), "No Pokok Wajib Pajak tidak valid (harus 15 digit)")
    add_error(is_npwp_not_blank & (~df['No Pokok Wajib Pajak'].astype(str).str.isdigit()), "No Pokok Wajib Pajak harus numerik")

    #---Validate_Alamat_---#
    add_error(validate_not_blank(df['Alamat']), "Alamat kosong")
    add_error(validate_special_characters(df['Alamat']), "Alamat mengandung karakter khusus")
    add_error(df['Alamat'].astype(str).str.len() > 100, "Alamat melebihi 100 karakter")
    add_error(validate_minimal_karakter(df['Alamat'], 2), "Alamat minimal 3 karakter")

    #---Validate_Kelurahan_---#
    add_error(validate_not_blank(df['Kelurahan']), "Kelurahan kosong")
    add_error(validate_special_characters(df['Kelurahan']), "Kelurahan mengandung karakter khusus")
    add_error(df['Kelurahan'].astype(str).str.len() > 50, "Kelurahan melebihi 50 karakter")
    add_error(validate_minimal_karakter(df['Kelurahan'], 2), "Kelurahan minimal 3 karakter")
    add_error(~validate_numeric_only(df['Kelurahan']), "Kelurahan tidak boleh numeric only")
    
    #---Validate_Kecamatan_---#
    add_error(validate_not_blank(df['Kecamatan']), "Kecamatan kosong")
    add_error(validate_special_characters(df['Kecamatan']), "Kecamatan mengandung karakter khusus")
    add_error(df['Kecamatan'].astype(str).str.len() > 50, "Kecamatan melebihi 50 karakter")
    add_error(validate_minimal_karakter(df['Kecamatan'], 2), "Kecamatan minimal 3 karakter")
    add_error(~validate_numeric_only(df['Kecamatan']), "Kecamatan tidak boleh numeric only")

    #---Validate_Kode Kabupaten atau Kota_---#
    add_error(validate_not_blank(df['Kode Kabupaten atau Kota']), "Kode Kabupaten atau Kota kosong")
    add_error(validate_kode_kabupaten(df['Kode Kabupaten atau Kota']), "Kode Kabupaten atau Kota tidak valid")

    #---Validate_Kode Pos_---#
    add_error(validate_not_blank(df['Kode Pos']), "Kode Pos kosong")
    add_error(validate_kode_pos(df['Kode Pos']), "Kode Pos tidak valid (harus 5 digit)")

    #---Validate_No Telepon_---#
    add_error(validate_not_blank(df['No Telepon']), "No Telepon kosong")
    add_error(~df['No Telepon'].astype(str).str.match(r'^\d+$'), "No Telepon harus numerik")

    #---Validate_No Telepon Seluler_---#
    add_error(validate_not_blank(df['No Telepon Seluler']), "No Telepon Seluler kosong")
    add_error(~df['No Telepon Seluler'].astype(str).str.match(r'^\d+$'), "No Telepon Seluler harus numerik")

    #---Validate_Alamat E-mail_---#
    is_email_not_blank = ~validate_not_blank(df['Alamat E-mail'])
    add_error(is_email_not_blank   & (~df['Alamat E-mail'].astype(str).str.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')), "Alamat E-mail tidak valid format")

    #---Validate_Kode Negara Domisili_---#
    add_error(validate_not_blank(df['Kode Negara Domisili']), "Kode Negara Domisili kosong")
    add_error(~df['Kode Negara Domisili'].isin(['ID', 'AS', 'AU', 'BN', 'CN', 'FR', 'DE', 'HK', 'IN', 'JP', 'KR', 'MY', 'NZ', 'PH', 'SG', 'TH', 'GB']), "Kode Negara Domisili tidak valid")

    #---Validate_Kode Pekerjaan_---#
    add_error(validate_not_blank(df['Kode Pekerjaan']), "Kode Pekerjaan kosong")
    add_error(validate_kode_pekerjaan(df['Kode Pekerjaan']), "Kode Pekerjaan tidak valid")

    #---Validate_Tempat Bekerja_---#
    add_error(validate_not_blank(df['Tempat Bekerja']), "Tempat Bekerja kosong")
    add_error(validate_special_characters(df['Tempat Bekerja']), "Tempat Bekerja mengandung karakter khusus")
    add_error(df['Tempat Bekerja'].astype(str).str.len() > 50, "Tempat Bekerja melebihi 50 karakter")

    #---Validate_Kode Bidang Usaha Tempat Bekerja_---#
    add_error(validate_not_blank(df['Kode Bidang Usaha Tempat Bekerja']), "Kode Bidang Usaha Tempat Bekerja kosong")
    add_error(validate_sektor_ekonomi(df['Kode Bidang Usaha Tempat Bekerja']), "Kode Bidang Usaha Tempat Bekerja tidak valid")
    add_error(validate_exact_length(df['Kode Bidang Usaha Tempat Bekerja'], 6), "Kode Bidang Usaha Tempat Bekerja harus 6 digit")

    #---Validate_Alamat Tempat Bekerja_---#
    is_atb_blank = validate_not_blank(df['Alamat Tempat Bekerja'])
    add_error(is_atb_blank, "Alamat Tempat Bekerja kosong")
    add_error(~is_atb_blank & validate_special_characters(df['Alamat Tempat Bekerja']), "Alamat Tempat Bekerja mengandung karakter khusus")


    #---Validate_Penghasilan Kotor Per-Tahun_---#
    add_error(validate_not_blank(df['Penghasilan Kotor Per-Tahun']), "Penghasilan Kotor Per-Tahun kosong")
    add_error(~validate_numeric_only(df['Penghasilan Kotor Per-Tahun']), "Penghasilan Kotor Per-Tahun harus numerik")
    add_error(df['Penghasilan Kotor Per-Tahun'].astype(float) < 0, "Penghasilan Kotor Per-Tahun tidak boleh negatif")
    add_error(validate_maximal_karakter(df['Penghasilan Kotor Per-Tahun'], 12), "Penghasilan Kotor Per-Tahun melebihi 12 karakter")

    #---Validate_Kode Sumber Penghasilan_---#
    add_error(validate_not_blank(df['Kode Sumber Penghasilan']), "Kode Sumber Penghasilan kosong")
    add_error(validate_kode_sumber_penghasilan(df['Kode Sumber Penghasilan']), "Kode Sumber Penghasilan tidak valid")

    #---Validate_Jmlh Tanggungan_---#
    add_error(validate_not_blank(df['Jmlh Tanggungan']), "Jmlh Tanggungan kosong")
    add_error(~validate_numeric_only(df['Jmlh Tanggungan']), "Jmlh Tanggungan harus numerik")

    #---Validate_Kode Hub. dengan Pelapor_---#
    add_error(validate_not_blank(df['Kode Hub. dengan Pelapor']), "Kode Hub. dengan Pelapor kosong")
    add_error(validate_kode_hubungan_dengan_pelapor(df['Kode Hub. dengan Pelapor']), "Kode Hub. dengan Pelapor tidak valid")

    #---Validate_Kode Golongan Debitur_---#
    add_error(validate_not_blank(df['Kode Golongan Debitur']), "Kode Golongan Debitur kosong")
    add_error(validate_kode_golongan_debitur(df['Kode Golongan Debitur']), "Kode Golongan Debitur tidak valid")

    #---Validate_Status Perkawinan Debitur_---#
    add_error(validate_not_blank(df['Status Perkawinan Debitur']), "Status Perkawinan Debitur kosong")
    add_error(validate_kode_status_perkawinan(df['Status Perkawinan Debitur']), "Status Perkawinan Debitur tidak valid")

    #---Validate_No Identitas Pasangan_---#
    is_no_id_pasangan_not_blank = ~validate_not_blank(df['No Identitas Pasangan'])
    add_error(is_no_id_pasangan_not_blank & (~df['No Identitas Pasangan'].astype(str).str.match(r'^\d{16}$')), "No Identitas Pasangan tidak valid (harus 16 digit)")
    add_error(is_no_id_pasangan_not_blank & (~df['No Identitas Pasangan'].astype(str).str.isdigit()), "No Identitas Pasangan harus numerik")

    #---Validate_Nama Pasangan_---#
    is_nama_pasangan_not_blank = ~validate_not_blank(df['Nama Pasangan'])
    add_error(is_nama_pasangan_not_blank & validate_special_characters(df['Nama Pasangan']), "Nama Pasangan mengandung karakter khusus")

    #---Validate_Tanggal Lahir Pasangan_---#
    is_tgl_lahir_pasangan_not_blank = ~validate_not_blank(df['Tanggal Lahir Pasangan'])
    add_error(is_tgl_lahir_pasangan_not_blank & validate_tanggal_format(df['Tanggal Lahir Pasangan']), "Tanggal Lahir Pasangan tidak sesuai format YYYY-MM-DD")

    #---Validate_Perjanjian Pisah Harta_---#
    is_status_perkawinan_married = df['Status Perkawinan Debitur'] == '1'
    add_error(is_status_perkawinan_married & validate_not_blank(df['Perjanjian Pisah Harta']), "Perjanjian Pisah Harta kosong padahal Status Perkawinan Debitur menikah")
    add_error(is_status_perkawinan_married & validate_perjanjian_pisah_harta(df['Perjanjian Pisah Harta']), "Perjanjian Pisah Harta tidak valid")

    #---Validate_Melanggar BMPK BMPD BMPP_---#
    add_error(validate_not_blank(df['Melanggar BMPK BMPD BMPP']), "Melanggar BMPK BMPD BMPP kosong")
    add_error(validate_melanggar_bmpk_bmpd_bmpp(df['Melanggar BMPK BMPD BMPP']), "Melanggar BMPK BMPD BMPP tidak valid")

    #---Validate_Melampaui BMPK BMPD BMPP_---#
    add_error(validate_not_blank(df['Melampaui BMPK BMPD BMPP']), "Melampaui BMPK BMPD BMPP kosong")    
    add_error(validate_melampaui_bmpk_bmpd_bmpp(df['Melampaui BMPK BMPD BMPP']), "Melampaui BMPK BMPD BMPP tidak valid")

    #---Validate_Nama Gadis Ibu Kandung_---#
    add_error(validate_not_blank(df['Nama Gadis Ibu Kandung']), "Nama Gadis Ibu Kandung kosong")
    add_error(validate_special_characters(df['Nama Gadis Ibu Kandung']), "Nama Gadis Ibu Kandung mengandung karakter khusus")


    #---Validate_Kode Kantor Cabang_---#
    add_error(validate_not_blank(df['Kode Kantor Cabang']), "Kode Kantor Cabang kosong")

    #---Validate_Operasi Data_---#
    add_error(validate_not_blank(df['Operasi Data']), "Operasi Data kosong")
    add_error(validate_operasi_data(df['Operasi Data']), "Operasi Data tidak valid")


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