import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO

# Thiết lập trang rộng hơn cho giao diện
st.set_page_config(layout="wide")

@st.cache_data # Cache để tăng tốc độ khi tải lại mà file không đổi, nếu file không thay đổi
def load_excel(uploaded_file_obj):
    """
    Hàm tải và đọc một file Excel cụ thể, xử lý các định dạng .xls và .xlsx.
    """
    if uploaded_file_obj is not None:
        try:
            file_name = uploaded_file_obj.name
            if file_name.endswith('.xlsx'):
                return pd.read_excel(uploaded_file_obj, engine='openpyxl')
            elif file_name.endswith('.xls'):
                # Cố gắng đọc .xls, nếu lỗi thì thử lại với engine 'xlrd'
                try:
                    return pd.read_excel(uploaded_file_obj)
                except Exception as e_xls:
                    st.warning(f"Lỗi khi đọc file .xls {file_name} với engine mặc định: {e_xls}. Đang thử với xlrd...")
                    try:
                        return pd.read_excel(uploaded_file_obj, engine='xlrd')
                    except Exception as e_xlrd:
                        st.error(f"Không thể đọc file .xls {file_name} với xlrd: {e_xlrd}")
                        return None
            else: # Đối với các định dạng khác hoặc không có đuôi rõ ràng, thử đọc mặc định
                return pd.read_excel(uploaded_file_obj)
        except Exception as e:
            st.error(f"Lỗi khi đọc file {uploaded_file_obj.name}: {e}")
            return None
    return None

@st.cache_data # Cache để tăng tốc độ khi tải lại mà các file không đổi
def load_multiple_excel(uploaded_file_objs):
    """
    Hàm tải và đọc nhiều file Excel, sau đó ghép (concatenate) chúng lại thành một DataFrame duy nhất.
    """
    if uploaded_file_objs:
        df_list = []
        for file_obj in uploaded_file_objs:
            df = load_excel(file_obj)
            if df is not None:
                df_list.append(df)
        if df_list:
            try:
                return pd.concat(df_list, ignore_index=True)
            except Exception as e:
                st.error(f"Lỗi khi ghép các file Excel: {e}")
                return None
        return None # Trả về None nếu danh sách df_list rỗng
    return None # Trả về None nếu không có file nào được tải lên

def process_crm_data(
    df_crm4_raw, df_crm32_raw, df_muc_dich_data, df_code_tsbd_data,
    df_sol_data, df_giai_ngan_data, df_55_data, df_56_data, df_delay_data,
    chi_nhanh_filter, dia_ban_kt_filter
):
    """
    Hàm chính để xử lý toàn bộ dữ liệu CRM và tạo ra các bảng báo cáo.
    """
    # Tạo bản sao để tránh thay đổi dữ liệu gốc
    df_crm4 = df_crm4_raw.copy() if df_crm4_raw is not None else None
    df_crm32 = df_crm32_raw.copy() if df_crm32_raw is not None else None

    # Kiểm tra các file dữ liệu bắt buộc
    if df_crm4 is None or df_crm32 is None:
        st.error("Dữ liệu CRM4 hoặc CRM32 chưa được tải lên hoặc bị lỗi. Vui lòng kiểm tra lại.")
        return None, None, None, None, None, None, None, None, None, None, None # Trả về None cho tất cả kết quả

    st.info(f"Bắt đầu xử lý dữ liệu cho chi nhánh: '{chi_nhanh_filter}' và địa bàn kiểm toán: '{dia_ban_kt_filter}'")

    # Lọc dữ liệu CRM4 và CRM32 theo chi nhánh (BRCD/BRANCH_VAY)
    df_crm4_filtered = df_crm4[df_crm4['BRANCH_VAY'].astype(str).str.upper().str.contains(chi_nhanh_filter)].copy()
    df_crm32_filtered = df_crm32[df_crm32['BRCD'].astype(str).str.upper().str.contains(chi_nhanh_filter)].copy()

    st.write(f"📌 Số dòng CRM4 sau khi lọc theo chi nhánh '{chi_nhanh_filter}': **{len(df_crm4_filtered)}**")
    st.write(f"📌 Số dòng CRM32 sau khi lọc theo chi nhánh '{chi_nhanh_filter}': **{len(df_crm32_filtered)}**")

    # Xử lý thông tin Loại TSBĐ từ file CODE_LOAI TSBD
    if df_code_tsbd_data is None:
        st.error("File CODE_LOAI TSBD chưa được tải hoặc lỗi. Bỏ qua xử lý loại TSBĐ.")
        # Khởi tạo các DataFrame liên quan để tránh lỗi
        df_crm4_filtered['LOAI_TS'] = None
        df_crm4_filtered['GHI_CHU_TSBD'] = None
        pivot_merge = pd.DataFrame(columns=['CIF_KH_VAY', 'GIÁ TRỊ TS', 'DƯ NỢ'])
        pivot_final = pd.DataFrame(columns=['STT', 'CUSTTPCD', 'CIF_KH_VAY', 'TEN_KH_VAY', 'NHOM_NO'])
    else:
        df_code_tsbd = df_code_tsbd_data[['CODE CAP 2', 'CODE']].copy()
        df_code_tsbd.columns = ['CAP_2', 'LOAI_TS']
        df_tsbd_code = df_code_tsbd[['CAP_2', 'LOAI_TS']].drop_duplicates().copy()
        
        # Đảm bảo cột 'CAP_2' là string để merge
        df_crm4_filtered['CAP_2'] = df_crm4_filtered['CAP_2'].astype(str)
        df_tsbd_code['CAP_2'] = df_tsbd_code['CAP_2'].astype(str)

        df_crm4_filtered = df_crm4_filtered.merge(df_tsbd_code, how='left', on='CAP_2')
        df_crm4_filtered['LOAI_TS'] = df_crm4_filtered.apply(
            lambda row: 'Không TS' if pd.isna(row['CAP_2']) or str(row['CAP_2']).strip() == '' else row['LOAI_TS'],
            axis=1
        )
        df_crm4_filtered['GHI_CHU_TSBD'] = df_crm4_filtered.apply(
            lambda row: 'MỚI' if pd.notna(row['CAP_2']) and str(row['CAP_2']).strip() != '' and pd.isna(row['LOAI_TS']) else '',
            axis=1
        )

        df_vay_4 = df_crm4_filtered.copy()
        df_vay = df_vay_4[~df_vay_4['LOAI'].isin(['Bao lanh', 'LC'])].copy()

        if not df_vay.empty:
            pivot_ts = df_vay.pivot_table(
                index='CIF_KH_VAY', columns='LOAI_TS', values='TS_KW_VND',
                aggfunc='sum', fill_value=0
            ).add_suffix(' (Giá trị TS)').reset_index()
            pivot_no = df_vay.pivot_table(
                index='CIF_KH_VAY', columns='LOAI_TS', values='DU_NO_PHAN_BO_QUY_DOI',
                aggfunc='sum', fill_value=0
            ).reset_index()

            # Đảm bảo CIF_KH_VAY là string để merge
            pivot_no['CIF_KH_VAY'] = pivot_no['CIF_KH_VAY'].astype(str).str.strip()
            pivot_ts['CIF_KH_VAY'] = pivot_ts['CIF_KH_VAY'].astype(str).str.strip()

            pivot_merge = pivot_no.merge(pivot_ts, on='CIF_KH_VAY', how='left')
            ts_value_cols = [col for col in pivot_ts.columns if col.endswith(' (Giá trị TS)') and col != 'CIF_KH_VAY (Giá trị TS)']
            
            # Sử dụng .get() để tránh lỗi nếu cột không tồn tại sau merge
            pivot_merge['GIÁ TRỊ TS'] = pivot_merge.get(ts_value_cols, pd.DataFrame()).sum(axis=1) # Handle potential empty list
            
            du_no_cols = [col for col in pivot_no.columns if col != 'CIF_KH_VAY']
            pivot_merge['DƯ NỢ'] = pivot_merge.get(du_no_cols, pd.DataFrame()).sum(axis=1) # Handle potential empty list
        else:
            st.warning("Không có dữ liệu 'Cho vay' sau khi lọc theo chi nhánh. Bảng pivot TSBĐ có thể rỗng.")
            pivot_merge = pd.DataFrame(columns=['CIF_KH_VAY', 'GIÁ TRỊ TS', 'DƯ NỢ'])

        df_info = df_crm4_filtered[['CIF_KH_VAY', 'TEN_KH_VAY', 'CUSTTPCD', 'NHOM_NO']].drop_duplicates(subset='CIF_KH_VAY').copy()
        
        # Đảm bảo CIF_KH_VAY là string để merge
        df_info['CIF_KH_VAY'] = df_info['CIF_KH_VAY'].astype(str).str.strip()
        pivot_merge['CIF_KH_VAY'] = pivot_merge['CIF_KH_VAY'].astype(str).str.strip()

        pivot_final = df_info.merge(pivot_merge, on='CIF_KH_VAY', how='left')
        pivot_final = pivot_final.reset_index().rename(columns={'index': 'STT'})
        pivot_final['STT'] += 1
        
        # Đảm bảo các cột tồn tại trước khi sắp xếp thứ tự
        du_no_pivot_cols = [col for col in pivot_merge.columns if col not in ['CIF_KH_VAY', 'GIÁ TRỊ TS', 'DƯ NỢ'] and '(Giá trị TS)' not in col]
        ts_value_pivot_cols = [col for col in pivot_merge.columns if '(Giá trị TS)' in col and col != 'CIF_KH_VAY']
        
        cols_order = ['STT', 'CUSTTPCD', 'CIF_KH_VAY', 'TEN_KH_VAY', 'NHOM_NO'] + \
                     sorted(du_no_pivot_cols) + sorted(ts_value_pivot_cols) + ['DƯ NỢ', 'GIÁ TRỊ TS']
        
        # Lọc ra chỉ các cột thực sự tồn tại trong pivot_final
        cols_order_existing = [col for col in cols_order if col in pivot_final.columns]
        pivot_final = pivot_final[cols_order_existing]
        
    # Xử lý thông tin về hạn mức/cơ cấu nợ từ CRM32
    df_crm32_filtered['MA_PHE_DUYET'] = df_crm32_filtered['CAP_PHE_DUYET'].astype(str).str.split('-').str[0].str.strip().str.zfill(2)
    ma_cap_c = [f"{i:02d}" for i in range(1, 8)] + [f"{i:02d}" for i in range(28, 32)]
    list_cif_cap_c = df_crm32_filtered[df_crm32_filtered['MA_PHE_DUYET'].isin(ma_cap_c)]['CUSTSEQLN'].unique().astype(str)
    list_co_cau = ['ACOV1', 'ACOV3', 'ATT01', 'ATT02', 'ATT03', 'ATT04',
                   'BCOV1', 'BCOV2', 'BTT01', 'BTT02', 'BTT03',
                   'CCOV2', 'CCOV3', 'CTT03', 'RCOV3', 'RTT03']
    cif_co_cau = df_crm32_filtered[df_crm32_filtered['SCHEME_CODE'].isin(list_co_cau)]['CUSTSEQLN'].unique().astype(str)

    # Xử lý thông tin Mục đích sử dụng vốn từ CODE_MDSDV4
    if df_muc_dich_data is None:
        st.error("File CODE_MDSDV4 chưa được tải hoặc lỗi. Bỏ qua xử lý mục đích sử dụng vốn.")
        pivot_mucdich = pd.DataFrame(columns=['CUSTSEQLN', 'DƯ NỢ CRM32'])
    else:
        df_muc_dich_vay_src = df_muc_dich_data[['CODE_MDSDV4', 'GROUP']].copy()
        df_muc_dich_vay_src.columns = ['MUC_DICH_VAY_CAP_4', 'MUC DICH']
        df_muc_dich_map = df_muc_dich_vay_src[['MUC_DICH_VAY_CAP_4', 'MUC DICH']].drop_duplicates().copy()
        
        # Đảm bảo cột 'MUC_DICH_VAY_CAP_4' là string để merge
        df_crm32_filtered['MUC_DICH_VAY_CAP_4'] = df_crm32_filtered['MUC_DICH_VAY_CAP_4'].astype(str)
        df_muc_dich_map['MUC_DICH_VAY_CAP_4'] = df_muc_dich_map['MUC_DICH_VAY_CAP_4'].astype(str)

        df_crm32_filtered = df_crm32_filtered.merge(df_muc_dich_map, how='left', on='MUC_DICH_VAY_CAP_4')
        df_crm32_filtered['MUC DICH'] = df_crm32_filtered['MUC DICH'].fillna('(blank)')
        df_crm32_filtered['GHI_CHU_MUC_DICH'] = df_crm32_filtered.apply(
            lambda row: 'MỚI' if pd.notna(row['MUC_DICH_VAY_CAP_4']) and str(row['MUC_DICH_VAY_CAP_4']).strip() != '' and pd.isna(row['MUC DICH']) and row['MUC DICH'] == '(blank)' else '',
            axis=1
        )
        pivot_mucdich = df_crm32_filtered.pivot_table(
            index='CUSTSEQLN', columns='MUC DICH', values='DU_NO_QUY_DOI',
            aggfunc='sum', fill_value=0
        ).reset_index()
        muc_dich_cols_for_sum = [col for col in pivot_mucdich.columns if col != 'CUSTSEQLN']
        pivot_mucdich['DƯ NỢ CRM32'] = pivot_mucdich.get(muc_dich_cols_for_sum, pd.DataFrame()).sum(axis=1)
        pivot_mucdich['CUSTSEQLN'] = pivot_mucdich['CUSTSEQLN'].astype(str).str.strip()
        pivot_final_CRM32 = pivot_mucdich.rename(columns={'CUSTSEQLN': 'CIF_KH_VAY'})
    
    # Đảm bảo CIF_KH_VAY là string để merge cho pivot_full
    pivot_final['CIF_KH_VAY'] = pivot_final['CIF_KH_VAY'].astype(str).str.strip()
    pivot_final_CRM32['CIF_KH_VAY'] = pivot_final_CRM32['CIF_KH_VAY'].astype(str).str.strip()
    
    pivot_full = pivot_final.merge(pivot_final_CRM32, on='CIF_KH_VAY', how='left')
    pivot_full.fillna(0, inplace=True) # Điền 0 cho các giá trị NaN sau merge

    pivot_full['LECH'] = pivot_full['DƯ NỢ'] - pivot_full.get('DƯ NỢ CRM32', 0)
    pivot_full['LECH'] = pivot_full['LECH'].fillna(0) # Đảm bảo LECH không có NaN

    # Xử lý các khoản dư nợ blank (không có mục đích sử dụng vốn)
    cif_lech = pivot_full[pivot_full['LECH'] != 0]['CIF_KH_VAY'].unique()
    df_crm4_blank = df_crm4_filtered[~df_crm4_filtered['LOAI'].isin(['Cho vay', 'Bao lanh', 'LC'])].copy()
    df_crm4_blank['CIF_KH_VAY'] = df_crm4_blank['CIF_KH_VAY'].astype(str).str.strip()

    if not df_crm4_blank.empty and cif_lech.size > 0 :
        du_no_bosung = (
            df_crm4_blank[df_crm4_blank['CIF_KH_VAY'].isin(cif_lech)]
            .groupby('CIF_KH_VAY', as_index=False)['DU_NO_PHAN_BO_QUY_DOI']
            .sum().rename(columns={'DU_NO_PHAN_BO_QUY_DOI': '(blank)'})
        )
        du_no_bosung['CIF_KH_VAY'] = du_no_bosung['CIF_KH_VAY'].astype(str).str.strip()
        pivot_full = pivot_full.merge(du_no_bosung, on='CIF_KH_VAY', how='left')
        pivot_full['(blank)'] = pivot_full['(blank)'].fillna(0)
        pivot_full['DƯ NỢ CRM32'] = pivot_full.get('DƯ NỢ CRM32', 0) + pivot_full['(blank)']
    else:
        pivot_full['(blank)'] = 0 # Đảm bảo cột (blank) luôn tồn tại
    
    # Đảm bảo thứ tự cột nếu '(blank)' có
    cols = list(pivot_full.columns)
    if '(blank)' in cols and 'DƯ NỢ CRM32' in cols:
        cols.insert(cols.index('DƯ NỢ CRM32'), cols.pop(cols.index('(blank)')))
        pivot_full = pivot_full[cols]
    
    pivot_full['LECH'] = pivot_full['DƯ NỢ'] - pivot_full.get('DƯ NỢ CRM32',0)

    # Thêm các cột về nhóm nợ, phê duyệt cấp C, nợ cơ cấu
    pivot_full['NHOM_NO'] = pivot_full['NHOM_NO'].astype(str)
    pivot_full['Nợ nhóm 2'] = pivot_full['NHOM_NO'].apply(lambda x: 'x' if x.strip() == '2' or x.strip() == '2.0' else '')
    pivot_full['Nợ xấu'] = pivot_full['NHOM_NO'].apply(lambda x: 'x' if x.strip() in ['3', '4', '5', '3.0', '4.0', '5.0'] else '')
    pivot_full['CIF_KH_VAY'] = pivot_full['CIF_KH_VAY'].astype(str).str.strip()
    
    list_cif_cap_c_str = [str(c).strip() for c in list_cif_cap_c]
    cif_co_cau_str = [str(c).strip() for c in cif_co_cau]
    
    pivot_full['Chuyên gia PD cấp C duyệt'] = pivot_full['CIF_KH_VAY'].apply(lambda x: 'x' if x in list_cif_cap_c_str else '')
    pivot_full['NỢ CƠ_CẤU'] = pivot_full['CIF_KH_VAY'].apply(lambda x: 'x' if x in cif_co_cau_str else '')

    # Xử lý Dư nợ Bảo lãnh và LC
    df_baolanh = df_crm4_filtered[df_crm4_filtered['LOAI'] == 'Bao lanh'].copy()
    df_lc = df_crm4_filtered[df_crm4_filtered['LOAI'] == 'LC'].copy()
    df_baolanh['CIF_KH_VAY'] = df_baolanh['CIF_KH_VAY'].astype(str).str.strip()
    df_lc['CIF_KH_VAY'] = df_lc['CIF_KH_VAY'].astype(str).str.strip()
    
    df_baolanh_sum = df_baolanh.groupby('CIF_KH_VAY', as_index=False)['DU_NO_PHAN_BO_QUY_DOI'].sum().rename(columns={'DU_NO_PHAN_BO_QUY_DOI': 'DƯ_NỢ_BẢO_LÃNH'})
    df_lc_sum = df_lc.groupby('CIF_KH_VAY', as_index=False)['DU_NO_PHAN_BO_QUY_DOI'].sum().rename(columns={'DU_NO_PHAN_BO_QUY_DOI': 'DƯ_NỢ_LC'})
    
    pivot_full = pivot_full.merge(df_baolanh_sum, on='CIF_KH_VAY', how='left')
    pivot_full = pivot_full.merge(df_lc_sum, on='CIF_KH_VAY', how='left')
    pivot_full['DƯ_NỢ_BẢO_LÃNH'] = pivot_full['DƯ_NỢ_BẢO_LÃNH'].fillna(0)
    pivot_full['DƯ_NỢ_LC'] = pivot_full['DƯ_NỢ_LC'].fillna(0)

    # Xử lý giải ngân tiền mặt
    if df_giai_ngan_data is not None:
        df_giai_ngan = df_giai_ngan_data.copy()
        # Đảm bảo các cột là string để so sánh
        df_crm32_filtered['KHE_UOC'] = df_crm32_filtered['KHE_UOC'].astype(str).str.strip()
        df_crm32_filtered['CUSTSEQLN'] = df_crm32_filtered['CUSTSEQLN'].astype(str).str.strip()
        df_giai_ngan['FORACID'] = df_giai_ngan['FORACID'].astype(str).str.strip()
        
        df_match_gn = df_crm32_filtered[df_crm32_filtered['KHE_UOC'].isin(df_giai_ngan['FORACID'])].copy()
        ds_cif_tien_mat = df_match_gn['CUSTSEQLN'].unique()
        pivot_full['GIẢI_NGÂN_TIEN_MAT'] = pivot_full['CIF_KH_VAY'].isin(ds_cif_tien_mat).map({True: 'x', False: ''})
    else:
        pivot_full['GIẢI_NGÂN_TIEN_MAT'] = ''
        st.warning("Không có dữ liệu giải ngân tiền mặt để xử lý.")

    # Xử lý TSBĐ cầm cố tại TCTD khác
    df_crm4_filtered['CAP_2'] = df_crm4_filtered['CAP_2'].astype(str)
    df_cc_tctd = df_crm4_filtered[df_crm4_filtered['CAP_2'].str.contains('TCTD', case=False, na=False)].copy()
    df_cc_tctd['CIF_KH_VAY'] = df_cc_tctd['CIF_KH_VAY'].astype(str).str.strip()
    df_cc_flag = df_cc_tctd[['CIF_KH_VAY']].drop_duplicates().copy()
    df_cc_flag['Cầm cố tại TCTD khác'] = 'x'
    pivot_full = pivot_full.merge(df_cc_flag, on='CIF_KH_VAY', how='left')
    pivot_full['Cầm cố tại TCTD khác'] = pivot_full['Cầm cố tại TCTD khác'].fillna('')

    # Xử lý Top 10 dư nợ KHCN/KHDN
    pivot_full['DƯ NỢ'] = pd.to_numeric(pivot_full['DƯ NỢ'], errors='coerce').fillna(0)
    pivot_full['CUSTTPCD'] = pivot_full['CUSTTPCD'].astype(str).str.strip().str.lower()
    
    top10_khcn_cif = pivot_full[pivot_full['CUSTTPCD'] == 'ca nhan'].nlargest(10, 'DƯ NỢ')['CIF_KH_VAY'].astype(str).str.strip().values
    pivot_full['Top 10 dư nợ KHCN'] = pivot_full['CIF_KH_VAY'].apply(lambda x: 'x' if x in top10_khcn_cif else '')
    
    top10_khdn_cif = pivot_full[pivot_full['CUSTTPCD'] == 'doanh nghiep'].nlargest(10, 'DƯ NỢ')['CIF_KH_VAY'].astype(str).str.strip().values
    pivot_full['Top 10 dư nợ KHDN'] = pivot_full['CIF_KH_VAY'].apply(lambda x: 'x' if x in top10_khdn_cif else '')

    # Xử lý TSBĐ quá hạn định giá
    ngay_danh_gia_tsbd = pd.to_datetime("2025-03-31") # Có thể biến thành input từ người dùng
    loai_ts_r34 = ['BĐS', 'MMTB', 'PTVT']
    df_crm4_for_tsbd = df_crm4_filtered.copy() # df_crm4_for_tsbd được định nghĩa ở đây
    
    df_crm4_for_tsbd['LOAI_TS'] = df_crm4_for_tsbd['LOAI_TS'].astype(str)
    mask_r34 = df_crm4_for_tsbd['LOAI_TS'].isin(loai_ts_r34)
    df_crm4_for_tsbd['VALUATION_DATE'] = pd.to_datetime(df_crm4_for_tsbd['VALUATION_DATE'], errors='coerce')
    
    df_crm4_for_tsbd.loc[mask_r34, 'SO_NGAY_QUA_HAN'] = (
        (ngay_danh_gia_tsbd - df_crm4_for_tsbd.loc[mask_r34, 'VALUATION_DATE']).dt.days - 365
    )
    
    cif_quahan_series = df_crm4_for_tsbd[
        (df_crm4_for_tsbd['SO_NGAY_QUA_HAN'].notna()) & (df_crm4_for_tsbd['SO_NGAY_QUA_HAN'] > 30)
    ]['CIF_KH_VAY']
    
    cif_quahan = cif_quahan_series.astype(str).str.strip().unique()
    pivot_full['KH có TSBĐ quá hạn định giá'] = pivot_full['CIF_KH_VAY'].apply(
        lambda x: 'x' if x in cif_quahan else ''
    )

    # --- Bắt đầu phần xử lý TSBĐ khác địa bàn (đã điều chỉnh) ---
    df_bds_matched = pd.DataFrame() # Khởi tạo rỗng để tránh lỗi nếu điều kiện không thỏa
    if df_sol_data is not None and dia_ban_kt_filter:
        df_sol = df_sol_data.copy()
        # Đảm bảo các cột liên quan là string để so sánh và xử lý
        df_crm4_filtered['SECU_SRL_NUM'] = df_crm4_filtered['SECU_SRL_NUM'].astype(str).str.strip()
        df_sol['C01'] = df_sol['C01'].astype(str).str.strip()
        df_sol['C02'] = df_sol['C02'].astype(str).str.strip()
        df_sol['C19'] = df_sol['C19'].astype(str) # Cột địa chỉ

        ds_secu = df_crm4_filtered['SECU_SRL_NUM'].dropna().unique()
        df_17_filtered = df_sol[df_sol['C01'].isin(ds_secu)].copy()

        df_bds = df_17_filtered[df_17_filtered['C02'].str.strip() == 'Bat dong san'].copy()
        
        # Đảm bảo df_bds_matched được lọc đúng với SECU_SRL_NUM của df_crm4_filtered
        df_bds_matched = df_bds[df_bds['C01'].isin(df_crm4_filtered['SECU_SRL_NUM'])].copy()

        def extract_tinh_thanh(diachi):
            if pd.isna(diachi): return ''
            parts = str(diachi).split(',') # Chuyển sang string trước khi split
            return parts[-1].strip().lower() if parts else ''

        df_bds_matched['TINH_TP_TSBD'] = df_bds_matched['C19'].apply(extract_tinh_thanh)
        df_bds_matched['CANH_BAO_TS_KHAC_DIABAN'] = df_bds_matched['TINH_TP_TSBD'].apply(
            lambda x: 'x' if x and x != dia_ban_kt_filter else '' # Sử dụng dia_ban_kt_filter trực tiếp
        )

        ma_ts_canh_bao = df_bds_matched[df_bds_matched['CANH_BAO_TS_KHAC_DIABAN'] == 'x']['C01'].unique()
        
        # Lấy CIF từ df_crm4_filtered để đảm bảo đã lọc theo chi nhánh
        cif_canh_bao_series = df_crm4_filtered[df_crm4_filtered['SECU_SRL_NUM'].isin(ma_ts_canh_bao)]['CIF_KH_VAY']
        cif_canh_bao = cif_canh_bao_series.astype(str).str.strip().dropna().unique()

        pivot_full['KH có TSBĐ khác địa bàn'] = pivot_full['CIF_KH_VAY'].apply(
            lambda x: 'x' if x in cif_canh_bao else ''
        )
    else:
        pivot_full['KH có TSBĐ khác địa bàn'] = ''
        if df_sol_data is None: st.warning("Không có dữ liệu Mục 17 (df_sol) để xử lý TSBĐ khác địa bàn. Hãy tải file lên.")
        if not dia_ban_kt_filter: st.warning("Chưa nhập địa bàn kiểm toán để xử lý TSBĐ khác địa bàn. Hãy nhập thông tin.")
    # --- Kết thúc phần xử lý TSBĐ khác địa bàn ---

    # Xử lý các giao dịch giải ngân và tất toán (Mục 55 & 56)
    df_gop = pd.DataFrame()
    df_count = pd.DataFrame()
    if df_55_data is not None and df_56_data is not None:
        df_tt_raw = df_55_data.copy()
        df_gn_raw = df_56_data.copy()
        
        # Đảm bảo các cột tồn tại trước khi chọn
        cols_tt = ['CUSTSEQLN', 'NMLOC', 'KHE_UOC', 'SOTIENGIAINGAN', 'NGAYGN', 'NGAYDH', 'NGAY_TT', 'LOAITIEN']
        cols_gn = ['CIF', 'TEN_KHACH_HANG', 'KHE_UOC', 'SO_TIEN_GIAI_NGAN_VND', 'NGAY_GIAI_NGAN', 'NGAY_DAO_HAN', 'LOAI_TIEN_HD']
        
        df_tt = df_tt_raw[[col for col in cols_tt if col in df_tt_raw.columns]].copy()
        df_gn = df_gn_raw[[col for col in cols_gn if col in df_gn_raw.columns]].copy()

        df_tt.columns = ['CIF', 'TEN_KHACH_HANG', 'KHE_UOC', 'SO_TIEN_GIAI_NGAN_VND', 'NGAY_GIAI_NGAN', 'NGAY_DAO_HAN', 'NGAY_TT', 'LOAI_TIEN_HD']
        df_tt['GIAI_NGAN_TT'] = 'Tất toán'
        df_tt['NGAY'] = pd.to_datetime(df_tt['NGAY_TT'], errors='coerce')
        df_tt['CIF'] = df_tt['CIF'].astype(str).str.strip()
        
        df_gn['GIAI_NGAN_TT'] = 'Giải ngân'
        df_gn['NGAY_GIAI_NGAN'] = pd.to_datetime(df_gn['NGAY_GIAI_NGAN'], format='%Y%m%d', errors='coerce')
        df_gn['NGAY_DAO_HAN'] = pd.to_datetime(df_gn['NGAY_DAO_HAN'], format='%Y%m%d', errors='coerce')
        df_gn['NGAY'] = df_gn['NGAY_GIAI_NGAN']
        df_gn['CIF'] = df_gn['CIF'].astype(str).str.strip()
        
        df_gop = pd.concat([df_tt, df_gn], ignore_index=True)
        df_gop = df_gop[df_gop['NGAY'].notna()].copy()
        df_gop = df_gop.sort_values(by=['CIF', 'NGAY', 'GIAI_NGAN_TT']).copy()
        
        df_count = df_gop.groupby(['CIF', 'NGAY', 'GIAI_NGAN_TT']).size().unstack(fill_value=0).reset_index()
        if 'Giải ngân' not in df_count.columns: df_count['Giải ngân'] = 0
        if 'Tất toán' not in df_count.columns: df_count['Tất toán'] = 0
        df_count['CO_CA_GN_VA_TT'] = ((df_count['Giải ngân'] > 0) & (df_count['Tất toán'] > 0)).astype(int)
        
        ds_ca_gn_tt_series = df_count[df_count['CO_CA_GN_VA_TT'] == 1]['CIF']
        ds_ca_gn_tt = ds_ca_gn_tt_series.astype(str).str.strip().unique()
        
        pivot_full['KH có cả GNG và TT trong 1 ngày'] = pivot_full['CIF_KH_VAY'].apply(
            lambda x: 'x' if x in ds_ca_gn_tt else ''
        )
    else:
        pivot_full['KH có cả GNG và TT trong 1 ngày'] = ''
        st.warning("Không có dữ liệu Mục 55 hoặc Mục 56 để xử lý giao dịch giải ngân/tất toán trong cùng ngày.")

    # Xử lý chậm trả (Mục 57)
    df_delay_processed = pd.DataFrame() # Khởi tạo rỗng
    if df_delay_data is not None:
        df_delay = df_delay_data.copy()
        # Đảm bảo tên cột CIF thống nhất là 'CIF_ID'
        if 'CIF_ID' not in df_delay.columns and 'CUSTSEQLN' in df_delay.columns:
             df_delay.rename(columns={'CUSTSEQLN': 'CIF_ID'}, inplace=True)
        
        if 'CIF_ID' in df_delay.columns:
            df_delay['CIF_ID'] = df_delay['CIF_ID'].astype(str).str.strip()
            df_delay['NGAY_DEN_HAN_TT'] = pd.to_datetime(df_delay['NGAY_DEN_HAN_TT'], errors='coerce')
            df_delay['NGAY_THANH_TOAN'] = pd.to_datetime(df_delay['NGAY_THANH_TOAN'], errors='coerce')
            
            ngay_danh_gia_cham_tra = pd.to_datetime("2025-03-31") # Có thể biến thành input
            
            df_delay['NGAY_THANH_TOAN_FILL'] = df_delay['NGAY_THANH_TOAN'].fillna(ngay_danh_gia_cham_tra)
            df_delay['SO_NGAY_CHAM_TRA'] = (df_delay['NGAY_THANH_TOAN_FILL'] - df_delay['NGAY_DEN_HAN_TT']).dt.days
            
            mask_period = df_delay['NGAY_DEN_HAN_TT'].dt.year.between(2023, 2025)
            df_delay = df_delay[mask_period & df_delay['NGAY_DEN_HAN_TT'].notna()].copy()
            
            pivot_full_temp_for_delay = pivot_full[['CIF_KH_VAY', 'DƯ NỢ', 'NHOM_NO']].rename(columns={'CIF_KH_VAY': 'CIF_ID'}).copy()
            pivot_full_temp_for_delay['CIF_ID'] = pivot_full_temp_for_delay['CIF_ID'].astype(str).str.strip()
            
            df_delay = df_delay.merge(pivot_full_temp_for_delay, on='CIF_ID', how='left')
            df_delay['NHOM_NO'] = pd.to_numeric(df_delay['NHOM_NO'], errors='coerce')
            df_delay = df_delay[df_delay['NHOM_NO'] == 1.0].copy() # Chỉ xét nợ nhóm 1
            
            def cap_cham_tra(days):
                if pd.isna(days): return None
                if days >= 10: return '>=10'
                if days >= 4: return '4-9'
                if days > 0: return '<4'
                return None
            
            df_delay['CAP_CHAM_TRA'] = df_delay['SO_NGAY_CHAM_TRA'].apply(cap_cham_tra)
            df_delay['NGAY_DEN_HAN_TT_DATE'] = df_delay['NGAY_DEN_HAN_TT'].dt.date
            
            df_delay.sort_values(['CIF_ID', 'NGAY_DEN_HAN_TT_DATE', 'CAP_CHAM_TRA'],
                                 key=lambda s: s.map({'>=10':0, '4-9':1, '<4':2, None: 3}) if s.name == 'CAP_CHAM_TRA' else s,
                                 inplace=True, na_position='last')
            
            df_unique_delay = df_delay.drop_duplicates(subset=['CIF_ID', 'NGAY_DEN_HAN_TT_DATE'], keep='first').copy()
            df_dem_delay = df_unique_delay.groupby(['CIF_ID', 'CAP_CHAM_TRA']).size().unstack(fill_value=0)
            
            df_dem_delay['KH Phát sinh chậm trả > 10 ngày'] = np.where(df_dem_delay.get('>=10', 0) > 0, 'x', '')
            df_dem_delay['KH Phát sinh chậm trả 4-9 ngày'] = np.where(
                (df_dem_delay.get('>=10', 0) == 0) & (df_dem_delay.get('4-9', 0) > 0), 'x', ''
            )
            df_dem_delay.index = df_dem_delay.index.astype(str) # Đảm bảo index là string để merge
            pivot_full['CIF_KH_VAY'] = pivot_full['CIF_KH_VAY'].astype(str).str.strip()

            pivot_full = pivot_full.merge(
                df_dem_delay[['KH Phát sinh chậm trả > 10 ngày', 'KH Phát sinh chậm trả 4-9 ngày']],
                left_on='CIF_KH_VAY', right_index=True, how='left'
            )
            pivot_full[['KH Phát sinh chậm trả > 10 ngày', 'KH Phát sinh chậm trả 4-9 ngày']] = \
                pivot_full[['KH Phát sinh chậm trả > 10 ngày', 'KH Phát sinh chậm trả 4-9 ngày']].fillna('')
            df_delay_processed = df_delay # Lưu lại df đã xử lý để xuất nếu cần
        else:
            st.warning("Cột 'CIF_ID' hoặc 'CUSTSEQLN' không tìm thấy trong dữ liệu Mục 57 (chậm trả). Bỏ qua xử lý chậm trả.")
            pivot_full['KH Phát sinh chậm trả > 10 ngày'] = ''
            pivot_full['KH Phát sinh chậm trả 4-9 ngày'] = ''
    else:
        pivot_full['KH Phát sinh chậm trả > 10 ngày'] = ''
        pivot_full['KH Phát sinh chậm trả 4-9 ngày'] = ''
        st.warning("Không có dữ liệu Mục 57 (chậm trả) để xử lý. Hãy tải file lên.")

    # --- Debugging: Kiểm tra df_bds_matched trước khi trả về ---
    st.subheader("⚙️ Debug: Thông tin DataFrame TSBĐ khác địa bàn (df_bds_matched_res)")
    if df_bds_matched is not None:
        st.write(f"Shape của df_bds_matched: **{df_bds_matched.shape}**")
        if not df_bds_matched.empty:
            st.dataframe(df_bds_matched.head())
        else:
            st.info("⚠️ **df_bds_matched rỗng** sau khi xử lý. Các sheet liên quan sẽ không được tạo.")
    else:
        st.warning("❌ **df_bds_matched là None**. Có lỗi xảy ra hoặc file Mục 17 chưa được tải.")
    # --- Kết thúc Debugging ---

    # Trả về tất cả các DataFrame cần thiết cho việc xuất file
    return (pivot_full, df_crm4_filtered, pivot_final, pivot_merge,
            df_crm32_filtered, pivot_mucdich, df_delay_processed, df_gop, df_count, df_bds_matched,
            df_crm4_for_tsbd,cif_canh_bao_series,cif_canh_bao)


# --- Giao diện người dùng Streamlit ---
st.title("📊 Ứng dụng Xử lý Dữ liệu CRM và Tạo Báo cáo Kiểm toán")
st.markdown("---")

with st.sidebar:
    st.header("📂 Tải lên các file Excel")
    st.markdown("Vui lòng tải lên các file dữ liệu cần thiết:")
    
    uploaded_crm4_files = st.file_uploader("1. Các file CRM4 (Du_no_theo_tai_san_dam_bao_ALL.xlsx/xls)", type=["xls", "xlsx"], accept_multiple_files=True, key="crm4_uploader")
    uploaded_crm32_files = st.file_uploader("2. Các file CRM32 (RPT_CRM_32.xlsx/xls)", type=["xls", "xlsx"], accept_multiple_files=True, key="crm32_uploader")
    uploaded_muc_dich_file = st.file_uploader("3. File CODE_MDSDV4.xlsx", type="xlsx", key="m_dich_uploader")
    uploaded_code_tsbd_file = st.file_uploader("4. File CODE_LOAI TSBD.xlsx", type="xlsx", key="tsbd_uploader")
    uploaded_sol_file = st.file_uploader("5. File MUC 17.xlsx (Dữ liệu SOL)", type="xlsx", key="sol_uploader")
    uploaded_giai_ngan_file = st.file_uploader("6. File Giai_ngan_tien_mat_1_ty (hoặc tương tự).xlsx/xls", type=["xls","xlsx"], key="giai_ngan_uploader")
    uploaded_55_file = st.file_uploader("7. File Muc55 (Tất toán).xlsx", type="xlsx", key="muc55_uploader")
    uploaded_56_file = st.file_uploader("8. File Muc56 (Giải ngân).xlsx", type="xlsx", key="muc56_uploader")
    uploaded_delay_file = st.file_uploader("9. File Muc57 (Chậm trả).xlsx", type="xlsx", key="delay_uploader")

    st.header("⚙️ Thông số tùy chọn")
    st.markdown("Nhập thông tin chi nhánh và địa bàn để lọc và phân tích:")
    chi_nhanh_input = st.text_input("Nhập tên chi nhánh hoặc mã SOL (ví dụ: HANOI hoặc 001):", key="chi_nhanh_val").strip().upper()
    dia_ban_kt_input = st.text_input("Nhập tỉnh/thành kiểm toán (ví dụ: Bạc Liêu):", key="dia_ban_val").strip().lower()

st.markdown("---")

# Nút bắt đầu xử lý
if st.button("🚀 Bắt đầu xử lý dữ liệu và Tạo báo cáo", key="process_button"):
    # Kiểm tra các file bắt buộc phải có
    required_files_present = all([
        uploaded_crm4_files, uploaded_crm32_files, uploaded_muc_dich_file,
        uploaded_code_tsbd_file, uploaded_55_file, uploaded_56_file, uploaded_delay_file
    ])

    if not required_files_present:
        st.error("❌ Vui lòng tải lên tất cả các file bắt buộc: CRM4, CRM32, CODE_MDSDV4, CODE_LOAI_TSBD, Muc55, Muc56, Muc57.")
    elif not chi_nhanh_input:
        st.error("❌ Vui lòng nhập **tên chi nhánh hoặc mã SOL** để bắt đầu xử lý.")
    else:
        with st.spinner("⏳ Đang tải và xử lý dữ liệu... Quá trình này có thể mất vài phút. Vui lòng chờ."):
            # Tải dữ liệu từ các file đã upload
            df_crm4_raw = load_multiple_excel(uploaded_crm4_files)
            df_crm32_raw = load_multiple_excel(uploaded_crm32_files)
            df_muc_dich_data = load_excel(uploaded_muc_dich_file)
            df_code_tsbd_data = load_excel(uploaded_code_tsbd_file)
            df_sol_data = load_excel(uploaded_sol_file) # Đây là file MUC 17
            df_giai_ngan_data = load_excel(uploaded_giai_ngan_file)
            df_55_data = load_excel(uploaded_55_file)
            df_56_data = load_excel(uploaded_56_file)
            df_delay_data = load_excel(uploaded_delay_file)

            # Kiểm tra lại sau khi tải liệu liệu có bị lỗi đọc không
            if df_crm4_raw is None or df_crm32_raw is None or \
               df_muc_dich_data is None or df_code_tsbd_data is None or \
               df_55_data is None or df_56_data is None or df_delay_data is None:
                st.error("❌ Một hoặc nhiều file bắt buộc không thể đọc được. Vui lòng kiểm tra lại định dạng file hoặc nội dung.")
            else:
                try:
                    # Gọi hàm xử lý chính
                    results = process_crm_data(
                        df_crm4_raw, df_crm32_raw, df_muc_dich_data, df_code_tsbd_data,
                        df_sol_data, df_giai_ngan_data, df_55_data, df_56_data, df_delay_data,
                        chi_nhanh_input, dia_ban_kt_input
                    )

                    if results and all(r is not None for r in results): # Kiểm tra tất cả các kết quả trả về không phải là None
                        # Giải nén (unpack) các DataFrame kết quả
                        (pivot_full_res, df_crm4_filtered_res, pivot_final_res, pivot_merge_res,
                         df_crm32_filtered_res, pivot_mucdich_res, df_delay_res, df_gop_res,
                         df_count_res, df_bds_matched_res, df_crm4_for_tsbd_res) = results

                        st.success("🎉 Xử lý dữ liệu hoàn tất! Bạn có thể xem trước kết quả và tải file.")

                        st.subheader("📊 Xem trước Bảng tổng hợp khách hàng (KQ_KH)")
                        if not pivot_full_res.empty:
                            st.dataframe(pivot_full_res.head(10)) # Hiển thị 10 dòng đầu
                        else:
                            st.warning("⚠️ Bảng kết quả chính (KQ_KH) rỗng sau khi xử lý.")

                        # Tạo file Excel trong bộ nhớ
                        output = BytesIO()
                        with pd.ExcelWriter(output, engine='openpyxl') as writer:
                            # Ghi từng DataFrame vào một sheet riêng
                            if not df_crm4_filtered_res.empty: df_crm4_filtered_res.to_excel(writer, sheet_name='df_crm4_LOAI_TS', index=False)
                            if not pivot_final_res.empty: pivot_final_res.to_excel(writer, sheet_name='KQ_CRM4', index=False)
                            if not pivot_merge_res.empty: pivot_merge_res.to_excel(writer, sheet_name='Pivot_crm4', index=False)
                            if not df_crm32_filtered_res.empty: df_crm32_filtered_res.to_excel(writer, sheet_name='df_crm32_MUC_DICH', index=False)
                            if not pivot_full_res.empty: pivot_full_res.to_excel(writer, sheet_name='KQ_KH', index=False)
                            if not pivot_mucdich_res.empty: pivot_mucdich_res.to_excel(writer, sheet_name='Pivot_crm32', index=False)
                            if not df_delay_res.empty: df_delay_res.to_excel(writer, sheet_name='tieu chi 4 (cham tra)', index=False)
                            if not df_gop_res.empty: df_gop_res.to_excel(writer, sheet_name='tieu chi 3 (gop GN TT)', index=False)
                            if not df_count_res.empty: df_count_res.to_excel(writer, sheet_name='tieu chi 3 (dem GN TT)', index=False)
                            if not cif_canh_bao_series.empty: cif_canh_bao_series.to_excel(writer, sheet_name='tieu chi 2_ (dem GN TT)', index=False)
                            if not cif_canh_bao.empty: cif_canh_bao.to_excel(writer, sheet_name='tieu chi 2_ (dem GN TT)', index=False)    
                            # df_bds_matched_res: TSBĐ khác địa bàn
                            if not df_bds_matched_res.empty:
                                df_bds_matched_res.to_excel(writer, sheet_name='tieu chi 2 (BDS khac DB)', index=False)
                                df_bds_matched_res.to_excel(writer, sheet_name='tieu chi 2_dot3', index=False)
                            else:
                                st.info("⚠️ Không có dữ liệu TSBĐ khác địa bàn để xuất ra sheet 'tieu chi 2 (BDS khac DB)' và 'tieu chi 2_dot3'.")

                            # df_crm4_for_tsbd_res: TSBĐ quá hạn định giá
                            if not df_crm4_for_tsbd_res.empty:
                                df_crm4_for_tsbd_res.to_excel(writer, sheet_name='tieu chi 1)', index=False)
                            else:
                                st.info("⚠️ Không có dữ liệu TSBĐ quá hạn định giá để xuất ra sheet 'tieu chi 1)'.")


                        excel_data = output.getvalue()

                        # Nút tải xuống file Excel
                        if excel_data:
                            st.download_button(
                                label="📥 Tải xuống file Excel kết quả (KQ_XuLy.xlsx)",
                                data=excel_data,
                                file_name="KQ_XuLy_CRM.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                            )
                            st.balloons() # Hiệu ứng vui mắt khi hoàn thành
                        else:
                            st.warning("⚠️ Không có dữ liệu để xuất ra file Excel. Vui lòng kiểm tra lại đầu vào và điều kiện lọc.")
                    else:
                        st.error("❌ Xử lý dữ liệu không thành công hoặc không có kết quả trả về. Vui lòng kiểm tra lại dữ liệu đầu vào và các thông số.")
                except Exception as e:
                    st.error(f"Đã xảy ra lỗi nghiêm trọng trong quá trình xử lý: {e}")
                    st.exception(e) # Hiển thị stack trace để debug
else:
    st.info("💡 Bắt đầu bằng cách tải lên các file cần thiết và nhập thông tin ở thanh bên, sau đó nhấn nút 'Bắt đầu xử lý dữ liệu'.")
