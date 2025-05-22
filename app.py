import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
import os
import uuid

# Thiết lập trang rộng hơn
st.set_page_config(layout="wide")

@st.cache_data
def load_excel(uploaded_file_obj):
    """Hàm tải và đọc file Excel."""
    if uploaded_file_obj is not None:
        try:
            file_name = uploaded_file_obj.name
            if file_name.endswith('.xlsx'):
                return pd.read_excel(uploaded_file_obj, engine='openpyxl')
            elif file_name.endswith('.xls'):
                try:
                    return pd.read_excel(uploaded_file_obj)
                except Exception as e_xls:
                    st.warning(f"Lỗi khi đọc file .xls {file_name} với engine mặc định: {e_xls}. Thử với xlrd...")
                    try:
                        return pd.read_excel(uploaded_file_obj, engine='xlrd')
                    except Exception as e_xlrd:
                        st.error(f"Không thể đọc file .xls {file_name} với xlrd: {e_xlrd}")
                        return None
            else:
                return pd.read_excel(uploaded_file_obj)
        except Exception as e:
            st.error(f"Lỗi khi đọc file {uploaded_file_obj.name}: {e}")
            return None
    return None

@st.cache_data
def load_multiple_excel(uploaded_file_objs):
    """Hàm tải và đọc nhiều file Excel, sau đó ghép lại."""
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
    return None

def process_crm_data(
    df_crm4_raw, df_crm32_raw, df_muc_dich_data, df_code_tsbd_data,
    df_sol_data, df_giai_ngan_data, df_55_data, df_56_data, df_delay_data,
    chi_nhanh_filter, dia_ban_kt_filter
):
    """
    Hàm chính để xử lý tất cả dữ liệu CRM.
    """
    df_crm4 = df_crm4_raw.copy() if df_crm4_raw is not None else None
    df_crm32 = df_crm32_raw.copy() if df_crm32_raw is not None else None

    if df_crm4 is None or df_crm32 is None:
        st.error("Dữ liệu CRM4 hoặc CRM32 chưa được tải lên hoặc bị lỗi.")
        return None, None, None, None, None, None, None, None, None, None, None, None, None

    st.info(f"Bắt đầu xử lý dữ liệu cho chi nhánh: '{chi_nhanh_filter}' và địa bàn kiểm toán: '{dia_ban_kt_filter}'")

    # Lọc dữ liệu theo BRCD chứa chuỗi nhập vào
    df_crm4_filtered = df_crm4[df_crm4['BRANCH_VAY'].astype(str).str.upper().str.contains(chi_nhanh_filter)]
    df_crm32_filtered = df_crm32[df_crm32['BRCD'].astype(str).str.upper().str.contains(chi_nhanh_filter)]

    st.write(f"📌 Số dòng CRM4 sau khi lọc theo chi nhánh '{chi_nhanh_filter}': {len(df_crm4_filtered)}")
    st.write(f"📌 Số dòng CRM32 sau khi lọc theo chi nhánh '{chi_nhanh_filter}': {len(df_crm32_filtered)}")

    if df_code_tsbd_data is None:
        st.error("File CODE_LOAI TSBD chưa được tải hoặc lỗi.")
        return None, None, None, None, None, None, None, None, None, None, None, None, None
    df_code_tsbd = df_code_tsbd_data[['CODE CAP 2', 'CODE']].copy()
    df_code_tsbd.columns = ['CAP_2', 'LOAI_TS']
    df_tsbd_code = df_code_tsbd[['CAP_2', 'LOAI_TS']].drop_duplicates()
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
    df_vay = df_vay_4[~df_vay_4['LOAI'].isin(['Bao lanh', 'LC'])]

    if not df_vay.empty:
        pivot_ts = df_vay.pivot_table(
            index='CIF_KH_VAY', columns='LOAI_TS', values='TS_KW_VND',
            aggfunc='sum', fill_value=0
        ).add_suffix(' (Giá trị TS)').reset_index()
        pivot_no = df_vay.pivot_table(
            index='CIF_KH_VAY', columns='LOAI_TS', values='DU_NO_PHAN_BO_QUY_DOI',
            aggfunc='sum', fill_value=0
        ).reset_index()
        pivot_merge = pivot_no.merge(pivot_ts, on='CIF_KH_VAY', how='left')
        ts_value_cols = [col for col in pivot_ts.columns if col.endswith(' (Giá trị TS)') and col != 'CIF_KH_VAY (Giá trị TS)']
        pivot_merge['GIÁ TRỊ TS'] = pivot_ts[ts_value_cols].sum(axis=1)
        du_no_cols = [col for col in pivot_no.columns if col != 'CIF_KH_VAY']
        pivot_merge['DƯ NỢ'] = pivot_no[du_no_cols].sum(axis=1)
    else:
        st.warning("Không có dữ liệu 'Cho vay' sau khi lọc, bảng pivot có thể rỗng hoặc lỗi.")
        pivot_merge = pd.DataFrame(columns=['CIF_KH_VAY', 'GIÁ TRỊ TS', 'DƯ NỢ'])

    df_info = df_crm4_filtered[['CIF_KH_VAY', 'TEN_KH_VAY', 'CUSTTPCD', 'NHOM_NO']].drop_duplicates(subset='CIF_KH_VAY')
    pivot_final = df_info.merge(pivot_merge, on='CIF_KH_VAY', how='left')
    pivot_final = pivot_final.reset_index().rename(columns={'index': 'STT'})
    pivot_final['STT'] += 1
    du_no_pivot_cols = [col for col in pivot_merge.columns if col not in ['CIF_KH_VAY', 'GIÁ TRỊ TS', 'DƯ NỢ'] and '(Giá trị TS)' not in col]
    ts_value_pivot_cols = [col for col in pivot_merge.columns if '(Giá trị TS)' in col and col != 'CIF_KH_VAY']
    cols_order = ['STT', 'CUSTTPCD', 'CIF_KH_VAY', 'TEN_KH_VAY', 'NHOM_NO'] + \
                 sorted(du_no_pivot_cols) + sorted(ts_value_pivot_cols) + ['DƯ NỢ', 'GIÁ TRỊ TS']
    cols_order_existing = [col for col in cols_order if col in pivot_final.columns]
    pivot_final = pivot_final[cols_order_existing]

    df_crm32_filtered = df_crm32_filtered.copy()
    df_crm32_filtered['MA_PHE_DUYET'] = df_crm32_filtered['CAP_PHE_DUYET'].astype(str).str.split('-').str[0].str.strip().str.zfill(2)
    ma_cap_c = [f"{i:02d}" for i in range(1, 8)] + [f"{i:02d}" for i in range(28, 32)]
    list_cif_cap_c = df_crm32_filtered[df_crm32_filtered['MA_PHE_DUYET'].isin(ma_cap_c)]['CUSTSEQLN'].unique().astype(str)
    list_co_cau = ['ACOV1', 'ACOV3', 'ATT01', 'ATT02', 'ATT03', 'ATT04',
                   'BCOV1', 'BCOV2', 'BTT01', 'BTT02', 'BTT03',
                   'CCOV2', 'CCOV3', 'CTT03', 'RCOV3', 'RTT03']
    cif_co_cau = df_crm32_filtered[df_crm32_filtered['SCHEME_CODE'].isin(list_co_cau)]['CUSTSEQLN'].unique().astype(str)

    if df_muc_dich_data is None:
        st.error("File CODE_MDSDV4 chưa được tải hoặc lỗi.")
        return None, None, None, None, None, None, None, None, None, None, None, None, None
    df_muc_dich_vay_src = df_muc_dich_data[['CODE_MDSDV4', 'GROUP']].copy()
    df_muc_dich_vay_src.columns = ['MUC_DICH_VAY_CAP_4', 'MUC DICH']
    df_muc_dich_map = df_muc_dich_vay_src[['MUC_DICH_VAY_CAP_4', 'MUC DICH']].drop_duplicates()
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
    pivot_mucdich['DƯ NỢ CRM32'] = pivot_mucdich[muc_dich_cols_for_sum].sum(axis=1)
    pivot_mucdich['CUSTSEQLN'] = pivot_mucdich['CUSTSEQLN'].astype(str).str.strip()
    pivot_final_CRM32 = pivot_mucdich.rename(columns={'CUSTSEQLN': 'CIF_KH_VAY'})
    pivot_final['CIF_KH_VAY'] = pivot_final['CIF_KH_VAY'].astype(str).str.strip()
    pivot_full = pivot_final.merge(pivot_final_CRM32, on='CIF_KH_VAY', how='left')
    pivot_full.fillna(0, inplace=True)

    pivot_full['LECH'] = pivot_full['DƯ NỢ'] - pivot_full.get('DƯ NỢ CRM32', 0)
    pivot_full['LECH'] = pivot_full['LECH'].fillna(0)
    cif_lech = pivot_full[pivot_full['LECH'] != 0]['CIF_KH_VAY'].unique()
    df_crm4_blank = df_crm4_filtered[~df_crm4_filtered['LOAI'].isin(['Cho vay', 'Bao lanh', 'LC'])].copy()
    df_crm4_blank['CIF_KH_VAY'] = df_crm4_blank['CIF_KH_VAY'].astype(str).str.strip()

    if not df_crm4_blank.empty and cif_lech.size > 0:
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
        pivot_full['(blank)'] = 0
    cols = list(pivot_full.columns)
    if '(blank)' in cols and 'DƯ NỢ CRM32' in cols:
        cols.insert(cols.index('DƯ NỢ CRM32'), cols.pop(cols.index('(blank)')))
        pivot_full = pivot_full[cols]
    pivot_full['LECH'] = pivot_full['DƯ NỢ'] - pivot_full.get('DƯ NỢ CRM32', 0)

    pivot_full['NHOM_NO'] = pivot_full['NHOM_NO'].astype(str)
    pivot_full['Nợ nhóm 2'] = pivot_full['NHOM_NO'].apply(lambda x: 'x' if x.strip() == '2' or x.strip() == '2.0' else '')
    pivot_full['Nợ xấu'] = pivot_full['NHOM_NO'].apply(lambda x: 'x' if x.strip() in ['3', '4', '5', '3.0', '4.0', '5.0'] else '')
    pivot_full['CIF_KH_VAY'] = pivot_full['CIF_KH_VAY'].astype(str).str.strip()
    list_cif_cap_c_str = [str(c).strip() for c in list_cif_cap_c]
    cif_co_cau_str = [str(c).strip() for c in cif_co_cau]
    pivot_full['Chuyên gia PD cấp C duyệt'] = pivot_full['CIF_KH_VAY'].apply(lambda x: 'x' if x in list_cif_cap_c_str else '')
    pivot_full['NỢ CƠ_CẤU'] = pivot_full['CIF_KH_VAY'].apply(lambda x: 'x' if x in cif_co_cau_str else '')

    df_baolanh = df_crm4_filtered[df_crm4_filtered['LOAI'] == 'Bao lanh'].copy()
    df_lc = df_crm4_filtered[df_crm4_filtered['LOAI'] == 'LC'].copy()
    df_baolanh['CIF_KH_VAY'] = df_baolanh['CIF_KH_VAY'].astype(str).str.strip()
    df_lc['CIF_KH_VAY'] = df_lc['CIF_KH_VAY'].astype(str).str.strip()
    df_baolanh_sum = df_baolanh.groupby('CIF_KH_VAY', as_index=False)['DU_NO_PHAN_BO_QUY_DOI'].sum().rename(columns={'DU_NO_PHAN_BO_QUY_DOI': 'DƯ_NỢ_BẢO_LÃNH'})
    df_lc_sum = df_lc.groupby('CIF_KH_VAY', as_index=False)['DU_NO_PHAN_BO_QUY_DOI'].sum().rename(columns={'DU_NO_PHAN_BO_QUY_DOI': 'DƯ_NỢ_LC'})
    pivot_full = pivot_full.merge(df_baolanh_sum, on='CIF_KH_VAY', how='left')
    pivot_full = pivot_full.merge(df_lc_sum, on='CIF_KH_VAY', how='left')
    pivot_full['DƯ_NỢ_BẢO_LÃNH'] = pivot_full['DƯ_NỢ_BẢO_LÃNH'].fillna(0)
    pivot_full['DƯ_NỢ_LC'] = pivot_full['DƯ_NỌ_LC'].fillna(0)

    if df_giai_ngan_data is not None:
        df_giai_ngan = df_giai_ngan_data.copy()
        df_crm32_filtered['KHE_UOC'] = df_crm32_filtered['KHE_UOC'].astype(str).str.strip()
        df_crm32_filtered['CUSTSEQLN'] = df_crm32_filtered['CUSTSEQLN'].astype(str).str.strip()
        df_giai_ngan['FORACID'] = df_giai_ngan['FORACID'].astype(str).str.strip()
        df_match_gn = df_crm32_filtered[df_crm32_filtered['KHE_UOC'].isin(df_giai_ngan['FORACID'])].copy()
        ds_cif_tien_mat = df_match_gn['CUSTSEQLN'].unique()
        pivot_full['GIẢI_NGÂN_TIEN_MAT'] = pivot_full['CIF_KH_VAY'].isin(ds_cif_tien_mat).map({True: 'x', False: ''})
    else:
        pivot_full['GIẢI_NGÂN_TIEN_MAT'] = ''
        st.warning("Không có dữ liệu giải ngân tiền mặt để xử lý.")

    df_crm4_filtered['CAP_2'] = df_crm4_filtered['CAP_2'].astype(str)
    df_cc_tctd = df_crm4_filtered[df_crm4_filtered['CAP_2'].str.contains('TCTD', case=False, na=False)].copy()
    df_cc_tctd['CIF_KH_VAY'] = df_cc_tctd['CIF_KH_VAY'].astype(str).str.strip()
    df_cc_flag = df_cc_tctd[['CIF_KH_VAY']].drop_duplicates()
    df_cc_flag['Cầm cố tại TCTD khác'] = 'x'
    pivot_full = pivot_full.merge(df_cc_flag, on='CIF_KH_VAY', how='left')
    pivot_full['Cầm cố tại TCTD khác'] = pivot_full['Cầm cố tại TCTD khác'].fillna('')

    pivot_full['DƯ NỢ'] = pd.to_numeric(pivot_full['DƯ NỢ'], errors='coerce').fillna(0)
    pivot_full['CUSTTPCD'] = pivot_full['CUSTTPCD'].astype(str)
    top10_khcn_cif = pivot_full[pivot_full['CUSTTPCD'].str.strip().str.lower() == 'ca nhan'].nlargest(10, 'DƯ NỢ')['CIF_KH_VAY'].astype(str).str.strip().values
    pivot_full['Top 10 dư nợ KHCN'] = pivot_full['CIF_KH_VAY'].apply(lambda x: 'x' if x in top10_khcn_cif else '')
    top10_khdn_cif = pivot_full[pivot_full['CUSTTPCD'].str.strip().str.lower() == 'doanh nghiep'].nlargest(10, 'DƯ NỢ')['CIF_KH_VAY'].astype(str).str.strip().values
    pivot_full['Top 10 dư nợ KHDN'] = pivot_full['CIF_KH_VAY'].apply(lambda x: 'x' if x in top10_khdn_cif else '')

    # TSBĐ quá hạn định giá
    ngay_danh_gia_tsbd = pd.to_datetime("2025-03-31")
    loai_ts_r34 = ['BĐS', 'MMTB', 'PTVT']
    df_crm4_for_tsbd = df_crm4_filtered.copy()
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

    # Xử lý TSBĐ khác địa bàn (từ code Colab)
    cif_canh_bao_series = pd.Series(dtype=str)
    cif_canh_bao = np.array([], dtype=str)
    df_bds_matched = pd.DataFrame()  # Khởi tạo df_bds_matched rỗng
    if df_sol_data is not None and dia_ban_kt_filter:
        df_sol = df_sol_data.copy()
        if all(col in df_sol.columns for col in ['C01', 'C02', 'C19']):
            # Chuẩn hóa dữ liệu
            df_crm4_filtered['SECU_SRL_NUM'] = df_crm4_filtered['SECU_SRL_NUM'].astype(str).str.strip()
            df_sol['C01'] = df_sol['C01'].astype(str).str.strip()
            df_sol['C02'] = df_sol['C02'].astype(str).str.strip()
            df_sol['C19'] = df_sol['C19'].astype(str)

            ds_secu = df_crm4_filtered['SECU_SRL_NUM'].dropna().unique()
            df_17_filtered = df_sol[df_sol['C01'].isin(ds_secu)]
            df_bds = df_17_filtered[df_17_filtered['C02'].str.strip() == 'Bat dong san'].copy()
            df_bds_matched = df_bds[df_bds['C01'].isin(df_crm4_filtered['SECU_SRL_NUM'])].copy()

            # Debug thông tin
            st.write("Debug: Số dòng df_17_filtered (sau lọc C01 khớp SECU_SRL_NUM):", len(df_17_filtered))
            st.write("Debug: Số dòng df_bds (sau lọc C02 = 'Bat dong san'):", len(df_bds))
            st.write("Debug: Số dòng df_bds_matched:", len(df_bds_matched))

            if not df_bds_matched.empty:
                def extract_tinh_thanh(diachi):
                    if pd.isna(diachi): return ''
                    parts = str(diachi).split(',')
                    return parts[-1].strip().lower() if parts else ''
                df_bds_matched['TINH_TP_TSBD'] = df_bds_matched['C19'].apply(extract_tinh_thanh)
                df_bds_matched['CANH_BAO_TS_KHAC_DIABAN'] = df_bds_matched['TINH_TP_TSBD'].apply(
                    lambda x: 'x' if x and x != dia_ban_kt_filter.strip().lower() else ''
                )
                st.write("Debug: Các tỉnh/thành trong TSBĐ:", df_bds_matched['TINH_TP_TSBD'].unique())
                st.write("Debug: Địa bàn kiểm toán:", dia_ban_kt_filter)
                ma_ts_canh_bao = df_bds_matched[df_bds_matched['CANH_BAO_TS_KHAC_DIABAN'] == 'x']['C01'].unique()
                cif_canh_bao_series = df_crm4_filtered[df_crm4_filtered['SECU_SRL_NUM'].isin(ma_ts_canh_bao)]['CIF_KH_VAY']
                cif_canh_bao = cif_canh_bao_series.astype(str).str.strip().dropna().unique()
                st.write("Debug: Số mã tài sản cảnh báo (ma_ts_canh_bao):", len(ma_ts_canh_bao))
                st.write("Debug: Số CIF cảnh báo (cif_canh_bao):", len(cif_canh_bao))
    pivot_full['KH có TSBĐ khác địa bàn'] = pivot_full['CIF_KH_VAY'].apply(
        lambda x: 'x' if x in cif_canh_bao else ''
    )

    df_gop = pd.DataFrame()
    df_count = pd.DataFrame()
    if df_55_data is not None and df_56_data is not None:
        df_tt_raw = df_55_data.copy()
        df_gn_raw = df_56_data.copy()
        df_tt = df_tt_raw[['CUSTSEQLN', 'NMLOC', 'KHE_UOC', 'SOTIENGIAINGAN', 'NGAYGN', 'NGAYDH', 'NGAY_TT', 'LOAITIEN']].copy()
        df_tt.columns = ['CIF', 'TEN_KHACH_HANG', 'KHE_UOC', 'SO_TIEN_GIAI_NGAN_VND', 'NGAY_GIAI_NGAN', 'NGAY_DAO_HAN', 'NGAY_TT', 'LOAI_TIEN_HD']
        df_tt['GIAI_NGAN_TT'] = 'Tất toán'
        df_tt['NGAY'] = pd.to_datetime(df_tt['NGAY_TT'], errors='coerce')
        df_tt['CIF'] = df_tt['CIF'].astype(str).str.strip()
        df_gn = df_gn_raw[['CIF', 'TEN_KHACH_HANG', 'KHE_UOC', 'SO_TIEN_GIAI_NGAN_VND', 'NGAY_GIAI_NGAN', 'NGAY_DAO_HAN', 'LOAI_TIEN_HD']].copy()
        df_gn['GIAI_NGAN_TT'] = 'Giải ngân'
        df_gn['NGAY_GIAI_NGAN'] = pd.to_datetime(df_gn['NGAY_GIAI_NGAN'], format='%Y%m%d', errors='coerce')
        df_gn['NGAY_DAO_HAN'] = pd.to_datetime(df_gn['NGAY_DAO_HAN'], format='%Y%m%d', errors='coerce')
        df_gn['NGAY'] = df_gn['NGAY_GIAI_NGAN']
        df_gn['CIF'] = df_gn['CIF'].astype(str).str.strip()
        df_gop = pd.concat([df_tt, df_gn], ignore_index=True)
        df_gop = df_gop[df_gop['NGAY'].notna()]
        df_gop = df_gop.sort_values(by=['CIF', 'NGAY', 'GIAI_NGAN_TT'])
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
        st.warning("Không có dữ liệu Mục 55 hoặc Mục 56 để xử lý giao dịch giải ngân/tất toán.")

    df_delay_processed = pd.DataFrame()
    if df_delay_data is not None:
        df_delay = df_delay_data.copy()
        if 'CIF_ID' not in df_delay.columns and 'CUSTSEQLN' in df_delay.columns:
            df_delay.rename(columns={'CUSTSEQLN': 'CIF_ID'}, inplace=True)
        if 'CIF_ID' in df_delay.columns:
            df_delay['CIF_ID'] = df_delay['CIF_ID'].astype(str).str.strip()
            df_delay['NGAY_DEN_HAN_TT'] = pd.to_datetime(df_delay['NGAY_DEN_HAN_TT'], errors='coerce')
            df_delay['NGAY_THANH_TOAN'] = pd.to_datetime(df_delay['NGAY_THANH_TOAN'], errors='coerce')
            ngay_danh_gia_cham_tra = pd.to_datetime("2025-03-31")
            df_delay['NGAY_THANH_TOAN_FILL'] = df_delay['NGAY_THANH_TOAN'].fillna(ngay_danh_gia_cham_tra)
            df_delay['SO_NGAY_CHAM_TRA'] = (df_delay['NGAY_THANH_TOAN_FILL'] - df_delay['NGAY_DEN_HAN_TT']).dt.days
            mask_period = df_delay['NGAY_DEN_HAN_TT'].dt.year.between(2023, 2025)
            df_delay = df_delay[mask_period & df_delay['NGAY_DEN_HAN_TT'].notna()]
            pivot_full_temp_for_delay = pivot_full[['CIF_KH_VAY', 'DƯ NỢ', 'NHOM_NO']].rename(columns={'CIF_KH_VAY': 'CIF_ID'})
            pivot_full_temp_for_delay['CIF_ID'] = pivot_full_temp_for_delay['CIF_ID'].astype(str).str.strip()
            df_delay = df_delay.merge(pivot_full_temp_for_delay, on='CIF_ID', how='left')
            df_delay['NHOM_NO'] = pd.to_numeric(df_delay['NHOM_NO'], errors='coerce')
            df_delay = df_delay[df_delay['NHOM_NO'] == 1.0]
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
            df_unique_delay = df_delay.drop_duplicates(subset=['CIF_ID', 'NGAY_DEN_HAN_TT_DATE'], keep='first')
            df_dem_delay = df_unique_delay.groupby(['CIF_ID', 'CAP_CHAM_TRA']).size().unstack(fill_value=0)
            df_dem_delay['KH Phát sinh chậm trả > 10 ngày'] = np.where(df_dem_delay.get('>=10', 0) > 0, 'x', '')
            df_dem_delay['KH Phát sinh chậm trả 4-9 ngày'] = np.where(
                (df_dem_delay.get('>=10', 0) == 0) & (df_dem_delay.get('4-9', 0) > 0), 'x', ''
            )
            pivot_full = pivot_full.merge(
                df_dem_delay[['KH Phát sinh chậm trả > 10 ngày', 'KH Phát sinh chậm trả 4-9 ngày']],
                left_on='CIF_KH_VAY', right_index=True, how='left'
            )
            pivot_full[['KH Phát sinh chậm trả > 10 ngày', 'KH Phát sinh chậm trả 4-9 ngày']] = \
                pivot_full[['KH Phát sinh chậm trả > 10 ngày', 'KH Phát sinh chậm trả 4-9 ngày']].fillna('')
            df_delay_processed = df_delay
        else:
            st.warning("Cột 'CIF_ID' không tìm thấy trong dữ liệu Mục 57 (chậm trả). Bỏ qua xử lý chậm trả.")
            pivot_full['KH Phát sinh chậm trả > 10 ngày'] = ''
            pivot_full['KH Phát sinh chậm trả 4-9 ngày'] = ''
    else:
        pivot_full['KH Phát sinh chậm trả > 10 ngày'] = ''
        pivot_full['KH Phát sinh chậm trả 4-9 ngày'] = ''
        st.warning("Không có dữ liệu Mục 57 (chậm trả) để xử lý.")

    # Debugging: Kiểm tra df_bds_matched
    st.subheader("⚙️ Debug: Thông tin DataFrame TSBĐ khác địa bàn (df_bds_matched)")
    if df_bds_matched is not None:
        st.write(f"Shape của df_bds_matched: {df_bds_matched.shape}")
        if not df_bds_matched.empty:
            st.dataframe(df_bds_matched.head())
        else:
            st.info("⚠️ df_bds_matched rỗng sau khi xử lý. Các sheet liên quan sẽ không được tạo.")
    else:
        st.warning("❌ df_bds_matched là None. Có lỗi xảy ra hoặc file Mục 17 chưa được tải.")

    return (pivot_full, df_crm4_filtered, pivot_final, pivot_merge,
            df_crm32_filtered, pivot_mucdich, df_delay_processed, df_gop, df_count,
            df_bds_matched, df_crm4_for_tsbd, cif_canh_bao_series, cif_canh_bao)

# Giao diện Streamlit
st.title("Ứng dụng xử lý dữ liệu CRM và tạo báo cáo")

with st.sidebar:
    st.header("Tải lên các file Excel")
    uploaded_crm4_files = st.file_uploader("1. Các file CRM4 (Du_no_theo_tai_san_dam_bao_ALL)", type=["xls", "xlsx"], accept_multiple_files=True, key="crm4")
    uploaded_crm32_files = st.file_uploader("2. Các file CRM32 (RPT_CRM_32)", type=["xls", "xlsx"], accept_multiple_files=True, key="crm32")
    uploaded_muc_dich_file = st.file_uploader("3. File CODE_MDSDV4.xlsx", type="xlsx", key="m_dich")
    uploaded_code_tsbd_file = st.file_uploader("4. File CODE_LOAI TSBD.xlsx", type="xlsx", key="tsbd")
    uploaded_sol_file = st.file_uploader("5. File MUC 17.xlsx (Dữ liệu SOL)", type="xlsx", key="sol")
    uploaded_giai_ngan_file = st.file_uploader("6. File Giai_ngan_tien_mat_1_ty (hoặc tương tự)", type=["xls", "xlsx"], key="giai_ngan")
    uploaded_55_file = st.file_uploader("7. File Muc55 (Tất toán).xlsx", type="xlsx", key="muc55")
    uploaded_56_file = st.file_uploader("8. File Muc56 (Giải ngân).xlsx", type="xlsx", key="muc56")
    uploaded_delay_file = st.file_uploader("9. File Muc57 (Chậm trả).xlsx", type="xlsx", key="delay")

    st.header("Thông số tùy chọn")
    chi_nhanh_input = st.text_input("Nhập tên chi nhánh hoặc mã SOL (ví dụ: HANOI hoặc 001):", key="chi_nhanh_val").strip().upper()
    dia_ban_kt_input = st.text_input("Nhập tỉnh/thành kiểm toán (ví dụ: Bạc Liêu):", key="dia_ban_val").strip().lower()

if st.button("🚀 Bắt đầu xử lý dữ liệu", key="process_button"):
    required_files_present = all([
        uploaded_crm4_files, uploaded_crm32_files, uploaded_muc_dich_file,
        uploaded_code_tsbd_file,
        uploaded_55_file, uploaded_56_file, uploaded_delay_file
    ])

    if not required_files_present:
        st.error("Vui lòng tải lên các file CRM4, CRM32, CODE_MDSDV4, CODE_LOAI_TSBD, Muc55, Muc56, Muc57.")
    elif not chi_nhanh_input:
        st.error("Vui lòng nhập tên chi nhánh hoặc mã SOL.")
    else:
        with st.spinner("⏳ Đang tải và xử lý dữ liệu... Vui lòng chờ."):
            df_crm4_raw = load_multiple_excel(uploaded_crm4_files)
            df_crm32_raw = load_multiple_excel(uploaded_crm32_files)
            df_muc_dich_data = load_excel(uploaded_muc_dich_file)
            df_code_tsbd_data = load_excel(uploaded_code_tsbd_file)
            df_sol_data = load_excel(uploaded_sol_file)
            df_giai_ngan_data = load_excel(uploaded_giai_ngan_file)
            df_55_data = load_excel(uploaded_55_file)
            df_56_data = load_excel(uploaded_56_file)
            df_delay_data = load_excel(uploaded_delay_file)

            if df_crm4_raw is None or df_crm32_raw is None or \
               df_muc_dich_data is None or df_code_tsbd_data is None or \
               df_55_data is None or df_56_data is None or df_delay_data is None:
                st.error("Một hoặc nhiều file bắt buộc không thể đọc được. Vui lòng kiểm tra lại file đã tải lên.")
            else:
                try:
                    results = process_crm_data(
                        df_crm4_raw, df_crm32_raw, df_muc_dich_data, df_code_tsbd_data,
                        df_sol_data, df_giai_ngan_data, df_55_data, df_56_data, df_delay_data,
                        chi_nhanh_input, dia_ban_kt_input
                    )

                    if results and all(r is not None for r in results):
                        (pivot_full_res, df_crm4_filtered_res, pivot_final_res, pivot_merge_res,
                         df_crm32_filtered_res, pivot_mucdich_res, df_delay_res, df_gop_res,
                         df_count_res, df_bds_matched_res, df_crm4_for_tsbd_res,
                         cif_canh_bao_series_res, cif_canh_bao_res) = results

                        st.success("🎉 Xử lý dữ liệu hoàn tất!")

                        st.subheader("Xem trước kết quả chính (KQ_KH - pivot_full)")
                        if pivot_full_res is not None and not pivot_full_res.empty:
                            st.dataframe(pivot_full_res.head())
                        else:
                            st.warning("Bảng kết quả chính (pivot_full) rỗng.")

                        output = BytesIO()
                        with pd.ExcelWriter(output, engine='openpyxl') as writer:
                            if df_crm4_filtered_res is not None and not df_crm4_filtered_res.empty:
                                df_crm4_filtered_res.to_excel(writer, sheet_name='df_crm4_LOAI_TS', index=False)
                            if pivot_final_res is not None and not pivot_final_res.empty:
                                pivot_final_res.to_excel(writer, sheet_name='KQ_CRM4', index=False)
                            if pivot_merge_res is not None and not pivot_merge_res.empty:
                                pivot_merge_res.to_excel(writer, sheet_name='Pivot_crm4', index=False)
                            if df_crm32_filtered_res is not None and not df_crm32_filtered_res.empty:
                                df_crm32_filtered_res.to_excel(writer, sheet_name='df_crm32_MUC_DICH', index=False)
                            if pivot_full_res is not None and not pivot_full_res.empty:
                                pivot_full_res.to_excel(writer, sheet_name='KQ_KH', index=False)
                            if pivot_mucdich_res is not None and not pivot_mucdich_res.empty:
                                pivot_mucdich_res.to_excel(writer, sheet_name='Pivot_crm32', index=False)
                            if df_delay_res is not None and not df_delay_res.empty:
                                df_delay_res.to_excel(writer, sheet_name='tieu chi 4 (cham tra)', index=False)
                            if df_gop_res is not None and not df_gop_res.empty:
                                df_gop_res.to_excel(writer, sheet_name='tieu chi 3 (gop GN TT)', index=False)
                            if df_count_res is not None and not df_count_res.empty:
                                df_count_res.to_excel(writer, sheet_name='tieu chi 3 (dem GN TT)', index=False)
                            if df_bds_matched_res is not None and not df_bds_matched_res.empty:
                                df_bds_matched_res.to_excel(writer, sheet_name='tieu chi 2 (BDS khac DB)', index=False)
                                df_bds_matched_res.to_excel(writer, sheet_name='tieu chi 2_dot3', index=False)
                            else:
                                st.info("⚠️ Không có dữ liệu TSBĐ khác địa bàn để xuất ra sheet 'tieu chi 2 (BDS khac DB)' và 'tieu chi 2_dot3'.")
                            if df_crm4_for_tsbd_res is not None and not df_crm4_for_tsbd_res.empty:
                                df_crm4_for_tsbd_res.to_excel(writer, sheet_name='tieu chi 1)', index=False)
                            else:
                                st.info("⚠️ Không có dữ liệu TSBĐ quá hạn định giá để xuất ra sheet 'tieu chi 1)'.")
                            if cif_canh_bao_series_res is not None and not cif_canh_bao_series_res.empty:
                                cif_canh_bao_series_res.to_excel(writer, sheet_name='CIF_CanhBao_Series', index=False)
                            else:
                                st.info("⚠️ Không có dữ liệu CIF_CanhBao_Series để xuất ra sheet.")
                            if cif_canh_bao_res is not None and len(cif_canh_bao_res) > 0:
                                pd.DataFrame(cif_canh_bao_res, columns=['CIF_KH_VAY']).to_excel(writer, sheet_name='CIF_CanhBao_Unique', index=False)
                            else:
                                st.info("⚠️ Không có dữ liệu CIF_CanhBao_Unique để xuất ra sheet.")

                        excel_data = output.getvalue()
                        if excel_data:
                            st.download_button(
                                label="📥 Tải xuống file Excel kết quả (KQ_XuLy.xlsx)",
                                data=excel_data,
                                file_name="KQ_XuLy_CRM.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                            )
                            st.balloons()
                        else:
                            st.warning("Không có dữ liệu để xuất ra file Excel.")
                    else:
                        st.error("Xử lý dữ liệu không thành công hoặc không có kết quả trả về.")
                except Exception as e:
                    st.error(f"Đã xảy ra lỗi nghiêm trọng trong quá trình xử lý: {e}")
                    st.exception(e)
else:
    st.info("ℹ️ Vui lòng tải lên các file cần thiết và nhập thông tin ở thanh bên, sau đó nhấn nút 'Bắt đầu xử lý dữ liệu'.")
