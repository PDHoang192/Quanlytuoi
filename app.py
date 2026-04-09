import streamlit as st
import polars as pl
import json
import re
import plotly.express as px
from datetime import datetime, timedelta

st.set_page_config(page_title="Hệ Thống Phân Tích Tưới Thông Minh", layout="wide", page_icon="🚜")

# --- HÀM CHUẨN HÓA TÊN KHU VỰC (VÍ DỤ: "BỒN TG-ANT1" -> "ANT1") ---
def normalize_area_name(name):
    if not name: return ""
    # Chuyển thành chữ hoa
    name = str(name).upper()
    # Loại bỏ các từ khóa thừa và ký tự đặc biệt
    name = re.sub(r'(BỒN|TG|KHU|VƯỜN|[\s\-_])', '', name)
    return name

# --- HÀM ĐỌC DỮ LIỆU ---
def parse_raw_log(file_content):
    raw_text = file_content.getvalue().decode("utf-8").strip()
    try:
        data = json.loads(raw_text)
        return data if isinstance(data, list) else [data]
    except:
        pass

    data = []
    matches = re.findall(r'\{.*?\}', raw_text, re.DOTALL)
    for m in matches:
        try:
            clean_m = re.sub(r'\s+', ' ', m)
            data.append(json.loads(clean_m))
        except: continue
    return data

def load_data(uploaded_files, target_area_normalized):
    if not uploaded_files: return None
    
    all_records = []
    for f in uploaded_files:
        all_records.extend(parse_raw_log(f))
        
    if not all_records: return None
    
    df = pl.DataFrame(all_records)
    
    # 1. Chuyển đổi thời gian
    df = df.with_columns(pl.col("Thời gian").str.to_datetime("%Y-%m-%d %H-%M-%S").alias("dt"))

    # 2. Xử lý số liệu (120 -> 1.2)
    def clean_num(col_name):
        if col_name in df.columns:
            return (pl.col(col_name).cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False) / 100.0)
        return pl.lit(None)

    df = df.with_columns([
        clean_num("TBEC").alias("val_ec"),
        clean_num("TBPH").alias("val_ph"),
        clean_num("EC yêu cầu").alias("ec_target")
    ])

    # 3. CHUẨN HÓA TÊN KHU ĐỂ LỌC
    # Tạo một cột tạm "normalized_name" để so khớp
    df = df.with_columns(
        pl.col("Tên khu").map_elements(normalize_area_name, return_dtype=pl.Utf8).alias("norm_name")
    )
    
    # Lọc theo mã lõi
    filtered_df = df.filter(pl.col("norm_name").str.contains(target_area_normalized))
    
    return filtered_df.sort("dt"), df["Tên khu"].unique().to_list()

# --- LOGIC TÍNH TOÁN ---
def get_irrigation_events(df):
    if df.is_empty(): return pl.DataFrame()
    df_on = df.filter(pl.col("Trạng thái") == "Bật")
    df_off = df.filter(pl.col("Trạng thái") == "Tắt").select([pl.col("dt").alias("dt_end")])
    if df_on.is_empty() or df_off.is_empty(): return pl.DataFrame()

    df_pairs = df_on.join_asof(df_off, left_on="dt", right_on="dt_end", strategy="forward")
    df_pairs = df_pairs.filter(pl.col("dt_end").is_not_null())
    df_pairs = df_pairs.with_columns([
        ((pl.col("dt_end") - pl.col("dt")).dt.total_seconds()).alias("duration_s"),
        pl.col("dt").dt.date().alias("Date")
    ])
    return df_pairs.filter((pl.col("duration_s") >= 10) & (pl.col("duration_s") < 1200))

# --- GIAO DIỆN ---
st.title("🚜 Nhật Ký Vận Hành & Đối Soát EC")

with st.sidebar:
    st.header("Cấu Hình")
    raw_area = st.text_input("Nhập khu vực (vd: ANT-1 hoặc ANT1):", "ANT-1")
    target_norm = normalize_area_name(raw_area)
    
    st.divider()
    files_fert = st.file_uploader("1. File CHÂM PHÂN (JSON)", type=['json', 'txt'], accept_multiple_files=True)
    files_drip = st.file_uploader("2. File NHỎ GIỌT (TXT)", type=['txt', 'json'], accept_multiple_files=True)
    gap_limit = st.slider("Ngày nghỉ ngắt vụ:", 1, 10, 2)

if files_fert and files_drip:
    # Load và lấy danh sách tên khu để debug nếu cần
    res_fert, names_fert = load_data(files_fert, target_norm)
    res_drip, names_drip = load_data(files_drip, target_norm)

    # Hiển thị lỗi thông minh
    if (res_fert is None or res_fert.is_empty()) or (res_drip is None or res_drip.is_empty()):
        st.error(f"❌ Không tìm thấy dữ liệu khớp với mã '{target_norm}'")
        with st.expander("🔍 Kiểm tra tên các khu vực có trong file của bạn"):
            col1, col2 = st.columns(2)
            col1.write("**File Châm phân có:**")
            col1.write(names_fert if names_fert else "Trống")
            col2.write("**File Nhỏ giọt có:**")
            col2.write(names_drip if names_drip else "Trống")
    else:
        # --- TIẾN HÀNH PHÂN TÍCH KHI ĐÃ CÓ DỮ LIỆU KHỚP ---
        # 1. Tính EC Yêu cầu trung bình từ file Châm Phân
        daily_target = res_fert.filter(pl.col("Trạng thái") == "Bật").group_by(
            pl.col("dt").dt.date().alias("Date")
        ).agg(pl.col("ec_target").mean().alias("EC_Yeu_Cau"))

        # 2. Tính Thực tế từ file Nhỏ Giọt
        events = get_irrigation_events(res_drip)
        if not events.is_empty():
            daily_real = events.group_by("Date").agg([
                pl.count().alias("So_Lan_Tuoi"),
                pl.col("duration_s").mean().round(0).alias("TB_Giay"),
                pl.col("val_ec").mean().round(2).alias("EC_Thuc_Te")
            ])

            # 3. Gộp báo cáo
            final = daily_real.join(daily_target, on="Date", how="left").sort("Date")
            
            # 4. Hiển thị
            st.success(f"✅ Đã kết nối dữ liệu thành công cho mã lõi: {target_norm}")
            
            tab1, tab2 = st.tabs(["📈 Biểu đồ so sánh EC", "📋 Nhật ký chi tiết"])
            
            with tab1:
                fig = px.line(final.to_pandas(), x="Date", y=["EC_Yeu_Cau", "EC_Thuc_Te"],
                             markers=True, title=f"Đối soát EC Yêu cầu (Bồn) vs EC Thực tế (Khu {raw_area})",
                             color_discrete_map={"EC_Yeu_Cau": "#EF553B", "EC_Thuc_Te": "#00CC96"})
                st.plotly_chart(fig, use_container_width=True)
            
            with tab2:
                st.dataframe(final.to_pandas(), use_container_width=True, hide_index=True)
        else:
            st.warning("Dữ liệu nhỏ giọt tìm thấy nhưng không đủ các cặp Bật/Tắt để tính thời gian.")
else:
    st.info("💡 Hãy tải cả 2 loại file log để hệ thống tự động đồng bộ hóa khu vực.")
