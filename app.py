import streamlit as st
import polars as pl
import json
import re
import plotly.express as px
from datetime import datetime, timedelta

st.set_page_config(page_title="Hệ Thống Phân Tích Tưới", layout="wide", page_icon="🌱")

# --- CHUẨN HÓA TÊN KHU VỰC ---
def normalize_area_name(name):
    if not name: return ""
    name = str(name).upper()
    # Loại bỏ tiền tố và ký tự ngăn cách
    name = re.sub(r'(BỒN|TG|KHU|VƯỜN|[\s\-_])', '', name)
    return name

# --- ĐỌC FILE VỚI CƠ CHẾ CHỐNG SẬP (ANTI-CRASH) ---
def parse_raw_log(file_content):
    raw_text = file_content.getvalue().decode("utf-8").strip()
    data = []
    
    # Thử parse toàn bộ file
    try:
        parsed = json.loads(raw_text)
        if isinstance(parsed, list): return parsed
        return [parsed]
    except:
        # Nếu lỗi (như lỗi char 420914 bạn gặp), bóc tách từng object {...}
        matches = re.findall(r'\{.*?\}', raw_text, re.DOTALL)
        for m in matches:
            try:
                # Làm sạch các ký tự điều khiển ẩn
                clean_m = "".join(ch for ch in m if ord(ch) >= 32)
                data.append(json.loads(clean_m))
            except:
                continue # Bỏ qua object bị lỗi, đọc cái tiếp theo
    return data

def load_data(uploaded_files, target_area_normalized):
    # TRẢ VỀ DF RỖNG THAY VÌ NONE ĐỂ TRÁNH TYPEERROR
    empty_df = pl.DataFrame()
    if not uploaded_files: 
        return empty_df, []
    
    all_records = []
    for f in uploaded_files:
        all_records.extend(parse_raw_log(f))
        
    if not all_records: 
        return empty_df, []
    
    try:
        df = pl.DataFrame(all_records)
        
        # 1. Chuyển đổi thời gian
        if "Thời gian" in df.columns:
            df = df.with_columns(pl.col("Thời gian").str.to_datetime("%Y-%m-%d %H-%M-%S", strict=False).alias("dt"))
            df = df.filter(pl.col("dt").is_not_null())
        else:
            return empty_df, []

        # 2. Xử lý số liệu
        def clean_num(col_name):
            if col_name in df.columns:
                return (pl.col(col_name).cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False).fill_null(0) / 100.0)
            return pl.lit(0.0)

        df = df.with_columns([
            clean_num("TBEC").alias("val_ec"),
            clean_num("EC yêu cầu").alias("ec_target")
        ])

        # 3. Chuẩn hóa tên khu
        df = df.with_columns(
            pl.col("Tên khu").map_elements(normalize_area_name, return_dtype=pl.Utf8).alias("norm_name")
        )
        
        unique_names = df["Tên khu"].unique().to_list()
        filtered_df = df.filter(pl.col("norm_name").str.contains(target_area_normalized))
        
        return filtered_df.sort("dt"), unique_names
    except Exception as e:
        st.warning(f"Lỗi cấu trúc dữ liệu: {e}")
        return empty_df, []

# --- GIAO DIỆN CHÍNH ---
st.title("🚜 Hệ Thống Phân Tích Dữ Liệu Tưới")

with st.sidebar:
    st.header("Cài đặt")
    raw_area = st.text_input("Nhập mã khu (vd: ANT-3):", "ANT-3")
    target_norm = normalize_area_name(raw_area)
    
    files_fert = st.file_uploader("1. File CHÂM PHÂN (JSON/TXT)", type=['json', 'txt'], accept_multiple_files=True)
    files_drip = st.file_uploader("2. File NHỎ GIỌT (TXT/JSON)", type=['txt', 'json'], accept_multiple_files=True)

if files_fert and files_drip:
    # KHÔNG CÒN LỖI UNPACK VÌ LUÔN TRẢ VỀ 2 GIÁ TRỊ
    res_fert, names_fert = load_data(files_fert, target_norm)
    res_drip, names_drip = load_data(files_drip, target_norm)

    if res_fert.is_empty() or res_drip.is_empty():
        st.error(f"❌ Không tìm thấy dữ liệu cho mã '{target_norm}'")
        with st.expander("🔍 Xem danh sách tên khu hiện có trong file để sửa lại"):
            st.write("**File Châm phân:**", names_fert)
            st.write("**File Nhỏ giọt:**", names_drip)
    else:
        # Xử lý tính toán
        daily_target = res_fert.filter(pl.col("Trạng thái") == "Bật").group_by(
            pl.col("dt").dt.date().alias("Date")
        ).agg(pl.col("ec_target").mean().alias("EC_Yeu_Cau"))

        # Giả định file nhỏ giọt lấy TBEC thực tế
        daily_real = res_drip.group_by(pl.col("dt").dt.date().alias("Date")).agg(
            pl.col("val_ec").mean().round(2).alias("EC_Thuc_Te")
        )

        final = daily_real.join(daily_target, on="Date", how="left").sort("Date")
        
        st.success(f"✅ Đã đồng bộ dữ liệu khu {raw_area}")
        st.plotly_chart(px.line(final.to_pandas(), x="Date", y=["EC_Yeu_Cau", "EC_Thuc_Te"], markers=True), use_container_width=True)
        st.dataframe(final.to_pandas(), use_container_width=True)
else:
    st.info("Hãy tải file lên để bắt đầu.")
