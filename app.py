import streamlit as st
import polars as pl
import json
import re
import plotly.express as px

st.set_page_config(page_title="Hệ Thống Phân Tích Tưới", layout="wide")

# --- HÀM QUÉT DỮ LIỆU CỰC MẠNH (DEEP SCAN) ---
def parse_raw_log(file_content):
    raw_text = file_content.getvalue().decode("utf-8").strip()
    data = []
    
    # Chiến thuật 1: Regex lấy cụm nội dung giữa các dấu ngoặc nhọn
    # Dùng non-greedy để lấy từng object riêng lẻ
    matches = re.findall(r'\{[^{}]*\}', raw_text)
    
    # Nếu không tìm thấy bằng regex đơn giản, thử dùng logic đếm dấu ngoặc (cho JSON phức tạp)
    if not matches:
        start = -1
        count = 0
        for i, char in enumerate(raw_text):
            if char == '{':
                if count == 0: start = i
                count += 1
            elif char == '}':
                count -= 1
                if count == 0 and start != -1:
                    matches.append(raw_text[start:i+1])
    
    for m in matches:
        try:
            # Loại bỏ các ký tự xuống dòng và khoảng trắng thừa gây lỗi
            clean_m = re.sub(r'\s+', ' ', m)
            obj = json.loads(clean_m)
            data.append(obj)
        except:
            continue
            
    return data

def normalize_area_name(name):
    if not name: return ""
    return re.sub(r'(BỒN|TG|KHU|VƯỜN|[\s\-_])', '', str(name).upper())

def load_data(uploaded_files, target_area_normalized):
    all_records = []
    if uploaded_files:
        for f in uploaded_files:
            all_records.extend(parse_raw_log(f))
            
    if not all_records:
        return pl.DataFrame(), []
    
    df = pl.DataFrame(all_records)
    
    # Chuẩn hóa cột
    cols = df.columns
    if "Thời gian" not in cols:
        return pl.DataFrame(), []

    # Xử lý thời gian và số liệu
    df = df.with_columns([
        pl.col("Thời gian").str.to_datetime("%Y-%m-%d %H-%M-%S", strict=False).alias("dt"),
        (pl.col("TBEC").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False).fill_null(0) / 100).alias("val_ec"),
        (pl.col("EC yêu cầu").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False).fill_null(0) / 100).alias("ec_target") if "EC yêu cầu" in cols else pl.lit(0).alias("ec_target")
    ])
    
    df = df.filter(pl.col("dt").is_not_null())
    df = df.with_columns(pl.col("Tên khu").map_elements(normalize_area_name, return_dtype=pl.Utf8).alias("norm_name"))
    
    unique_names = df["Tên khu"].unique().to_list()
    filtered_df = df.filter(pl.col("norm_name").str.contains(target_area_normalized))
    
    return filtered_df.sort("dt"), unique_names

# --- GIAO DIỆN ---
st.title("🚜 Đối Soát Dữ Liệu Tưới")

area_input = st.sidebar.text_input("Mã khu vực:", "ANT3")
norm_input = normalize_area_name(area_input)

f_fert = st.sidebar.file_uploader("1. File Châm phân (JSON)", accept_multiple_files=True)
f_drip = st.sidebar.file_uploader("2. File Nhỏ giọt (TXT)", accept_multiple_files=True)

if f_fert and f_drip:
    res_fert, names_fert = load_data(f_fert, norm_input)
    res_drip, names_drip = load_data(f_drip, norm_input)

    if res_fert.is_empty() or res_drip.is_empty():
        st.error(f"⚠️ Không khớp dữ liệu cho mã '{norm_input}'")
        with st.expander("🔍 Danh sách khu vực tìm thấy trong file"):
            st.write("**File Châm phân:**", names_fert)
            st.write("**File Nhỏ giọt:**", names_drip)
            if not names_fert:
                st.warning("Hệ thống không bóc tách được bất kỳ bản ghi nào từ file Châm phân. Hãy kiểm tra lại file có nội dung bên trong không?")
    else:
        # Tính toán và hiển thị
        daily_target = res_fert.filter(pl.col("Trạng thái") == "Bật").group_by(pl.col("dt").dt.date()).agg(pl.col("ec_target").mean().alias("Target"))
        daily_real = res_drip.group_by(pl.col("dt").dt.date()).agg(pl.col("val_ec").mean().alias("Real"))
        
        final = daily_real.join(daily_target, on="dt", how="left").sort("dt")
        st.plotly_chart(px.line(final.to_pandas(), x="dt", y=["Target", "Real"], markers=True))
        st.dataframe(final.to_pandas())
