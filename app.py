import streamlit as st
import polars as pl
import json
import re
import plotly.express as px
from datetime import datetime, timedelta

st.set_page_config(page_title="Hệ Thống Phân Tích Tưới Chuyên Sâu", layout="wide", page_icon="🌱")

# --- HÀM ĐỌC JSON SIÊU BỀN BỈ (BULLETPROOF JSON PARSER) ---
def robust_json_parser(file_content):
    """
    Hàm này sẽ cố gắng đọc JSON bằng nhiều cách: 
    1. Parse toàn bộ (chuẩn)
    2. Parse từng dòng (JSONL)
    3. Tách các object dính liền nhau bằng Regex
    """
    raw_text = file_content.getvalue().decode("utf-8").strip()
    
    # Cách 1: Thử parse toàn bộ file theo chuẩn danh sách [...]
    try:
        data = json.loads(raw_text)
        return data if isinstance(data, list) else [data]
    except:
        pass

    # Cách 2: Parse từng dòng (phổ biến với log IoT)
    data = []
    lines = raw_text.split('\n')
    for line in lines:
        line = line.strip().rstrip(',') # Xóa khoảng trắng và dấu phẩy thừa ở cuối dòng
        if not line: continue
        try:
            data.append(json.loads(line))
        except:
            # Cách 3: Nếu một dòng chứa nhiều object dính nhau {...}{...}
            # Sử dụng Regex để tìm các khối nằm trong cặp ngoặc nhọn
            matches = re.findall(r'\{.*?\}', line)
            for m in matches:
                try:
                    data.append(json.loads(m))
                except:
                    continue
    return data

def load_multiple_files(uploaded_files, target_area=None):
    if not uploaded_files:
        return None
    
    all_records = []
    for file in uploaded_files:
        records = robust_json_parser(file)
        if records:
            all_records.extend(records)
            
    if not all_records:
        return None
    
    # Chuyển thành DataFrame Polars
    df = pl.DataFrame(all_records)
    
    # Chuẩn hóa các cột
    cols = df.columns
    # Tìm cột EC Yêu Cầu (có thể tên khác tùy máy)
    target_ec_col = next((c for c in ["EC_YeuCau", "Yêu Cầu EC", "EC_Setpoint", "SET_EC"] if c in cols), None)

    df = df.with_columns([
        pl.col("Thời gian").str.to_datetime("%Y-%m-%d %H-%M-%S").alias("dt"),
        pl.col("TBEC").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False),
        pl.col("TBPH").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False),
    ])
    
    if target_ec_col:
        df = df.with_columns(
            pl.col(target_ec_col).cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False).alias("ec_target")
        )
    else:
        # Nếu không có cột yêu cầu, lấy trung bình EC để giả lập (hoặc để trống)
        df = df.with_columns(pl.col("TBEC").alias("ec_target"))

    if target_area:
        # Lọc theo khu vực (Case-insensitive)
        df = df.filter(pl.col("Tên khu").str.to_uppercase().str.contains(target_area.upper()))
        
    return df.sort("dt")

# --- GIỮ NGUYÊN LOGIC TÍNH TOÁN VÀ GIAO DIỆN ---
def get_irrigation_events(df):
    df_on = df.filter(pl.col("Trạng thái") == "Bật")
    df_off = df.filter(pl.col("Trạng thái") == "Tắt").select([
        pl.col("dt").alias("dt_end"),
        pl.col("TBEC").alias("ec_end")
    ])
    df_pairs = df_on.join_asof(df_off, left_on="dt", right_on="dt_end", strategy="forward")
    df_pairs = df_pairs.filter(pl.col("dt_end").is_not_null())
    df_pairs = df_pairs.with_columns([
        ((pl.col("dt_end") - pl.col("dt")).dt.total_seconds()).alias("duration_s"),
        pl.col("dt").dt.date().alias("Date")
    ])
    return df_pairs.filter((pl.col("duration_s") >= 10) & (pl.col("duration_s") < 1200))

# --- GIAO DIỆN ---
st.title("🚜 Hệ Thống Phân Tích Tưới Đa Nguồn (Multi-JSON)")

with st.sidebar:
    st.header("Cấu Hình")
    area = st.text_input("Khu vực mục tiêu:", "ANT-2").upper()
    
    st.subheader("1. File Châm Phân (JSON)")
    files_fert = st.file_uploader("Tải log Châm phân", type=['json'], accept_multiple_files=True, key="fert_json")
    
    st.subheader("2. File Nhỏ Giọt (JSON)")
    files_drip = st.file_uploader("Tải log Nhỏ giọt", type=['json'], accept_multiple_files=True, key="drip_json")
    
    gap_limit = st.slider("Số ngày nghỉ để ngắt vụ:", 1, 7, 2)

if files_fert and files_drip:
    with st.spinner('Đang giải mã JSON...'):
        df_fert_raw = load_multiple_files(files_fert)
        df_drip_raw = load_multiple_files(files_drip, target_area=area)

    if df_fert_raw is not None and df_drip_raw is not None:
        # Xử lý Nhỏ giọt (Thực tế)
        df_events = get_irrigation_events(df_drip_raw)
        daily_stats = df_events.group_by("Date").agg([
            pl.count().alias("turns"),
            pl.col("duration_s").mean().round(0).alias("avg_duration"),
            pl.col("TBEC").mean().round(2).alias("avg_ec_real")
        ]).sort("Date")

        # Xử lý Châm phân (Yêu cầu)
        daily_setpoint = df_fert_raw.with_columns(pl.col("dt").dt.date().alias("Date")) \
            .group_by("Date").agg(pl.col("ec_target").median().alias("ec_target")) \
            .sort("Date")

        final_daily = daily_stats.join(daily_setpoint, on="Date", how="left")

        tab1, tab2 = st.tabs(["📅 Quản lý Vụ Mùa", "📈 Biểu đồ EC & Vận hành"])

        with tab1:
            # Tính toán mùa vụ
            daily_stats = daily_stats.with_columns([(pl.col("Date").diff().dt.total_days() > gap_limit).fill_null(False).alias("is_new")])
            daily_stats = daily_stats.with_columns(pl.col("is_new").cum_sum().alias("s_id"))
            
            seasons = daily_stats.group_by("s_id").agg([
                pl.col("Date").min().alias("Start"),
                pl.col("Date").max().alias("End"),
                ((pl.col("Date").max() - pl.col("Date").min()).dt.total_days() + 1).alias("Days")
            ]).sort("Start")
            
            st.table(seasons.to_pandas())

        with tab2:
            st.subheader(f"Đối soát EC Yêu Cầu vs Thực Tế - Khu {area}")
            fig = px.line(final_daily.to_pandas(), x="Date", y=["ec_target", "avg_ec_real"],
                         labels={"value": "EC (mS/cm)", "variable": "Loại EC"},
                         title="Sự sai lệch giữa cài đặt và thực tế khu vực", markers=True)
            st.plotly_chart(fig, use_container_width=True)
            
            st.subheader("Nhật ký vận hành")
            st.dataframe(final_daily.to_pandas(), use_container_width=True)
    else:
        st.error("Không tìm thấy dữ liệu hợp lệ trong các file JSON đã tải lên.")
else:
    st.info("Vui lòng tải các file log định dạng JSON vào các mục tương ứng ở Sidebar.")
