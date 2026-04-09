import streamlit as st
import polars as pl
import json
import re
import plotly.express as px
from datetime import datetime, timedelta

st.set_page_config(page_title="Hệ Thống Quản Lý Tưới", layout="wide", page_icon="🌱")

# --- HÀM ĐỌC DỮ LIỆU SẠCH (FIX LỖI U+00A0 VÀ JSON ERROR) ---
def parse_raw_log(file_content):
    try:
        # Giải mã và loại bỏ các ký tự ẩn gây lỗi Syntax/JSON
        raw_text = file_content.getvalue().decode("utf-8")
        raw_text = raw_text.replace('\u00a0', ' ').strip() # Xử lý lỗi U+00A0
        
        data = []
        # Chiến thuật bóc tách từng object để tránh lỗi 'Expecting property name'
        matches = re.findall(r'\{[^{}]*\}', raw_text, re.DOTALL)
        
        for m in matches:
            try:
                # Làm sạch nội dung bên trong object trước khi parse
                clean_m = re.sub(r'\s+', ' ', m)
                data.append(json.loads(clean_m))
            except:
                continue
        return data
    except Exception as e:
        st.error(f"Lỗi đọc file thô: {e}")
        return []

def process_data(uploaded_file, target_area, gap_limit, min_days):
    records = parse_raw_log(uploaded_file)
    if not records:
        return None, "Không thể bóc tách dữ liệu từ file. Vui lòng kiểm tra lại định dạng JSON."

    df = pl.DataFrame(records)
    
    # Kiểm tra cột cần thiết
    needed_cols = ["Thời gian", "Tên khu", "TBEC", "TBPH", "Trạng thái", "EC yêu cầu"]
    for col in needed_cols:
        if col not in df.columns:
            if col == "EC yêu cầu": df = df.with_columns(pl.lit("0").alias("EC yêu cầu"))
            else: return None, f"File thiếu cột quan trọng: {col}"

    # Lọc khu vực (Chuẩn hóa tên để khớp BỒN TG-ANT3 và ANT3)
    df = df.filter(pl.col("Tên khu").str.to_uppercase().str.contains(target_area.upper()))
    
    if df.is_empty():
        return None, f"Không tìm thấy dữ liệu cho khu vực: {target_area}"

    # Chuyển đổi kiểu dữ liệu (Xử lý số liệu chia 100)
    df = df.with_columns([
        pl.col("Thời gian").str.to_datetime("%Y-%m-%d %H-%M-%S", strict=False).alias("dt"),
        (pl.col("TBEC").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False).fill_null(0) / 100).alias("val_ec"),
        (pl.col("EC yêu cầu").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False).fill_null(0) / 100).alias("val_ec_target")
    ]).filter(pl.col("dt").is_not_null()).sort("dt")

    # Tính toán sự kiện Bật/Tắt
    df_on = df.filter(pl.col("Trạng thái") == "Bật")
    df_off = df.filter(pl.col("Trạng thái") == "Tắt").select([
        pl.col("dt").alias("dt_end")
    ])

    if df_on.is_empty() or df_off.is_empty():
        return None, "Dữ liệu không có đủ cặp trạng thái Bật và Tắt."

    df_pairs = df_on.join_asof(df_off, left_on="dt", right_on="dt_end", strategy="forward")
    df_pairs = df_pairs.filter(pl.col("dt_end").is_not_null())
    
    df_pairs = df_pairs.with_columns([
        ((pl.col("dt_end") - pl.col("dt")).dt.total_seconds()).alias("duration_s"),
        pl.col("dt").dt.date().alias("Date")
    ]).filter((pl.col("duration_s") >= 15) & (pl.col("duration_s") < 900))

    # Nhóm theo ngày
    daily = df_pairs.group_by("Date").agg([
        pl.count().alias("turns"),
        pl.col("duration_s").mean().round(0).alias("avg_duration"),
        pl.col("val_ec").mean().round(2).alias("avg_ec_real"),
        pl.col("val_ec_target").mean().round(2).alias("avg_ec_target")
    ]).sort("Date")

    # Chia vụ mùa
    daily = daily.with_columns([
        (pl.col("Date").diff().dt.total_days() > gap_limit).fill_null(False).alias("is_new")
    ])
    daily = daily.with_columns(pl.col("is_new").cum_sum().alias("s_id"))

    seasons = daily.group_by("s_id").agg([
        pl.col("Date").min().alias("Start"),
        pl.col("Date").max().alias("End"),
        ((pl.col("Date").max() - pl.col("Date").min()).dt.total_days() + 1).alias("Days")
    ]).filter(pl.col("Days") >= min_days).sort("Start")

    return (df_pairs, seasons, daily), "Thành công"

# --- GIAO DIỆN ---
with st.sidebar:
    st.header("Cài đặt")
    target_area = st.text_input("Khu vực (vd: ANT3):", "ANT-3")
    gap_limit = st.slider("Số ngày nghỉ ngắt vụ:", 1, 10, 2)
    min_days = st.number_input("Số ngày tối thiểu/vụ:", 5)
    uploaded_file = st.file_uploader("Tải file log (JSON/TXT)", type=['json', 'txt'])

if uploaded_file:
    res, msg = process_data(uploaded_file, target_area, gap_limit, min_days)
    if res:
        df_p, seasons, daily = res
        tab1, tab2 = st.tabs(["📅 Chu Kỳ Vụ Mùa", "📊 Phân Tích EC"])
        
        with tab1:
            st.subheader("Bảng tổng hợp Giai đoạn")
            s_list = seasons.to_dicts()
            report = []
            for i, s in enumerate(s_list):
                report.append({"Giai đoạn": f"VỤ {i+1}", "Bắt đầu": s["Start"], "Kết thúc": s["End"], "Số ngày": s["Days"]})
                if i < len(s_list) - 1:
                   
