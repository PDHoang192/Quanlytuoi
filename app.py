import streamlit as st
import polars as pl
import json
import re
import plotly.express as px
from datetime import datetime, timedelta

st.set_page_config(page_title="Hệ Thống Phân Tích Tưới Đa Nguồn", layout="wide", page_icon="🚜")

# --- HÀM XỬ LÝ DỮ LIỆU THÔ (JSON & TXT) ---
def parse_raw_log(file_content):
    """
    Hàm xử lý thông minh: 
    - Đọc JSON chuẩn
    - Đọc TXT chứa các object JSON dính liền hoặc xuống dòng
    """
    raw_text = file_content.getvalue().decode("utf-8").strip()
    
    # Thử parse toàn bộ như JSON chuẩn
    try:
        data = json.loads(raw_text)
        if isinstance(data, list): return data
        return [data]
    except:
        pass

    # Nếu thất bại, xử lý theo dạng log thô (TXT)
    data = []
    # Tìm tất cả các khối {...} bằng Regex để tránh lỗi định dạng file TXT
    matches = re.findall(r'\{.*?\}', raw_text, re.DOTALL)
    for m in matches:
        try:
            # Làm sạch chuỗi trước khi parse
            clean_m = m.replace('\n', '').replace('\r', '')
            data.append(json.loads(clean_m))
        except:
            continue
    return data

def load_data(uploaded_files, is_drip=False, target_area=None):
    if not uploaded_files:
        return None
    
    all_records = []
    for f in uploaded_files:
        records = parse_raw_log(f)
        all_records.extend(records)
        
    if not all_records:
        return None
    
    df = pl.DataFrame(all_records)
    cols = df.columns

    # Tiền xử lý dữ liệu số và thời gian
    df = df.with_columns([
        pl.col("Thời gian").str.to_datetime("%Y-%m-%d %H-%M-%S").alias("dt"),
        pl.col("TBEC").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False),
        pl.col("TBPH").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False)
    ])

    # Nếu là file châm phân, tìm cột EC Yêu Cầu
    if not is_drip:
        target_ec_col = next((c for c in ["EC_YeuCau", "Yêu Cầu EC", "EC_Setpoint", "SET_EC"] if c in cols), None)
        if target_ec_col:
            df = df.with_columns(pl.col(target_ec_col).cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False).alias("ec_target"))
        else:
            df = df.with_columns(pl.col("TBEC").alias("ec_target"))
    
    # Nếu là file nhỏ giọt, lọc theo khu vực
    if is_drip and target_area:
        df = df.filter(pl.col("Tên khu").str.to_uppercase().str.contains(target_area.upper()))
        
    return df.sort("dt")

# --- LOGIC GHÉP CẶP BẬT/TẮT ---
def get_events(df):
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
    return df_pairs.filter((pl.col("duration_s") >= 15) & (pl.col("duration_s") < 900))

# --- GIAO DIỆN CHÍNH ---
st.title("🌱 Phân Tích Tưới: Kết Hợp JSON & TXT")

with st.sidebar:
    st.header("Cấu Hình Hệ Thống")
    area_name = st.text_input("Khu vực cần xem:", "ANT-2").upper()
    
    st.divider()
    st.subheader("1. File Châm Phân (JSON)")
    st.info("Dùng để xác định EC Yêu Cầu và chia giai đoạn.")
    files_fert = st.file_uploader("Upload file .json", type=['json'], accept_multiple_files=True)
    
    st.subheader("2. File Nhỏ Giọt (TXT)")
    st.info("Dùng để phân tích lịch tưới thực tế và mùa vụ.")
    files_drip = st.file_uploader("Upload file .txt", type=['txt'], accept_multiple_files=True)
    
    st.divider()
    gap_limit = st.slider("Ngày nghỉ ngắt vụ:", 1, 10, 2)

# --- XỬ LÝ VÀ HIỂN THỊ ---
if files_fert and files_drip:
    with st.spinner('Đang đồng bộ dữ liệu...'):
        df_fert = load_data(files_fert, is_drip=False)
        df_drip = load_data(files_drip, is_drip=True, target_area=area_name)

    if df_fert is not None and df_drip is not None:
        # Xử lý mùa vụ từ file Nhỏ Giọt (TXT)
        events = get_events(df_drip)
        daily_drip = events.group_by("Date").agg([
            pl.count().alias("turns"),
            pl.col("duration_s").mean().round(0).alias("avg_duration"),
            pl.col("TBEC").mean().round(2).alias("avg_ec_real")
        ]).sort("Date")

        # Xác định Vụ/Nghỉ
        daily_drip = daily_drip.with_columns([
            (pl.col("Date").diff().dt.total_days() > gap_limit).fill_null(False).alias("is_new_season")
        ])
        daily_drip = daily_drip.with_columns(pl.col("is_new_season").cum_sum().alias("s_id"))

        # Lấy EC Yêu cầu từ file Châm Phân (JSON)
        daily_fert = df_fert.with_columns(pl.col("dt").dt.date().alias("Date")) \
            .group_by("Date").agg(pl.col("ec_target").median().alias("ec_target")) \
            .sort("Date")

        # Hợp nhất 2 nguồn dữ liệu
        final_df = daily_drip.join(daily_fert, on="Date", how="left")

        # --- TABS ---
        tab_vụ, tab_đối_soát = st.tabs(["📅 Quản lý Vụ Mùa & Nghỉ", "📊 Đối soát EC & Vận hành"])

        with tab_vụ:
            st.subheader(f"Chu kỳ canh tác khu {area_name}")
            seasons = daily_drip.group_by("s_id").agg([
                pl.col("Date").min().alias("Start"),
                pl.col("Date").max().alias("End"),
                ((pl.col("Date").max() - pl.col("Date").min()).dt.total_days() + 1).alias("Days")
            ]).sort("Start")
            
            # Tạo bảng báo cáo Vụ/Nghỉ
            report = []
            s_list = seasons.to_dicts()
            for i, s in enumerate(s_list):
                report.append({"Giai đoạn": f"VỤ {i+1}", "Bắt đầu": s["Start"], "Kết thúc": s["End"], "Số ngày": s["Days"]})
                if i < len(s_list) - 1:
                    gap = (s_list[i+1]["Start"] - s["End"]).days - 1
                    if gap > 0:
                        report.append({"Giai đoạn": "🟢 NGHỈ ĐẤT", "Bắt đầu": s["End"] + timedelta(days=1), 
                                       "Kết thúc": s_list[i+1]["Start"] - timedelta(days=1), "Số ngày": gap})
            st.table(report)

        with tab_đối_soát:
            st.subheader(f"So sánh EC Target (JSON) vs EC Thực tế (TXT) - {area_name}")
            fig = px.line(final_df.to_pandas(), x="Date", y=["ec_target", "avg_ec_real"],
                         labels={"value": "EC (mS/cm)", "variable": "Nguồn dữ liệu"},
                         markers=True, title="Độ chính xác của hệ thống châm phân")
            st.plotly_chart(fig, use_container_width=True)
            
            st.divider()
            st.subheader("Chi tiết vận hành hàng ngày")
            st.dataframe(final_df.select([
                "Date", "ec_target", "avg_ec_real", "turns", "avg_duration"
            ]).to_pandas(), use_container_width=True)
    else:
        st.error("Lỗi: Không thể tìm thấy dữ liệu phù hợp trong các file đã tải.")
else:
    st.info("Vui lòng tải đầy đủ file Châm Phân (.json) và Nhỏ Giọt (.txt) để bắt đầu phân tích.")
