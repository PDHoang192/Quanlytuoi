import streamlit as st
import polars as pl
import json
import re
import plotly.express as px
from datetime import datetime, timedelta

st.set_page_config(page_title="Hệ Thống Phân Tích Tưới", layout="wide", page_icon="🌱")

# --- HÀM ĐỌC DỮ LIỆU SIÊU CẤP ---
def parse_raw_log(file_content):
    raw_text = file_content.getvalue().decode("utf-8").strip()
    
    # Thử parse toàn bộ (JSON chuẩn)
    try:
        data = json.loads(raw_text)
        if isinstance(data, list): return data
        return [data]
    except:
        pass

    # Nếu file TXT hoặc JSON lỗi, dùng Regex để bóc tách từng { ... }
    data = []
    matches = re.findall(r'\{.*?\}', raw_text, re.DOTALL)
    for m in matches:
        try:
            # Xử lý các ký tự xuống dòng bên trong object
            clean_m = re.sub(r'\s+', ' ', m) 
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
    
    # --- CHUẨN HÓA CỘT THEO ẢNH BẠN GỬI ---
    # 1. Chuyển đổi thời gian
    if "Thời gian" in df.columns:
        df = df.with_columns(pl.col("Thời gian").str.to_datetime("%Y-%m-%d %H-%M-%S").alias("dt"))
    else:
        return None

    # 2. Xử lý số liệu (Chuyển "120" -> 1.2, "580" -> 5.8)
    # Hàm hỗ trợ convert chuỗi số đặc biệt
    def clean_num(col_name):
        if col_name in df.columns:
            return (pl.col(col_name).cast(pl.Utf8)
                    .str.replace(",", ".")
                    .cast(pl.Float64, strict=False) / 100.0)
        return pl.lit(0.0)

    df = df.with_columns([
        clean_num("TBEC").alias("val_ec"),
        clean_num("TBPH").alias("val_ph"),
        clean_num("EC yêu cầu").alias("ec_target") # Theo đúng ảnh bạn gửi
    ])

    # 3. Lọc theo khu vực (Ví dụ: "ANT-3" sẽ khớp với "BỒN TG-ANT3")
    if target_area:
        df = df.filter(
            pl.col("Tên khu").str.to_uppercase().str.contains(target_area.upper())
        )
        
    return df.sort("dt")

# --- XỬ LÝ SỰ KIỆN TƯỚI ---
def get_irrigation_events(df):
    if df.is_empty(): return pl.DataFrame()
    
    df_on = df.filter(pl.col("Trạng thái") == "Bật")
    df_off = df.filter(pl.col("Trạng thái") == "Tắt").select([
        pl.col("dt").alias("dt_end")
    ])
    
    if df_on.is_empty() or df_off.is_empty(): return pl.DataFrame()

    df_pairs = df_on.join_asof(df_off, left_on="dt", right_on="dt_end", strategy="forward")
    df_pairs = df_pairs.filter(pl.col("dt_end").is_not_null())
    df_pairs = df_pairs.with_columns([
        ((pl.col("dt_end") - pl.col("dt")).dt.total_seconds()).alias("duration_s"),
        pl.col("dt").dt.date().alias("Date")
    ])
    return df_pairs.filter((pl.col("duration_s") >= 10) & (pl.col("duration_s") < 1200))

# --- GIAO DIỆN ---
st.title("🚜 Hệ Thống Đối Soát Tưới & Dinh Dưỡng")

with st.sidebar:
    area_input = st.text_input("Mã khu vực (vd: ANT-3):", "ANT-3").upper()
    st.divider()
    files_fert = st.file_uploader("1. File CHÂM PHÂN (JSON)", type=['json', 'txt'], accept_multiple_files=True)
    files_drip = st.file_uploader("2. File NHỎ GIỌT (TXT)", type=['txt', 'json'], accept_multiple_files=True)
    gap_limit = st.slider("Ngày nghỉ ngắt vụ:", 1, 10, 2)

if files_fert and files_drip:
    df_fert = load_data(files_fert, is_drip=False, target_area=area_input)
    df_drip = load_data(files_drip, is_drip=True, target_area=area_input)

    if df_fert is not None and not df_fert.is_empty() and df_drip is not None:
        # 1. Phân tích EC yêu cầu trung bình mỗi ngày từ file Châm Phân
        # Vì EC yêu cầu có trong mỗi lần 'Bật', ta lấy trung bình theo ngày
        daily_target = df_fert.filter(pl.col("Trạng thái") == "Bật").group_by(
            pl.col("dt").dt.date().alias("Date")
        ).agg(
            pl.col("ec_target").mean().round(2).alias("EC_Yeu_Cau")
        )

        # 2. Phân tích thực tế từ file Nhỏ Giọt
        events = get_irrigation_events(df_drip)
        if not events.is_empty():
            daily_real = events.group_by("Date").agg([
                pl.count().alias("So_Lan_Tuoi"),
                pl.col("duration_s").mean().round(0).alias("TB_Giay"),
                pl.col("val_ec").mean().round(2).alias("EC_Thuc_Te")
            ])

            # 3. Gộp dữ liệu
            final_report = daily_real.join(daily_target, on="Date", how="left").sort("Date")

            # --- HIỂN THỊ ---
            st.subheader(f"📊 Kết quả đối soát Khu {area_input}")
            
            # Biểu đồ so sánh
            fig = px.line(final_report.to_pandas(), x="Date", y=["EC_Yeu_Cau", "EC_Thuc_Te"],
                         markers=True, title="So sánh EC Yêu cầu (Châm phân) vs EC Thực tế (Vòi)")
            st.plotly_chart(fig, use_container_width=True)

            # Bảng chi tiết
            st.write("**Chi tiết vận hành hàng ngày:**")
            st.dataframe(final_report.to_pandas(), use_container_width=True, hide_index=True)
        else:
            st.warning(f"Dữ liệu Nhỏ Giọt của khu {area_input} không có đủ cặp trạng thái Bật/Tắt để phân tích.")
    else:
        st.error(f"Không tìm thấy dữ liệu cho khu vực '{area_input}'. Hãy kiểm tra lại mã khu trong file (ví dụ: {area_input}).")
else:
    st.info("Vui lòng tải lên cả 2 loại file để hệ thống bắt đầu đối soát.")
