import streamlit as st
import polars as pl
import json
import re
import ast
import plotly.express as px
from datetime import datetime, timedelta

st.set_page_config(page_title="Hệ Thống Quản Lý Tưới", layout="wide", page_icon="🌱")

# --- HÀM ĐỌC FILE (GIỮ NGUYÊN LOGIC SIÊU CƯỜNG CỦA BẠN) ---
def parse_log_file(file_content):
    raw_text = file_content.getvalue().decode("utf-8").strip()
    raw_text_clean = re.sub(r'"\s*\n\s*"', '",\n"', raw_text)
    raw_text_clean = re.sub(r'\}\s*\{', '},{', raw_text_clean)
    json_text = raw_text_clean if raw_text_clean.startswith('[') else f"[{raw_text_clean}]"
    
    try:
        return json.loads(json_text)
    except:
        records = []
        chunks = raw_text.split('{') 
        for chunk in chunks:
            if not chunk.strip(): continue
            record = {}
            time_match = re.search(r'"Thời gian"\s*:\s*"?([^",}]+)"?', chunk)
            if time_match: record["Thời gian"] = time_match.group(1).strip('"')
            else: continue
            khu_match = re.search(r'"Tên khu"\s*:\s*"?([^",}]+)"?', chunk)
            if khu_match: record["Tên khu"] = khu_match.group(1).strip('"')
            tbec_match = re.search(r'"TBEC"\s*:\s*"?([^",}\s]+)"?', chunk)
            if tbec_match: record["TBEC"] = tbec_match.group(1)
            state_match = re.search(r'"Trạng thái"\s*:\s*"?([^",}]+)"?', chunk)
            if state_match: record["Trạng thái"] = state_match.group(1).strip('"')
            records.append(record)
        return records

def process_data(file_content, target_area, gap_limit, min_season_days, date_range):
    try:
        data = parse_log_file(file_content)
        df = pl.DataFrame(data)
    except Exception as e:
        return None, f"Lỗi đọc file log: {e}"

    needed_cols = ["Thời gian", "Tên khu", "TBEC", "Trạng thái"]
    df = df.with_columns([
        pl.col("Thời gian").str.to_datetime("%Y-%m-%d %H-%M-%S", strict=False).alias("dt"),
        pl.col("TBEC").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False).fill_null(0) / 100
    ]).filter(
        (pl.col("Tên khu").str.to_uppercase().str.contains(target_area.upper())) &
        (pl.col("dt").cast(pl.Date) >= date_range[0]) &
        (pl.col("dt").cast(pl.Date) <= date_range[1])
    ).drop_nulls(subset=["dt"]).sort("dt")
    
    if df.is_empty():
        return None, "Không tìm thấy dữ liệu trong khoảng thời gian và khu vực đã chọn."

    # Logic Bật/Tắt
    df_on = df.filter(pl.col("Trạng thái").str.to_uppercase() == "BẬT")
    df_off = df.filter(pl.col("Trạng thái").str.to_uppercase() == "TẮT").select([pl.col("dt").alias("dt_end"), pl.col("TBEC").alias("tbec_end")])
    
    df_pairs = df_on.join_asof(df_off, on="dt", strategy="forward")
    df_pairs = df_pairs.with_columns([
        ((pl.col("dt_end") - pl.col("dt")).dt.total_seconds()).alias("duration_s"),
        pl.col("dt").dt.date().alias("Date"),
        pl.coalesce(["tbec_end", "TBEC"]).alias("val_ec")
    ]).filter((pl.col("duration_s") > 0) & (pl.col("duration_s") < 900))

    # Chia vụ (Seasons)
    daily = df_pairs.group_by("Date").agg([pl.count().alias("turns"), pl.col("val_ec").mean().alias("avg_ec")])
    daily = daily.with_columns([(pl.col("Date").diff().dt.total_days() > gap_limit).fill_null(False).alias("is_new_season")]).sort("Date")
    daily = daily.with_columns(pl.col("is_new_season").cum_sum().alias("s_id"))
    
    seasons = daily.group_by("s_id").agg([
        pl.col("Date").min().alias("Start"),
        pl.col("Date").max().alias("End"),
        ((pl.col("Date").max() - pl.col("Date").min()).dt.total_days() + 1).alias("Days")
    ]).filter(pl.col("Days") >= min_season_days).sort("Start")

    return (df_pairs.join(daily.select(["Date", "s_id"]), on="Date"), seasons, daily), "Thành công"

# --- GIAO DIỆN STREAMLIT ---
st.sidebar.header("⚙️ Cấu Hình Lọc")
uploaded_file = st.sidebar.file_uploader("Tải file log", type=['txt', 'json'])
target_area = st.sidebar.text_input("Khu vực:", "ANT-2")
date_range = st.sidebar.date_input("Khoảng thời gian phân tích:", [datetime(2024,1,1), datetime.now()])
gap_limit = st.sidebar.slider("Ngắt vụ (ngày):", 1, 10, 2)
min_days = st.sidebar.number_input("Ngày tối thiểu/vụ:", value=5)

if uploaded_file and len(date_range) == 2:
    res, msg = process_data(uploaded_file, target_area, gap_limit, min_days, date_range)
    
    if res:
        df_p, seasons, daily = res
        tab1, tab2, tab3 = st.tabs(["📋 Tổng quan Vụ", "📊 Phân tích TBEC & Tưới", "🔬 Chi tiết Giai đoạn"])

        with tab1:
            st.dataframe(seasons, use_container_width=True)

        with tab2:
            # Biểu đồ kết hợp EC và Số lần tưới
            chart_daily = daily.to_pandas()
            fig = px.bar(chart_daily, x="Date", y="turns", title="Tần suất tưới và Biến thiên TBEC", color="avg_ec", color_continuous_scale="RdYlGn_r")
            st.plotly_chart(fig, use_container_width=True)

        with tab3:
            st.subheader("📍 Phân tích Giai đoạn Cây trồng (Sai số gộp: 3.0)")
            
            # Chọn Vụ
            s_list = seasons.to_dicts()
            s_names = [f"Vụ {i+1} ({s['Start']} -> {s['End']})" for i, s in enumerate(s_list)]
            sel_s = st.selectbox("Chọn Vụ:", s_names)
            sel_id = s_list[s_names.index(sel_s)]['s_id']
            
            # Lọc dữ liệu vụ đó
            df_s = daily.filter(pl.col("s_id") == sel_id).sort("Date")
            
            # --- LOGIC GỘP GIAI ĐOẠN THEO SAI SỐ 3.0 ---
            # Hệ thống sẽ gộp nếu |Giá trị ngày hôm nay - Giá trị ngày trước đó| <= 3.0
            tolerance = 3.0
            df_s = df_s.with_columns([
                (pl.col("avg_ec").diff().abs() > tolerance).fill_null(True).alias("change_ec"),
                (pl.col("turns").diff().abs() > tolerance).fill_null(True).alias("change_turns")
            ])
            # Giai đoạn mới hình thành nếu 1 trong 2 chỉ số thay đổi vượt ngưỡng
            df_s = df_s.with_columns((pl.col("change_ec") | pl.col("change_turns")).alias("new_stage"))
            df_s = df_s.with_columns(pl.col("new_stage").cum_sum().alias("stage_id"))
            
            # Gom nhóm Giai đoạn
            stages = df_s.group_by("stage_id").agg([
                pl.col("Date").min().alias("Bắt đầu"),
                pl.col("Date").max().alias("Kết thúc"),
                pl.col("avg_ec").mean().round(2).alias("TBEC Giai đoạn"),
                pl.col("turns").mean().round(1).alias("Lần tưới TB"),
                pl.count().alias("Số ngày")
            ]).sort("Bắt đầu")
            
            # Hiển thị biểu đồ giai đoạn
            fig_stg = px.line(df_s.to_pandas(), x="Date", y="avg_ec", color="stage_id", title="Phân đoạn Giai đoạn dựa trên TBEC (Màu sắc thay đổi khi lệch > 3.0)")
            st.plotly_chart(fig_stg, use_container_width=True)
            
            st.info("💡 **Hướng dẫn:** Nhấn vào dòng trong bảng dưới đây để xem thông số chi tiết của các ngày trong giai đoạn đó.")
            
            # Bảng tương tác
            df_stages_view = stages.to_pandas()
            df_stages_view.insert(0, "Tên Giai Đoạn", [f"Giai đoạn {i+1}" for i in range(len(df_stages_view))])
            
            edited_stages = st.data_editor(df_stages_view, use_container_width=True, hide_index=True, key="stage_editor")
            
            # --- PHẦN SHOW THÔNG SỐ CHI TIẾT KHI CLICK ---
            st.divider()
            selected_stage_id = st.selectbox("🔍 Chọn 'Giai đoạn' để xem chi tiết từng đợt tưới bên dưới:", options=df_stages_view["stage_id"].tolist())
            
            if selected_stage_id:
                st.write(f"### Chi tiết Giai đoạn {selected_stage_id}")
                detail_data = df_p.filter(pl.col("Date").is_in(df_s.filter(pl.col("stage_id") == selected_stage_id)["Date"]))
                
                # Tính toán thêm các chỉ số "khác"
                col_m1, col_m2, col_m3 = st.columns(3)
                col_m1.metric("Tổng số lần tưới", len(detail_data))
                col_m2.metric("Thời gian tưới TB (giây)", round(detail_data["duration_s"].mean(), 0))
                col_m3.metric("TBEC cao nhất", round(detail_data["val_ec"].max(), 2))
                
                st.dataframe(detail_data.select([
                    pl.col("dt").alias("Thời điểm"),
                    pl.col("duration_s").alias("Số giây tưới"),
                    pl.col("val_ec").alias("Chỉ số EC")
                ]).to_pandas(), use_container_width=True)
    else:
        st.error(msg)
