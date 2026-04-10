import streamlit as st
import polars as pl
import json
import re
import ast
import plotly.express as px
from datetime import datetime, timedelta

st.set_page_config(page_title="Hệ Thống Quản Lý Tưới", layout="wide", page_icon="🌱")

# --- GIỮ NGUYÊN BỘ PARSER CŨ CỦA BẠN ---
def parse_log_file(file_content):
    raw_text = file_content.getvalue().decode("utf-8").strip()
    raw_text_clean = re.sub(r'"\s*\n\s*"', '",\n"', raw_text)
    raw_text_clean = re.sub(r',\s*\}', '}', raw_text_clean)
    raw_text_clean = re.sub(r',\s*\]', ']', raw_text_clean)
    raw_text_clean = re.sub(r'\}\s*\{', '},{', raw_text_clean)
    json_text = raw_text_clean if raw_text_clean.startswith('[') else f"[{raw_text_clean}]"
    
    try:
        return json.loads(json_text)
    except Exception:
        pass
    try:
        py_text = json_text.replace('true', 'True').replace('false', 'False').replace('null', 'None')
        return ast.literal_eval(py_text)
    except Exception:
        pass

    records = []
    chunks = raw_text.split('{') 
    for chunk in chunks:
        if not chunk.strip(): continue
        record = {}
        time_match = re.search(r'"Thời gian"\s*:\s*"?([^",}]+)"?', chunk)
        if time_match: 
            record["Thời gian"] = time_match.group(1).strip('"')
        else: continue
        khu_match = re.search(r'"Tên khu"\s*:\s*"?([^",}]+)"?', chunk)
        if khu_match: record["Tên khu"] = khu_match.group(1).strip('"')
        bon_match = re.search(r'"Tên bồn"\s*:\s*"?([^",}]+)"?', chunk)
        if bon_match: record["Tên bồn"] = bon_match.group(1).strip('"')
        state_match = re.search(r'"Trạng thái"\s*:\s*"?([^",}]+)"?', chunk)
        if state_match: record["Trạng thái"] = state_match.group(1).strip('"')
        ec_req_match = re.search(r'"EC yêu cầu"\s*:\s*"?([^",}\s]+)"?', chunk)
        if ec_req_match: record["EC yêu cầu"] = ec_req_match.group(1)
        tbec_match = re.search(r'"TBEC"\s*:\s*"?([^",}\s]+)"?', chunk)
        if tbec_match: record["TBEC"] = tbec_match.group(1)
        tbph_match = re.search(r'"TBPH"\s*:\s*"?([^",}\s]+)"?', chunk)
        if tbph_match: record["TBPH"] = tbph_match.group(1)
        records.append(record)
            
    if records: return records
    raise Exception("File log bị hỏng quá nặng.")

def process_data(file_content, target_area, gap_limit, min_season_days, date_range):
    try:
        data = parse_log_file(file_content)
        df = pl.DataFrame(data)
    except Exception as e:
        return None, f"Lỗi đọc file: {e}"

    needed_cols = ["Thời gian", "Tên khu", "TBEC", "TBPH", "Trạng thái"]
    df = df.select([c for c in needed_cols if c in df.columns]).filter(
        pl.col("Tên khu").str.to_uppercase().str.contains(target_area.upper())
    )
    
    if df.is_empty(): return None, f"Không thấy dữ liệu khu: {target_area}"

    df = df.with_columns([
        pl.col("Thời gian").str.to_datetime("%Y-%m-%d %H-%M-%S", strict=False).alias("dt"),
        pl.col("TBEC").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False),
        pl.col("TBPH").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False)
    ]).drop_nulls(subset=["dt"]).sort("dt")

    # --- LỌC THEO NGÀY THÁNG NĂM ---
    start_dt = datetime.combine(date_range[0], datetime.min.time())
    end_dt = datetime.combine(date_range[1], datetime.max.time())
    df = df.filter((pl.col("dt") >= start_dt) & (pl.col("dt") <= end_dt))

    if df.is_empty(): return None, "Không có dữ liệu trong khoảng ngày đã chọn."

    df_on = df.filter(pl.col("Trạng thái").str.to_uppercase().str.strip_chars() == "BẬT")
    df_off = df.filter(pl.col("Trạng thái").str.to_uppercase().str.strip_chars() == "TẮT").with_columns(pl.col("dt").alias("dt_end"))

    df_pairs = df_on.join_asof(df_off, on="dt", strategy="forward", suffix="_end")
    df_pairs = df_pairs.filter(pl.col("dt_end").is_not_null())
    df_pairs = df_pairs.with_columns([
        ((pl.col("dt_end") - pl.col("dt")).dt.total_seconds()).alias("duration_s"),
        pl.col("dt").dt.date().alias("Date"),
        pl.coalesce(["TBEC_end", "TBEC"]).alias("val_ec_goc"),
        pl.coalesce(["TBPH_end", "TBPH"]).alias("val_ph_goc")
    ]).filter((pl.col("duration_s") > 0) & (pl.col("duration_s") < 900))

    daily = df_pairs.group_by("Date").agg([
        pl.count().alias("turns"),
        pl.col("val_ec_goc").mean().alias("avg_ec")
    ]).sort("Date")

    daily = daily.with_columns([(pl.col("Date").diff().dt.total_days() > gap_limit).fill_null(False).alias("is_new_season")])
    daily = daily.with_columns(pl.col("is_new_season").cum_sum().alias("s_id"))
    df_pairs = df_pairs.join(daily.select(["Date", "s_id"]), on="Date")

    seasons = daily.group_by("s_id").agg([
        pl.col("Date").min().alias("Start"),
        pl.col("Date").max().alias("End"),
        ((pl.col("Date").max() - pl.col("Date").min()).dt.total_days() + 1).alias("Days")
    ]).filter(pl.col("Days") >= min_season_days).sort("Start")

    return (df_pairs, seasons, daily), "Thành công"

# --- GIAO DIỆN ---
st.title("🚜 Phân Tích Tưới & Giai Đoạn Cây Trồng")

with st.sidebar:
    target_area = st.text_input("Khu vực:", "ANT-2").upper()
    
    # Lấy ngày mặc định (có thể sửa sau khi load file)
    date_range = st.date_input("Lọc khoảng ngày:", [datetime.now() - timedelta(days=30), datetime.now()])
    
    gap_limit = st.slider("Ngắt vụ (ngày):", 1, 10, 2)
    min_days = st.number_input("Ngày tối thiểu/vụ:", value=5)
    uploaded_file = st.file_uploader("Tải file log", type=['txt', 'json'])

if uploaded_file:
    res, msg = process_data(uploaded_file, target_area, gap_limit, min_days, date_range)
    
    if res:
        df_p, seasons, daily = res
        tab1, tab2, tab3 = st.tabs(["📋 Báo cáo Vụ", "📊 Biểu đồ & TBEC", "🌱 Giai đoạn Cây trồng"])

        with tab1:
            st.subheader("Chu kỳ canh tác")
            st.table(seasons.to_dicts())

        with tab2:
            col_a, col_b = st.columns(2)
            # Biểu đồ Turns
            fig1 = px.bar(daily.to_pandas(), x="Date", y="turns", title="Số lần tưới mỗi ngày", color="turns", color_continuous_scale="Viridis")
            col_a.plotly_chart(fig1, use_container_width=True)
            # Biểu đồ EC
            fig2 = px.line(daily.to_pandas(), x="Date", y="avg_ec", title="Biến thiên TBEC", markers=True)
            col_b.plotly_chart(fig2, use_container_width=True)

        with tab3:
            st.subheader("Tự động phân chia Giai đoạn (Sai số 3.0)")
            
            # CHỌN VỤ ĐỂ PHÂN TÍCH
            s_list = seasons.to_dicts()
            s_names = [f"Vụ {i+1} ({s['Start']} -> {s['End']})" for i, s in enumerate(s_list)]
            sel_s_name = st.selectbox("Chọn Vụ:", s_names)
            
            sel_idx = s_names.index(sel_s_name)
            sel_id = s_list[sel_idx]['s_id']
            
            # Logic gộp giai đoạn dựa trên sai số 3.0
            crit = st.radio("Tiêu chí gộp:", ["Số lần tưới", "TBEC"], horizontal=True)
            col_name = "turns" if crit == "Số lần tưới" else "avg_ec"
            tolerance = 3.0 # Theo yêu cầu của bạn
            
            df_stage = daily.filter(pl.col("s_id") == sel_id).sort("Date")
            
            # Thuật toán gom nhóm: Nếu giá trị lệch không quá 3.0 so với ngày trước đó -> Cùng Stage
            df_stage = df_stage.with_columns([
                (pl.col(col_name).diff().abs() > tolerance).fill_null(True).alias("change")
            ])
            df_stage = df_stage.with_columns(pl.col("change").cum_sum().alias("stage_id"))
            
            # Tổng hợp Stage
            stages_summary = df_stage.group_by("stage_id").agg([
                pl.col("Date").min().alias("Bắt đầu"),
                pl.col("Date").max().alias("Kết thúc"),
                pl.col(col_name).mean().round(2).alias("Giá trị TB"),
                pl.count().alias("Số ngày")
            ]).sort("Bắt đầu")

            # Hiển thị bảng Giai đoạn
            st.write("### Danh sách Giai đoạn")
            df_show = stages_summary.to_pandas()
            df_show.insert(0, "Tên Giai Đoạn", [f"Giai đoạn {i+1}" for i in range(len(df_show))])
            
            edited_df = st.data_editor(df_show.drop(columns=["stage_id"]), use_container_width=True, hide_index=True)

            st.divider()
            
            # XEM CHI TIẾT KHI NHẤN CHỌN
            st.write("### 🔍 Xem chi tiết thông số khác")
            sel_stg = st.selectbox("Chọn Giai đoạn để xem thông số chi tiết:", df_show["Tên Giai Đoạn"])
            
            # Lọc dữ liệu gốc của Stage đó
            stg_idx = df_show[df_show["Tên Giai Đoạn"] == sel_stg].index[0]
            stg_info = df_show.iloc[stg_idx]
            
            detail_data = df_p.filter(
                (pl.col("Date") >= stg_info["Bắt đầu"]) & 
                (pl.col("Date") <= stg_info["Kết thúc"])
            )
            
            if not detail_data.is_empty():
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("TB PH", round(detail_data["val_ph_goc"].mean(), 2))
                c2.metric("TB EC", round(detail_data["val_ec_goc"].mean(), 2))
                c3.metric("TG Tưới TB (s)", int(detail_data["duration_s"].mean()))
                c4.metric("Tổng lần tưới", len(detail_data))
                
                st.write("**Nhật ký tưới trong giai đoạn này:**")
                st.dataframe(detail_data.select(["Thời gian", "Trạng thái", "val_ec_goc", "val_ph_goc", "duration_s"]).to_pandas(), use_container_width=True)

    else:
        st.error(msg)
