import streamlit as st
import polars as pl
import json
import re
import ast
import plotly.express as px
from datetime import datetime, timedelta

st.set_page_config(page_title="Hệ Thống Quản Lý Tưới", layout="wide", page_icon="🌱")

# --- GIỮ NGUYÊN BỘ PARSER MẠNH MẼ CỦA BẠN ---
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
        state_match = re.search(r'"Trạng thái"\s*:\s*"?([^",}]+)"?', chunk)
        if state_match: record["Trạng thái"] = state_match.group(1).strip('"')
        ec_req_match = re.search(r'"EC yêu cầu"\s*:\s*"?([^",}\s]+)"?', chunk)
        if ec_req_match: record["EC yêu cầu"] = ec_req_match.group(1)
        tbec_match = re.search(r'"TBEC"\s*:\s*"?([^",}\s]+)"?', chunk)
        if tbec_match: record["TBEC"] = tbec_match.group(1)
        tbph_match = re.search(r'"TBPH"\s*:\s*"?([^",}\s]+)"?', chunk)
        if tbph_match: record["TBPH"] = tbph_match.group(1)
        records.append(record)
    return records

def process_data(file_content, target_area, gap_limit, min_season_days):
    try:
        data = parse_log_file(file_content)
        df = pl.DataFrame(data)
    except Exception as e:
        return None, f"Lỗi đọc file: {e}"

    needed_cols = ["Thời gian", "Tên khu", "TBEC", "TBPH", "Trạng thái"]
    df = df.select([c for c in needed_cols if c in df.columns]).filter(
        pl.col("Tên khu").str.to_uppercase().str.contains(target_area.upper())
    )
    
    if df.is_empty(): return None, f"Không tìm thấy dữ liệu cho {target_area}"

    df = df.with_columns([
        pl.col("Thời gian").str.to_datetime("%Y-%m-%d %H-%M-%S", strict=False).alias("dt"),
        pl.col("TBEC").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False),
        pl.col("TBPH").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False)
    ]).drop_nulls(subset=["dt"]).sort("dt")

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
        pl.col("val_ec_goc").mean().alias("avg_ec"),
        pl.col("val_ph_goc").mean().alias("avg_ph"),
        pl.col("duration_s").mean().alias("avg_dur")
    ]).sort("Date")

    daily = daily.with_columns([(pl.col("Date").diff().dt.total_days() > gap_limit).fill_null(False).alias("is_new_season")])
    daily = daily.with_columns(pl.col("is_new_season").cum_sum().alias("s_id"))
    
    seasons = daily.group_by("s_id").agg([
        pl.col("Date").min().alias("Start"),
        pl.col("Date").max().alias("End"),
        ((pl.col("Date").max() - pl.col("Date").min()).dt.total_days() + 1).alias("Days")
    ]).filter(pl.col("Days") >= min_season_days).sort("Start")

    return (df_pairs, seasons, daily), "Thành công"

# --- GIAO DIỆN CHÍNH ---
st.title("🚜 Hệ Thống Phân Tích Dữ Liệu Tưới")

with st.sidebar:
    target_area = st.text_input("Khu vực (vd: ANT-2):", "ANT-2").upper()
    gap_limit = st.slider("Ngắt vụ (ngày):", 1, 10, 2)
    min_days = st.number_input("Ngày tối thiểu/vụ:", value=5)
    uploaded_file = st.file_uploader("Tải file log", type=['txt', 'json'])

if uploaded_file:
    res, msg = process_data(uploaded_file, target_area, gap_limit, min_days)
    
    if res:
        df_p, seasons, daily = res
        tab1, tab2, tab3 = st.tabs(["📋 Tổng Hợp Vụ Mùa", "🔍 Biểu Đồ & Lọc Ngày", "🌱 Phân Tích Giai Đoạn"])

        # TAB 1: BÁO CÁO TỔNG QUAN
        with tab1:
            st.subheader("Chu kỳ canh tác tổng thể")
            st.table(seasons.to_dicts())

        # TAB 2: LỌC NGÀY VÀ BIỂU ĐỒ THEO VỤ
        with tab2:
            st.subheader("Phân tích chi tiết theo Vụ & Ngày")
            
            # 1. CHỌN VỤ
            s_list = seasons.to_dicts()
            s_options = {f"Vụ {i+1} ({s['Start']} đến {s['End']})": s['s_id'] for i, s in enumerate(s_list)}
            sel_s_label = st.selectbox("Chọn Vụ cần xem:", options=list(s_options.keys()))
            sel_s_id = s_options[sel_s_label]
            
            # Lấy dữ liệu của vụ được chọn
            df_s_daily = daily.filter(pl.col("s_id") == sel_s_id)
            
            # 2. LỌC NGÀY TRONG TAB 2 (Giới hạn trong khoảng của Vụ đó)
            col_d1, col_d2 = st.columns(2)
            min_d_s = df_s_daily["Date"].min()
            max_d_s = df_s_daily["Date"].max()
            
            with col_d1:
                date_range = st.date_input("Lọc khoảng ngày trong vụ:", [min_d_s, max_d_s], min_value=min_d_s, max_value=max_d_s)
            
            if len(date_range) == 2:
                # Lọc dữ liệu theo khoảng ngày đã chọn
                df_final_plot = df_s_daily.filter((pl.col("Date") >= date_range[0]) & (pl.col("Date") <= date_range[1]))
                
                # 3. BIỂU ĐỒ
                c1, c2 = st.columns(2)
                fig_turns = px.bar(df_final_plot.to_pandas(), x="Date", y="turns", title="Số lần tưới mỗi ngày", color="turns", color_continuous_scale="Viridis")
                c1.plotly_chart(fig_turns, use_container_width=True)
                
                fig_ec = px.line(df_final_plot.to_pandas(), x="Date", y="avg_ec", title="Biến thiên TBEC trung bình", markers=True)
                c2.plotly_chart(fig_ec, use_container_width=True)
                
                st.write("**Bảng số liệu chi tiết trong khoảng ngày đã lọc:**")
                st.dataframe(df_final_plot.drop("s_id", "is_new_season").to_pandas(), use_container_width=True, hide_index=True)

        # TAB 3: GIAI ĐOẠN (SAI SỐ 3.0)
        with tab3:
            st.subheader("Tự động phân chia Giai đoạn (Sai số 3.0)")
            sel_s_stg = st.selectbox("Chọn Vụ để chia giai đoạn:", options=list(s_options.keys()), key="stg_s")
            curr_id = s_options[sel_s_stg]
            
            # Logic: Lệch > 3.0 so với ngày trước đó thì qua giai đoạn mới
            crit = st.radio("Dựa trên:", ["Số lần tưới", "TBEC"], horizontal=True)
            val_col = "turns" if crit == "Số lần tưới" else "avg_ec"
            
            df_stg = daily.filter(pl.col("s_id") == curr_id).sort("Date")
            df_stg = df_stg.with_columns([(pl.col(val_col).diff().abs() > 3.0).fill_null(True).alias("is_new")])
            df_stg = df_stg.with_columns(pl.col("is_new").cum_sum().alias("stg_id"))
            
            stg_summary = df_stg.group_by("stg_id").agg([
                pl.col("Date").min().alias("Bắt đầu"),
                pl.col("Date").max().alias("Kết thúc"),
                pl.col(val_col).mean().round(2).alias("Giá trị TB"),
                pl.count().alias("Số ngày")
            ]).sort("Bắt đầu")
            
            df_stg_show = stg_summary.to_pandas()
            df_stg_show.insert(0, "Tên Giai Đoạn", [f"Giai đoạn {i+1}" for i in range(len(df_stg_show))])
            
            # Cho phép sửa tên giai đoạn
            edited_stg = st.data_editor(df_stg_show.drop(columns="stg_id"), use_container_width=True, hide_index=True)
            
            st.divider()
            
            # CHI TIẾT KHI CHỌN GIAI ĐOẠN (THAY CHO DOUBLE CLICK)
            sel_stg_name = st.selectbox("🔍 Chọn Giai đoạn để xem thông số chi tiết:", df_stg_show["Tên Giai Đoạn"])
            stg_row = df_stg_show[df_stg_show["Tên Giai Đoạn"] == sel_stg_name].iloc[0]
            
            # Lấy data gốc của giai đoạn đó
            df_detail = df_p.filter((pl.col("Date") >= stg_row["Bắt đầu"]) & (pl.col("Date") <= stg_row["Kết thúc"]))
            
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("TB PH", round(df_detail["val_ph_goc"].mean(), 2))
            m2.metric("TB EC", round(df_detail["val_ec_goc"].mean(), 2))
            m3.metric("TG Tưới TB (s)", int(df_detail["duration_s"].mean()))
            m4.metric("Tổng lần tưới", len(df_detail))
            
            with st.expander("Xem danh sách chi tiết các lần tưới"):
                st.dataframe(df_detail.select(["Thời gian", "val_ec_goc", "val_ph_goc", "duration_s"]).to_pandas(), use_container_width=True)

    else:
        st.error(msg)
