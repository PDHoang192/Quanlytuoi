import streamlit as st
import polars as pl
import json
import re
import ast
import datetime
import plotly.express as px
import plotly.graph_objects as go

# Cấu hình trang
st.set_page_config(page_title="Hệ Thống Phân Tích Tưới Đa Năng", layout="wide", page_icon="🌱")

# --- HÀM XỬ LÝ DỮ LIỆU ---
@st.cache_data
def parse_log_file_cached(file_content_bytes):
    raw_text = file_content_bytes.decode("utf-8").strip()
    raw_text_clean = re.sub(r'"\s*\n\s*"', '",\n"', raw_text)
    raw_text_clean = re.sub(r',\s*\}', '}', raw_text_clean)
    raw_text_clean = re.sub(r',\s*\]', ']', raw_text_clean)
    raw_text_clean = re.sub(r'\}\s*\{', '},{', raw_text_clean)
    json_text = raw_text_clean if raw_text_clean.startswith('[') else f"[{raw_text_clean}]"
    try:
        return json.loads(json_text)
    except:
        py_text = json_text.replace('true', 'True').replace('false', 'False').replace('null', 'None')
        return ast.literal_eval(py_text)

def process_data(df, gap_limit, min_season_days, start_d, end_d):
    # Lọc theo ngày tùy chọn
    if start_d and end_d:
        df = df.filter((pl.col("dt").dt.date() >= start_d) & (pl.col("dt").dt.date() <= end_d))
        
    if df.is_empty(): return None, "Không có dữ liệu trong khoảng thời gian này."

    df_on = df.filter(pl.col("Trạng thái").str.to_uppercase().str.strip_chars() == "BẬT")
    df_off = df.filter(pl.col("Trạng thái").str.to_uppercase().str.strip_chars() == "TẮT").with_columns(pl.col("dt").alias("dt_end"))

    df_pairs = df_on.join_asof(df_off, on="dt", strategy="forward", suffix="_end")
    df_pairs = df_pairs.with_columns([
        ((pl.col("dt_end") - pl.col("dt")).dt.total_seconds()).alias("duration_s"),
        pl.col("dt").dt.date().alias("Date"),
        pl.coalesce(["TBEC_end", "TBEC"]).alias("val_ec_goc")
    ]).filter((pl.col("duration_s") > 0) & (pl.col("duration_s") < 3600))

    daily = df_pairs.group_by("Date").agg([
        pl.count().alias("turns"),
        (pl.col("duration_s").sum() / 60).round(1).alias("total_time_min"),
        pl.col("val_ec_goc").mean().alias("avg_ec")
    ]).sort("Date")

    # Chia vụ mùa
    daily = daily.with_columns([(pl.col("Date").diff().dt.total_days() > gap_limit).fill_null(False).alias("is_new_season")])
    daily = daily.with_columns(pl.col("is_new_season").cum_sum().alias("s_id"))
    
    seasons = daily.group_by("s_id").agg([
        pl.col("Date").min().alias("Bắt đầu"),
        pl.col("Date").max().alias("Kết thúc"),
        ((pl.col("Date").max() - pl.col("Date").min()).dt.total_days() + 1).alias("Số ngày")
    ]).sort("Bắt đầu")
    
    seasons = seasons.filter(pl.col("Số ngày") >= min_season_days)
    return (df_pairs, seasons, daily), "Thành công"

# --- GIAO DIỆN CHÍNH ---
with st.sidebar:
    st.header("⚙️ Nguồn Dữ Liệu")
    
    # 1. Chỉnh lọc theo STT (không fix cứng mã khu ANT)
    target_stt = st.selectbox("Chọn STT Khu vực:", [1, 2, 3, 4], index=0)
    
    # Set mặc định theo yêu cầu: gap_limit=2, min_days=10
    gap_limit = st.slider("Ngày nghỉ để tách vụ:", 1, 15, 2)
    min_days = st.number_input("Ngày tối thiểu/vụ:", value=10)
    
    uploaded_file = st.file_uploader("1. Tải file Log Tưới (Chính)", type=['txt', 'json'], key="main_log")
    fert_file = st.file_uploader("2. Tải file Log Châm Phân", type=['txt', 'json'], key="fert_log")

if uploaded_file:
    raw_bytes = uploaded_file.getvalue()
    raw_data = parse_log_file_cached(raw_bytes)
    df_raw = pl.DataFrame(raw_data)
    
    # Lọc linh hoạt theo cột STT
    if "STT" in df_raw.columns:
        df_raw = df_raw.filter(pl.col("STT").cast(pl.Utf8).str.contains(str(target_stt)))
    elif "Tên khu" in df_raw.columns:
        # Nếu không có cột STT riêng, tìm con số trong Tên khu
        df_raw = df_raw.filter(pl.col("Tên khu").str.contains(str(target_stt)))
        
    if df_raw.is_empty():
        st.error(f"Không tìm thấy dữ liệu cho STT: {target_stt}")
    else:
        df_raw = df_raw.with_columns([
            pl.col("Thời gian").str.to_datetime("%Y-%m-%d %H-%M-%S", strict=False).alias("dt"),
            pl.col("TBEC").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False) if "TBEC" in df_raw.columns else pl.lit(None)
        ]).drop_nulls(subset=["dt"]).sort("dt")
        
        min_date = df_raw["dt"].min().date()
        max_date = df_raw["dt"].max().date()
        
        st.sidebar.divider()
        st.sidebar.subheader("📅 Bộ lọc thời gian")
        date_mode = st.sidebar.radio("Phạm vi dữ liệu:", ["Toàn bộ", "Tùy chọn khoảng ngày"])
        
        start_date, end_date = min_date, max_date
        if date_mode == "Tùy chọn khoảng ngày":
            selected_dates = st.sidebar.date_input("Chọn Ngày:", [min_date, max_date], min_value=min_date, max_value=max_date)
            if len(selected_dates) == 2:
                start_date, end_date = selected_dates
        
        res, msg = process_data(df_raw, gap_limit, min_days, start_date, end_date)
        
        if res:
            df_p, seasons, daily = res
            
            # --- TÍCH HỢP FILE CHÂM PHÂN ---
            daily = daily.with_columns(pl.lit(None).cast(pl.Float64).alias("avg_req_ec"))
            if fert_file:
                try:
                    fert_data = parse_log_file_cached(fert_file.getvalue())
                    df_fert = pl.DataFrame(fert_data)
                    if "EC yêu cầu" in df_fert.columns:
                        df_fert = df_fert.with_columns([
                            pl.col("Thời gian").str.to_datetime("%Y-%m-%d %H-%M-%S", strict=False).dt.date().alias("Date"),
                            pl.col("EC yêu cầu").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False)
                        ]).drop_nulls(subset=["Date", "EC yêu cầu"])
                        df_fert_daily = df_fert.group_by("Date").agg([pl.col("EC yêu cầu").mean().alias("avg_req_ec_new")])
                        daily = daily.join(df_fert_daily, on="Date", how="left")
                        daily = daily.with_columns(pl.coalesce(["avg_req_ec_new", "avg_req_ec"]).alias("avg_req_ec")).drop("avg_req_ec_new")
                except: pass

            s_dicts = seasons.to_dicts()
            if s_dicts:
                s_options = {f"Vụ {i+1} ({s['Bắt đầu']} -> {s['Kết thúc']})": s for i, s in enumerate(s_dicts)}
                tab1, tab2, tab3 = st.tabs(["📋 Danh Sách Vụ", "📊 Biểu Đồ Tổng Quan", "🧠 Phân Tích Giai Đoạn Đa Biến"])

                with tab1:
                    st.subheader("Thông tin chu kỳ canh tác")
                    display_seasons = []
                    for i, s in enumerate(s_dicts):
                        if i > 0:
                            rest_start = s_dicts[i-1]["Kết thúc"] + datetime.timedelta(days=1)
                            rest_end = s["Bắt đầu"] - datetime.timedelta(days=1)
                            rest_days = (rest_end - rest_start).days + 1
                            if rest_days > 0:
                                display_seasons.append({"Giai đoạn": "⏳ Nghỉ đất", "Bắt đầu": rest_start.strftime('%Y-%m-%d'), "Kết thúc": rest_end.strftime('%Y-%m-%d'), "Số ngày": rest_days})
                        display_seasons.append({"Giai đoạn": f"🌱 Vụ {i+1}", "Bắt đầu": s["Bắt đầu"].strftime('%Y-%m-%d'), "Kết thúc": s["Kết thúc"].strftime('%Y-%m-%d'), "Số ngày": s["Số ngày"]})
                    st.table(display_seasons)

                with tab2:
                    sel_label = st.selectbox("Chọn Vụ:", options=list(s_options.keys()))
                    df_s = daily.filter(pl.col("s_id") == s_options[sel_label]["s_id"]).sort("Date").to_pandas()
                    
                    fig_combo = go.Figure()
                    fig_combo.add_trace(go.Bar(x=df_s["Date"], y=df_s["turns"], name="Số lần tưới", marker_color='#3366CC', yaxis='y1'))
                    fig_combo.add_trace(go.Scatter(x=df_s["Date"], y=df_s["total_time_min"], name="Tổng T.gian (phút)", mode='lines+markers', marker_color='#FF3366', yaxis='y2'))
                    fig_combo.update_layout(title="Số lần tưới và Tổng thời gian tưới", xaxis=dict(dtick="86400000", tickformat="%d-%m"), yaxis=dict(title="Lần"), yaxis2=dict(title="Phút", side='right', overlaying='y', showgrid=False))
                    st.plotly_chart(fig_combo, use_container_width=True)
                    
                    st.plotly_chart(px.line(df_s, x="Date", y="avg_ec", title="TBEC thực tế", markers=True), use_container_width=True)

                with tab3:
                    st.subheader("Thuật toán phân chia giai đoạn Đa biến")
                    param_map = {"Số lần tưới": "turns", "TBEC thực tế": "avg_ec", "EC yêu cầu": "avg_req_ec"}
                    
                    c1, c2 = st.columns(2)
                    with c1:
                        sel_label_3 = st.selectbox("Chọn Vụ (Tab 3):", options=list(s_options.keys()))
                        cols_to_check = st.multiselect("Thông số xét duyệt:", options=list(param_map.keys()), default=["Số lần tưới", "TBEC thực tế"])
                        logic_mode = st.radio("Cơ chế cắt:", ["OR", "AND"])
                    with c2:
                        th_t3 = st.number_input("Ngưỡng: Số lần tưới", value=2.0)
                        th_e3 = st.number_input("Ngưỡng: TBEC", value=30.0)
                        th_req3 = st.number_input("Ngưỡng: EC Yêu cầu", value=12.0)
                        th_map = {"Số lần tưới": th_t3, "TBEC thực tế": th_e3, "EC yêu cầu": th_req3}

                    df_tab3 = daily.filter(pl.col("s_id") == s_options[sel_label_3]["s_id"]).sort("Date")
                    if cols_to_check:
                        req_cols = [param_map[k] for k in cols_to_check]
                        df_tab3_clean = df_tab3.drop_nulls(subset=req_cols)
                        
                        if not df_tab3_clean.is_empty():
                            dates, stage_labels, stages_multi = df_tab3_clean["Date"].to_list(), [], []
                            vals = {k: {"data": df_tab3_clean[param_map[k]].to_list(), "grp": []} for k in cols_to_check}
                            curr_start, idx = dates[0], 1
                            
                            for i in range(len(dates)):
                                conds = []
                                for k in cols_to_check:
                                    if vals[k]["grp"]:
                                        avg_grp = sum(vals[k]["grp"]) / len(vals[k]["grp"])
                                        conds.append(abs(vals[k]["data"][i] - avg_grp) > th_map[k])
                                    else: conds.append(False)
                                
                                cut_stage = any(conds) if logic_mode == "OR" else all(conds)
                                if cut_stage and len(vals[cols_to_check[0]]["grp"]) >= 2:
                                    stages_multi.append({"Giai đoạn": f"GĐ {idx}", "Bắt đầu": curr_start, "Kết thúc": dates[i-1]})
                                    curr_start, idx = dates[i], idx + 1
                                    for k in cols_to_check: vals[k]["grp"] = []
                                
                                for k in cols_to_check: vals[k]["grp"].append(vals[k]["data"][i])
                                stage_labels.append(f"GĐ {idx}")
                            
                            stages_multi.append({"Giai đoạn": f"GĐ {idx}", "Bắt đầu": curr_start, "Kết thúc": dates[-1]})
                            
                            df_plot = df_tab3_clean.to_pandas()
                            df_plot['Giai đoạn'] = stage_labels
                            
                            fig_multi = px.bar(df_plot, x="Date", y=param_map[cols_to_check[0]], color='Giai đoạn', title=f"Phân chia giai đoạn theo {cols_to_check[0]}")
                            fig_multi.update_xaxes(dtick="86400000", tickformat="%d-%m", tickangle=-45)
                            st.plotly_chart(fig_multi, use_container_width=True)

                            st.divider()
                            st.markdown("### 🔎 Chi tiết thông số & Đánh giá trung bình")
                            selected_stage = st.selectbox("Chọn giai đoạn để xem:", [stg["Giai đoạn"] for stg in stages_multi])
                            
                            df_detail = df_plot[df_plot['Giai đoạn'] == selected_stage][['Date', 'turns', 'total_time_min', 'avg_ec', 'avg_req_ec']]
                            df_detail.columns = ['Ngày', 'Số lần tưới', 'Tổng TG (phút)', 'TBEC thực tế', 'EC Yêu cầu']
                            
                            # Tính hàng trung bình
                            avg_row = {
                                'Ngày': '--- TRUNG BÌNH ---',
                                'Số lần tưới': df_detail['Số lần tưới'].mean(),
                                'Tổng TG (phút)': df_detail['Tổng TG (phút)'].mean(),
                                'TBEC thực tế': df_detail['TBEC thực tế'].mean(),
                                'EC Yêu cầu': df_detail['EC Yêu cầu'].mean()
                            }
                            
                            import pandas as pd
                            df_final = pd.concat([df_detail, pd.DataFrame([avg_row])], ignore_index=True)
                            
                            st.dataframe(df_final.style.format({'Số lần tưới': '{:.1f}', 'Tổng TG (phút)': '{:.1f}', 'TBEC thực tế': '{:.2f}', 'EC Yêu cầu': '{:.2f}'}), use_container_width=True, hide_index=True)
        else:
            st.error(msg)
