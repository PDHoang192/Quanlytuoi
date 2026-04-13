import streamlit as st
import polars as pl
import json
import re
import ast
import datetime
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

# Cấu hình trang
st.set_page_config(page_title="Hệ Thống Phân Tích Tưới", layout="wide", page_icon="🌱")

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

def process_data(df, start_d, end_d):
    # Cấu hình mặc định theo yêu cầu
    gap_limit = 2
    min_season_days = 10

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

    # Thuật toán chia vụ
    daily = daily.with_columns([(pl.col("Date").diff().dt.total_days() > gap_limit).fill_null(False).alias("is_new_season")])
    daily = daily.with_columns(pl.col("is_new_season").cum_sum().alias("s_id"))
    
    seasons = daily.group_by("s_id").agg([
        pl.col("Date").min().alias("Bắt đầu"),
        pl.col("Date").max().alias("Kết thúc"),
        ((pl.col("Date").max() - pl.col("Date").min()).dt.total_days() + 1).alias("Số ngày")
    ]).sort("Bắt đầu")
    
    seasons = seasons.filter(pl.col("Số ngày") >= min_season_days)
    return (df_pairs, seasons, daily), "Thành công"

# --- GIAO DIỆN ---
with st.sidebar:
    st.header("⚙️ Cài Đặt Nguồn")
    target_stt = st.selectbox("Chọn STT Khu vực:", [1, 2, 3, 4], index=0)
    uploaded_file = st.file_uploader("1. Log Tưới (Chính)", type=['txt', 'json'])
    fert_file = st.file_uploader("2. Log Châm Phân", type=['txt', 'json'])
    st.info("Mặc định: Nghỉ 2 ngày tách vụ | Vụ tối thiểu 10 ngày.")

if uploaded_file:
    raw_data = parse_log_file_cached(uploaded_file.getvalue())
    df_raw = pl.DataFrame(raw_data)
    
    # Lọc linh hoạt theo STT
    search_key = str(target_stt)
    if "STT" in df_raw.columns:
        df_raw = df_raw.filter(pl.col("STT").cast(pl.Utf8).str.contains(search_key))
    elif "Tên khu" in df_raw.columns:
        df_raw = df_raw.filter(pl.col("Tên khu").str.contains(search_key))
        
    if df_raw.is_empty():
        st.error(f"Không tìm thấy dữ liệu cho STT: {target_stt}")
    else:
        df_raw = df_raw.with_columns([
            pl.col("Thời gian").str.to_datetime("%Y-%m-%d %H-%M-%S", strict=False).alias("dt"),
            pl.col("TBEC").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False) if "TBEC" in df_raw.columns else pl.lit(None)
        ]).drop_nulls(subset=["dt"]).sort("dt")
        
        min_d, max_d = df_raw["dt"].min().date(), df_raw["dt"].max().date()
        
        st.sidebar.divider()
        date_mode = st.sidebar.radio("Khoảng ngày:", ["Toàn bộ", "Tùy chọn"])
        start_date, end_date = min_d, max_d
        if date_mode == "Tùy chọn":
            sel_dates = st.sidebar.date_input("Chọn ngày:", [min_d, max_d], min_value=min_d, max_value=max_d)
            if len(sel_dates) == 2: start_date, end_date = sel_dates
        
        res, msg = process_data(df_raw, start_date, end_date)
        
        if res:
            df_p, seasons, daily = res
            
            # Tích hợp Log Châm Phân
            daily = daily.with_columns(pl.lit(None).cast(pl.Float64).alias("avg_req_ec"))
            if fert_file:
                try:
                    df_f = pl.DataFrame(parse_log_file_cached(fert_file.getvalue()))
                    if "EC yêu cầu" in df_f.columns:
                        df_f = df_f.with_columns([
                            pl.col("Thời gian").str.to_datetime("%Y-%m-%d %H-%M-%S", strict=False).dt.date().alias("Date"),
                            pl.col("EC yêu cầu").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False)
                        ]).drop_nulls(subset=["Date", "EC yêu cầu"])
                        df_f_avg = df_f.group_by("Date").agg([pl.col("EC yêu cầu").mean().alias("avg_req_ec_new")])
                        daily = daily.join(df_f_avg, on="Date", how="left")
                        daily = daily.with_columns(pl.coalesce(["avg_req_ec_new", "avg_req_ec"]).alias("avg_req_ec")).drop("avg_req_ec_new")
                except: pass

            s_dicts = seasons.to_dicts()
            if s_dicts:
                s_opts = {f"Vụ {i+1} ({s['Bắt đầu']} -> {s['Kết thúc']})": s for i, s in enumerate(s_dicts)}
                t1, t2, t3 = st.tabs(["📋 Lịch Trình", "📊 So Sánh Trực Quan", "🧠 Chi Tiết Giai Đoạn"])

                with t1:
                    st.subheader("Chu kỳ canh tác và Nghỉ đất")
                    rows = []
                    for i, s in enumerate(s_dicts):
                        if i > 0:
                            r_s = s_dicts[i-1]["Kết thúc"] + datetime.timedelta(days=1)
                            r_e = s["Bắt đầu"] - datetime.timedelta(days=1)
                            if (r_e - r_s).days >= 0:
                                rows.append({"Đối tượng": "⏳ Nghỉ đất", "Từ": r_s, "Đến": r_e, "Số ngày": (r_e - r_s).days + 1})
                        rows.append({"Đối tượng": f"🌱 Vụ {i+1}", "Từ": s["Bắt đầu"], "Đến": s["Kết thúc"], "Số ngày": s["Số ngày"]})
                    st.table(rows)

                with t2:
                    sel_v = st.selectbox("Chọn Vụ:", options=list(s_opts.keys()), key="v2")
                    df_v = daily.filter(pl.col("s_id") == s_opts[sel_v]["s_id"]).sort("Date").to_pandas()
                    
                    # Biểu đồ 1: Số lần & Thời gian
                    fig1 = go.Figure()
                    fig1.add_trace(go.Bar(x=df_v["Date"], y=df_v["turns"], name="Lần tưới", marker_color='#3366CC', yaxis='y1'))
                    fig1.add_trace(go.Scatter(x=df_v["Date"], y=df_v["total_time_min"], name="Tổng phút", mode='lines+markers', marker_color='#FF3366', yaxis='y2'))
                    fig1.update_layout(title="Tưới: Số lần vs Tổng thời gian", xaxis=dict(dtick="86400000", tickformat="%d-%m"), 
                                      yaxis=dict(title="Lần"), yaxis2=dict(title="Phút", side='right', overlaying='y', showgrid=False), hovermode="x unified")
                    st.plotly_chart(fig1, use_container_width=True)
                    
                    st.divider()
                    # Biểu đồ 2: So sánh EC Thực tế vs EC Yêu cầu (Gộp chung)
                    fig2 = go.Figure()
                    fig2.add_trace(go.Scatter(x=df_v["Date"], y=df_v["avg_ec"], name="EC Thực tế", mode='lines+markers', line=dict(color='#FF9900', width=3)))
                    if "avg_req_ec" in df_v.columns and not df_v["avg_req_ec"].isnull().all():
                        fig2.add_trace(go.Scatter(x=df_v["Date"], y=df_v["avg_req_ec"], name="EC Yêu cầu", mode='lines', line=dict(color='#00CC96', dash='dash')))
                    
                    fig2.update_layout(title="So sánh EC Thực tế vs EC Yêu cầu", xaxis=dict(dtick="86400000", tickformat="%d-%m"), 
                                      yaxis=dict(title="mS/cm"), hovermode="x unified")
                    st.plotly_chart(fig2, use_container_width=True)

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
