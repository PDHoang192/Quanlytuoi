import streamlit as st
import polars as pl
import json
import re
import ast
import datetime
import plotly.express as px
import plotly.graph_objects as go

# Cấu hình trang
st.set_page_config(page_title="Hệ Thống Phân Tích Tưới (Polars Engine)", layout="wide", page_icon="🌱")

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
    gap_limit = 2
    min_season_days = 10

    if start_d and end_d:
        df = df.filter((pl.col("dt").dt.date() >= start_d) & (pl.col("dt").dt.date() <= end_d))
        
    if df.is_empty(): return None, "Không có dữ liệu trong khoảng thời gian này."

    df_on = df.filter(pl.col("Trạng thái").str.to_uppercase().str.strip_chars() == "BẬT")
    df_off = df.filter(pl.col("Trạng thái").str.to_uppercase().str.strip_chars() == "TẮT").with_columns(pl.col("dt").alias("dt_end"))

    # Join tìm cặp BẬT-TẮT
    df_pairs = df_on.join_asof(df_off, on="dt", strategy="forward", suffix="_end")
    df_pairs = df_pairs.with_columns([
        ((pl.col("dt_end") - pl.col("dt")).dt.total_seconds()).alias("duration_s"),
        pl.col("dt").dt.date().alias("Date"),
        pl.coalesce(["TBEC_end", "TBEC"]).alias("val_ec_goc")
    ]).filter((pl.col("duration_s") > 0) & (pl.col("duration_s") < 3600))

    # Tổng hợp theo ngày
    daily = df_pairs.group_by("Date").agg([
        pl.count().alias("turns"),
        (pl.col("duration_s").sum() / 60).round(1).alias("total_time_min"),
        pl.col("val_ec_goc").mean().alias("avg_ec")
    ]).sort("Date")

    # Thuật toán tách vụ
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
    st.header("⚙️ Nguồn Dữ Liệu")
    target_stt = st.selectbox("Chọn STT Khu vực:", [1, 2, 3, 4], index=0)
    uploaded_file = st.file_uploader("1. Log Tưới (Chính)", type=['txt', 'json'])
    fert_file = st.file_uploader("2. Log Châm Phân", type=['txt', 'json'])
    st.info("Mặc định: Tách vụ khi nghỉ 2 ngày | Vụ tối thiểu 10 ngày.")

if uploaded_file:
    raw_data = parse_log_file_cached(uploaded_file.getvalue())
    df_raw = pl.DataFrame(raw_data)
    
    search_key = str(target_stt)
    # Lọc theo STT hoặc Tên khu
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
        date_mode = st.sidebar.radio("Phạm vi dữ liệu:", ["Toàn bộ", "Tùy chọn"])
        start_date, end_date = min_d, max_d
        if date_mode == "Tùy chọn":
            sel_dates = st.sidebar.date_input("Khoảng ngày:", [min_d, max_d], min_value=min_d, max_value=max_d)
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
                t1, t2, t3 = st.tabs(["📋 Danh Sách Vụ", "📊 Biểu Đồ Tổng Quan", "🧠 Phân Tích Giai Đoạn"])

                with t1:
                    st.subheader("Chu kỳ canh tác và Nghỉ đất")
                    rows = []
                    for i, s in enumerate(s_dicts):
                        if i > 0:
                            r_s, r_e = s_dicts[i-1]["Kết thúc"] + datetime.timedelta(days=1), s["Bắt đầu"] - datetime.timedelta(days=1)
                            if (r_e - r_s).days >= 0:
                                rows.append({"Đối tượng": "⏳ Nghỉ đất", "Từ": r_s, "Đến": r_e, "Số ngày": (r_e - r_s).days + 1})
                        rows.append({"Đối tượng": f"🌱 Vụ {i+1}", "Từ": s["Bắt đầu"], "Đến": s["Kết thúc"], "Số ngày": s["Số ngày"]})
                    st.table(rows)

                with t2:
                    sel_v = st.selectbox("Chọn Vụ:", options=list(s_opts.keys()), key="v2")
                    df_v = daily.filter(pl.col("s_id") == s_opts[sel_v]["s_id"]).sort("Date")
                    
                    # Biểu đồ Số lần & Thời gian (Plotly nhận Polars trực tiếp)
                    f1 = go.Figure()
                    f1.add_trace(go.Bar(x=df_v["Date"], y=df_v["turns"], name="Lần tưới", marker_color='#3366CC', yaxis='y1'))
                    f1.add_trace(go.Scatter(x=df_v["Date"], y=df_v["total_time_min"], name="Tổng phút", mode='lines+markers', marker_color='#FF3366', yaxis='y2'))
                    f1.update_layout(title="Số lần tưới vs Tổng thời gian", xaxis=dict(dtick="86400000", tickformat="%d-%m"), 
                                     yaxis=dict(title="Lần"), yaxis2=dict(title="Phút", side='right', overlaying='y', showgrid=False), hovermode="x unified")
                    st.plotly_chart(f1, use_container_width=True)
                    
                    st.divider()
                    # Biểu đồ So sánh EC
                    f2 = go.Figure()
                    f2.add_trace(go.Scatter(x=df_v["Date"], y=df_v["avg_ec"], name="EC Thực tế", mode='lines+markers', line=dict(color='#FF9900', width=3)))
                    if "avg_req_ec" in df_v.columns and not df_v["avg_req_ec"].null_count() == len(df_v):
                        f2.add_trace(go.Scatter(x=df_v["Date"], y=df_v["avg_req_ec"], name="EC Yêu cầu", mode='lines', line=dict(color='#00CC96', dash='dash')))
                    f2.update_layout(title="So sánh EC Thực tế vs EC Yêu cầu", xaxis=dict(dtick="86400000", tickformat="%d-%m"), yaxis=dict(title="mS/cm"), hovermode="x unified")
                    st.plotly_chart(f2, use_container_width=True)

                with t3:
                    st.subheader("Phân tích chi tiết theo giai đoạn tự động")
                    p_map = {"Số lần tưới": "turns", "TBEC thực tế": "avg_ec", "EC yêu cầu": "avg_req_ec"}
                    c1, c2 = st.columns(2)
                    with c1:
                        sel_v3 = st.selectbox("Chọn Vụ:", options=list(s_opts.keys()), key="v3")
                        cols = st.multiselect("Thông số xét duyệt:", options=list(p_map.keys()), default=["Số lần tưới", "TBEC thực tế"])
                        mode = st.radio("Logic cắt:", ["OR", "AND"], horizontal=True)
                    with c2:
                        th_t = st.number_input("Ngưỡng sai số Lần tưới", value=2.0, step=0.5)
                        th_e = st.number_input("Ngưỡng sai số TBEC thực", value=30.0, step=5.0)
                        th_req = st.number_input("Ngưỡng sai số EC yêu cầu", value=10.0, step=2.0)
                        th_map = {"Số lần tưới": th_t, "TBEC thực tế": th_e, "EC yêu cầu": th_req}

                    df_t3 = daily.filter(pl.col("s_id") == s_opts[sel_v3]["s_id"]).sort("Date")
                    if cols:
                        df_clean = df_t3.drop_nulls(subset=[p_map[c] for c in cols])
                        if not df_clean.is_empty():
                            dts, labels, stgs = df_clean["Date"].to_list(), [], []
                            v_data = {c: {"d": df_clean[p_map[c]].to_list(), "g": []} for c in cols}
                            c_s, idx = dts[0], 1
                            
                            for i in range(len(dts)):
                                conds = []
                                for c in cols:
                                    if v_data[c]["g"]:
                                        avg = sum(v_data[c]["g"]) / len(v_data[c]["g"])
                                        conds.append(abs(v_data[c]["d"][i] - avg) > th_map[c])
                                    else: conds.append(False)
                                
                                if (any(conds) if mode == "OR" else all(conds)) and len(v_data[cols[0]]["g"]) >= 2:
                                    stgs.append({"Giai đoạn": f"GĐ {idx}", "Bắt đầu": c_s, "Kết thúc": dts[i-1]})
                                    c_s, idx = dts[i], idx + 1
                                    for c in cols: v_data[c]["g"] = []
                                
                                for c in cols: v_data[c]["g"].append(v_data[c]["d"][i])
                                labels.append(f"GĐ {idx}")
                            
                            stgs.append({"Giai đoạn": f"GĐ {idx}", "Bắt đầu": c_s, "Kết thúc": dts[-1]})
                            df_p3 = df_clean.with_columns(pl.Series("Giai đoạn", labels))
                            
                            st.plotly_chart(px.bar(df_p3, x="Date", y=p_map[cols[0]], color='Giai đoạn', title=f"Phân chia giai đoạn theo {cols[0]}").update_xaxes(dtick="86400000", tickformat="%d-%m"), use_container_width=True)

                            st.divider()
                            st.markdown("### 🔎 Chi tiết thông số giai đoạn")
                            sel_g = st.selectbox("Chọn Giai đoạn:", [s["Giai đoạn"] for s in stgs])
                            
                            # Lọc dữ liệu chi tiết bằng Polars
                            df_det = df_p3.filter(pl.col("Giai đoạn") == sel_g).select([
                                pl.col("Date").cast(pl.Utf8).alias("Ngày"),
                                pl.col("turns").alias("Lần tưới"),
                                pl.col("total_time_min").alias("Phút tưới"),
                                pl.col("avg_ec").alias("EC thực"),
                                pl.col("avg_req_ec").alias("EC yêu cầu")
                            ])

                            # Tính toán hàng trung bình bằng Polars
                            df_avg = df_det.select([
                                pl.lit("--- TRUNG BÌNH ---").alias("Ngày"),
                                pl.col("Lần tưới").mean(),
                                pl.col("Phút tưới").mean(),
                                pl.col("EC thực").mean(),
                                pl.col("EC yêu cầu").mean()
                            ])

                            # Kết hợp dữ liệu và hàng trung bình
                            df_final = pl.concat([df_det, df_avg])
                            
                            # Hiển thị bảng
                            st.dataframe(df_final, use_container_width=True, hide_index=True)
                        else:
                            st.warning("Dữ liệu không đủ để phân tích giai đoạn.")
        else:
            st.error(msg)
