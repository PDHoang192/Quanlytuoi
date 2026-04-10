import streamlit as st
import polars as pl
import json
import re
import ast
import plotly.express as px

# Cấu hình trang
st.set_page_config(page_title="Hệ Thống Phân Tích Tưới", layout="wide", page_icon="🌱")

# --- HÀM XỬ LÝ DỮ LIỆU ---
def parse_log_file(file_content):
    raw_text = file_content.getvalue().decode("utf-8").strip()
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

def process_data(file_content, target_area, gap_limit, min_season_days):
    try:
        data = parse_log_file(file_content)
        df = pl.DataFrame(data)
    except Exception as e:
        return None, f"Lỗi đọc file: {e}"

    if "Tên khu" in df.columns:
        df = df.filter(pl.col("Tên khu").str.to_uppercase().str.contains(target_area.upper()))
        if df.is_empty(): return None, f"Không tìm thấy dữ liệu cho {target_area}"

    df = df.with_columns([
        pl.col("Thời gian").str.to_datetime("%Y-%m-%d %H-%M-%S", strict=False).alias("dt"),
        pl.col("TBEC").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False) if "TBEC" in df.columns else pl.lit(None)
    ]).drop_nulls(subset=["dt"]).sort("dt")

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

def get_stages(df, col, thresh):
    dates, vals = df["Date"].to_list(), df[col].to_list()
    stgs = []
    if not vals: return stgs
    curr_start, curr_grp, idx = dates[0], [vals[0]], 1
    for i in range(1, len(vals)):
        if abs(vals[i] - (sum(curr_grp)/len(curr_grp))) > thresh:
            stgs.append({"n": f"GĐ {idx}", "s": curr_start, "e": dates[i-1], "c": idx})
            curr_start, curr_grp, idx = dates[i], [vals[i]], idx + 1
        else: curr_grp.append(vals[i])
    stgs.append({"n": f"GĐ {idx}", "s": curr_start, "e": dates[-1], "c": idx})
    return stgs

# --- GIAO DIỆN CHÍNH ---
with st.sidebar:
    st.header("⚙️ Nguồn Dữ Liệu")
    target_area = st.text_input("Mã khu vực:", "ANT-3").upper()
    gap_limit = st.slider("Ngày nghỉ để tách vụ:", 1, 15, 3)
    min_days = st.number_input("Ngày tối thiểu/vụ:", value=5)
    uploaded_file = st.file_uploader("1. Tải file Log Tưới (Chính)", type=['txt', 'json'], key="main_log")
    fert_file = st.file_uploader("2. Tải file Log Châm Phân (Để lấy EC Yêu Cầu)", type=['txt', 'json'], key="fert_log")

if uploaded_file:
    res, msg = process_data(uploaded_file, target_area, gap_limit, min_days)
    
    if res:
        df_p, seasons, daily = res
        
        # --- TÍCH HỢP DỮ LIỆU FILE CHÂM PHÂN ---
        daily = daily.with_columns(pl.lit(None).cast(pl.Float64).alias("avg_req_ec"))
        if fert_file:
            try:
                fert_data = parse_log_file(fert_file)
                df_fert = pl.DataFrame(fert_data)
                if "EC yêu cầu" in df_fert.columns:
                    df_fert = df_fert.with_columns([
                        pl.col("Thời gian").str.to_datetime("%Y-%m-%d %H-%M-%S", strict=False).dt.date().alias("Date"),
                        pl.col("EC yêu cầu").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False)
                    ]).drop_nulls(subset=["Date", "EC yêu cầu"])
                    
                    df_fert_daily = df_fert.group_by("Date").agg([pl.col("EC yêu cầu").mean().alias("avg_req_ec_new")])
                    daily = daily.join(df_fert_daily, on="Date", how="left")
                    daily = daily.with_columns(pl.coalesce(["avg_req_ec_new", "avg_req_ec"]).alias("avg_req_ec")).drop("avg_req_ec_new")
                else:
                    st.sidebar.warning("File châm phân không có cột 'EC yêu cầu'.")
            except Exception as e:
                st.sidebar.error(f"Lỗi đọc file châm phân: {e}")

        s_dicts = seasons.to_dicts()
        s_options = {f"Vụ {i+1} ({s['Bắt đầu']} -> {s['Kết thúc']})": s for i, s in enumerate(s_dicts)}
        
        tab1, tab2, tab3 = st.tabs(["📋 Danh Sách Vụ & Nghỉ Đất", "📊 Biểu Đồ Đơn Biến", "🧠 Chia Giai Đoạn Đa Biến"])

        # ==========================================
        # TAB 1: DANH SÁCH VỤ (CÓ XEN KẼ NGHỈ ĐẤT)
        # ==========================================
        with tab1:
            st.subheader("Thông tin chu kỳ canh tác")
            display_seasons = []
            for i, s in enumerate(s_dicts):
                if i > 0:
                    prev_end = s_dicts[i-1]["Kết thúc"]
                    rest_days = (s["Bắt đầu"] - prev_end).days - 1
                    display_seasons.append({
                        "Giai đoạn": "⏳ Nghỉ đất", 
                        "Bắt đầu": "-", 
                        "Kết thúc": "-", 
                        "Số ngày": rest_days if rest_days > 0 else 0
                    })
                display_seasons.append({
                    "Giai đoạn": f"🌱 Vụ {i+1}", 
                    "Bắt đầu": s["Bắt đầu"].strftime('%Y-%m-%d'), 
                    "Kết thúc": s["Kết thúc"].strftime('%Y-%m-%d'), 
                    "Số ngày": s["Số ngày"]
                })
            st.table(display_seasons)

        # ==========================================
        # TAB 2: BIỂU ĐỒ ĐƠN BIẾN
        # ==========================================
        with tab2:
            sel_label = st.selectbox("Chọn Vụ để phân tích biểu đồ:", options=list(s_options.keys()))
            curr_s = s_options[sel_label]
            df_s = daily.filter(pl.col("s_id") == curr_s["s_id"]).sort("Date")

            c_t1, c_t2 = st.columns([3, 1])
            with c_t2: err_turns = st.number_input("Sai số Lần tưới:", value=2.0, step=0.5, key="e_t")
            stgs_t = get_stages(df_s, "turns", err_turns)
            fig_t = px.bar(df_s.to_pandas(), x="Date", y="turns", title="Số lần tưới / ngày", color_discrete_sequence=['#3366CC'])
            for stg in stgs_t:
                fig_t.add_vrect(x0=stg["s"], x1=stg["e"], fillcolor="green" if stg["c"]%2==0 else "red", opacity=0.1, line_width=0, annotation_text=stg["n"])
            c_t1.plotly_chart(fig_t, use_container_width=True)

            st.divider()
            c_e1, c_e2 = st.columns([3, 1])
            with c_e2: err_ec = st.number_input("Sai số TBEC:", value=30.0, step=5.0, key="e_e")
            stgs_e = get_stages(df_s, "avg_ec", err_ec)
            fig_e = px.line(df_s.to_pandas(), x="Date", y="avg_ec", title="TBEC thực tế / ngày", markers=True)
            for stg in stgs_e:
                fig_e.add_vrect(x0=stg["s"], x1=stg["e"], fillcolor="blue" if stg["c"]%2==0 else "orange", opacity=0.1, line_width=0, annotation_text=stg["n"])
            c_e1.plotly_chart(fig_e, use_container_width=True)

            df_s_req = df_s.drop_nulls(subset=["avg_req_ec"])
            if not df_s_req.is_empty():
                st.divider()
                c_r1, c_r2 = st.columns([3, 1])
                with c_r2: err_req_ec = st.number_input("Sai số EC Yêu cầu:", value=12.0, step=2.0, key="e_req")
                stgs_req = get_stages(df_s_req, "avg_req_ec", err_req_ec)
                fig_req = px.line(df_s_req.to_pandas(), x="Date", y="avg_req_ec", title="EC Yêu Cầu Trung Bình / Ngày", markers=True, color_discrete_sequence=['#d62728'])
                for stg in stgs_req:
                    fig_req.add_vrect(x0=stg["s"], x1=stg["e"], fillcolor="pink" if stg["c"]%2==0 else "gray", opacity=0.1, line_width=0, annotation_text=stg["n"])
                c_r1.plotly_chart(fig_req, use_container_width=True)

        # ==========================================
        # TAB 3: CHIA GIAI ĐOẠN ĐA BIẾN (CÓ BIỂU ĐỒ & RULE 2 NGÀY)
        # ==========================================
        with tab3:
            st.subheader("Thuật toán phân chia giai đoạn Đa biến (Tối thiểu 2 ngày)")
            
            param_map = {
                "Số lần tưới": "turns",
                "TBEC thực tế": "avg_ec",
                "EC yêu cầu": "avg_req_ec"
            }

            col_cfg1, col_cfg2 = st.columns(2)
            with col_cfg1:
                sel_label_3 = st.selectbox("Chọn Vụ (Tab 3):", options=list(s_options.keys()), key="s_tab3")
                cols_to_check = st.multiselect("Chọn thông số tham gia xét duyệt:", 
                                               options=list(param_map.keys()),
                                               default=["Số lần tưới", "TBEC thực tế"])
                logic_mode = st.radio("Cơ chế cắt (Logic Gate):", ["OR (Chỉ cần 1 thông số vượt ngưỡng)", "AND (Tất cả phải vượt ngưỡng)"])
            
            with col_cfg2:
                th_t3 = st.number_input("Ngưỡng: Số lần tưới", value=2.0, step=0.5, key="th_t3")
                th_e3 = st.number_input("Ngưỡng: TBEC thực tế", value=30.0, step=5.0, key="th_e3")
                th_req3 = st.number_input("Ngưỡng: EC Yêu cầu", value=12.0, step=2.0, key="th_r3")
                
                th_map = {
                    "Số lần tưới": th_t3,
                    "TBEC thực tế": th_e3,
                    "EC yêu cầu": th_req3
                }

            # --- CHẠY THUẬT TOÁN ---
            df_tab3 = daily.filter(pl.col("s_id") == s_options[sel_label_3]["s_id"]).sort("Date")
            
            if cols_to_check:
                req_cols = [param_map[k] for k in cols_to_check]
                df_tab3_clean = df_tab3.drop_nulls(subset=req_cols)
                
                if df_tab3_clean.is_empty():
                    st.warning("⚠️ Không đủ dữ liệu để xét duyệt (Gợi ý: Cần tải file Châm phân ở Sidebar nếu chọn 'EC yêu cầu').")
                else:
                    dates = df_tab3_clean["Date"].to_list()
                    vals = {k: {"data": df_tab3_clean[param_map[k]].to_list(), "th": th_map[k], "grp": []} for k in cols_to_check}
                    
                    stages_multi = []
                    curr_start = dates[0]
                    idx = 1
                    
                    for k in cols_to_check: vals[k]["grp"] = [vals[k]["data"][0]]
                    
                    for i in range(1, len(dates)):
                        conds = []
                        for k in cols_to_check:
                            avg_grp = sum(vals[k]["grp"]) / len(vals[k]["grp"])
                            conds.append(abs(vals[k]["data"][i] - avg_grp) > vals[k]["th"])
                        
                        cut_stage = any(conds) if logic_mode.startswith("OR") else all(conds)
                        
                        # LUẬT 2 NGÀY: Chỉ cắt nếu giai đoạn ĐÃ TÍCH LŨY được từ 2 ngày trở lên
                        if cut_stage and len(vals[cols_to_check[0]]["grp"]) >= 2:
                            stages_multi.append({
                                "Giai đoạn": f"GĐ {idx}", 
                                "Bắt đầu": curr_start, 
                                "Kết thúc": dates[i-1], 
                                "Số ngày": (dates[i-1]-curr_start).days + 1
                            })
                            curr_start = dates[i]
                            for k in cols_to_check: vals[k]["grp"] = [vals[k]["data"][i]]
                            idx += 1
                        else:
                            # Nếu chưa đủ 2 ngày hoặc không có biến động, tiếp tục gộp dữ liệu
                            for k in cols_to_check: vals[k]["grp"].append(vals[k]["data"][i])
                            
                    stages_multi.append({
                        "Giai đoạn": f"GĐ {idx}", 
                        "Bắt đầu": curr_start, 
                        "Kết thúc": dates[-1], 
                        "Số ngày": (dates[-1]-curr_start).days + 1
                    })
                    
                    st.success(f"Đã chia thành **{len(stages_multi)}** giai đoạn (Đảm bảo tối thiểu 2 ngày/GĐ).")
                    
                    # --- BIỂU ĐỒ TRỰC QUAN GIAI ĐOẠN ĐA BIẾN ---
                    st.divider()
                    fig_multi = px.bar(df_tab3_clean.to_pandas(), x="Date", y=param_map[cols_to_check[0]], 
                                       title=f"Biểu đồ phân chia giai đoạn tổng hợp (Vẽ dựa trên {cols_to_check[0]})", 
                                       color_discrete_sequence=['#8c564b'])
                    
                    for stg in stages_multi:
                        c_idx = int(stg["Giai đoạn"].split(" ")[1])
                        fig_multi.add_vrect(
                            x0=stg["Bắt đầu"], x1=stg["Kết thúc"], 
                            fillcolor="cyan" if c_idx % 2 == 0 else "magenta", 
                            opacity=0.1, line_width=0, annotation_text=stg["Giai đoạn"],
                            annotation_position="top left"
                        )
                    st.plotly_chart(fig_multi, use_container_width=True)

                    # Bảng dữ liệu Giai đoạn Đa biến
                    st.table(stages_multi)
            else:
                st.info("Vui lòng chọn ít nhất 1 thông số để chạy thuật toán.")

    else:
        st.error(msg)
