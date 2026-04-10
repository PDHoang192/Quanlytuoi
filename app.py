import streamlit as st
import polars as pl
import json
import re
import ast
import plotly.express as px

# Cấu hình trang
st.set_page_config(page_title="Hệ Thống Quản Lý Tưới", layout="wide", page_icon="🌱")

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

    df = df.filter(pl.col("Tên khu").str.to_uppercase().str.contains(target_area.upper()))
    if df.is_empty(): return None, f"Không tìm thấy dữ liệu cho {target_area}"

    df = df.with_columns([
        pl.col("Thời gian").str.to_datetime("%Y-%m-%d %H-%M-%S", strict=False).alias("dt"),
        pl.col("TBEC").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False),
        pl.col("TBPH").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False)
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

# --- GIAO DIỆN ---
with st.sidebar:
    st.header("⚙️ Cấu hình")
    target_area = st.text_input("Khu vực:", "ANT-3").upper()
    gap_limit = st.slider("Ngày nghỉ để tách vụ:", 1, 15, 3)
    min_days = st.number_input("Ngày tối thiểu/vụ:", value=5)
    uploaded_file = st.file_uploader("Tải file Log", type=['txt', 'json'])

if uploaded_file:
    res, msg = process_data(uploaded_file, target_area, gap_limit, min_days)
    
    if res:
        df_p, seasons, daily = res
        tab1, tab2 = st.tabs(["📋 Danh Sách Vụ & Nghỉ Đất", "📊 Biểu Đồ Phân Tích"])

        # TAB 1: QUẢN LÝ VỤ MÙA
        with tab1:
            st.subheader("Thông tin chu kỳ canh tác")
            s_dicts = seasons.to_dicts()
            final_seasons = []
            for i, s in enumerate(s_dicts):
                s["STT"] = i + 1
                if i > 0:
                    prev_end = s_dicts[i-1]["Kết thúc"]
                    rest_days = (s["Bắt đầu"] - prev_end).days
                    s["Nghỉ đất (ngày)"] = rest_days
                else:
                    s["Nghỉ đất (ngày)"] = "-"
                final_seasons.append(s)
            
            st.table(pl.DataFrame(final_seasons).select(["STT", "Bắt đầu", "Kết thúc", "Số ngày", "Nghỉ đất (ngày)"]).to_pandas())

        # TAB 2: BIỂU ĐỒ VỚI SAI SỐ RIÊNG BIỆT
        with tab2:
            s_options = {f"Vụ {i+1} ({s['Bắt đầu']} -> {s['Kết thúc']})": s['s_id'] for i, s in enumerate(s_dicts)}
            sel_label = st.selectbox("Chọn Vụ để xem biểu đồ:", options=list(s_options.keys()))
            df_s = daily.filter(pl.col("s_id") == s_options[sel_label]).sort("Date")

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

            # --- BIỂU ĐỒ 1: SỐ LẦN TƯỚI ---
            st.divider()
            col_t1, col_t2 = st.columns([3, 1])
            with col_t2: 
                err_turns = st.number_input("Sai số Lần tưới:", value=2.0, step=0.5, key="err_t")
            
            stgs_t = get_stages(df_s, "turns", err_turns)
            fig_t = px.bar(df_s.to_pandas(), x="Date", y="turns", title="Biểu đồ Số lần tưới", color_discrete_sequence=['#3366CC'])
            for stg in stgs_t:
                fig_t.add_vrect(x0=stg["s"], x1=stg["e"], fillcolor="green" if stg["c"]%2==0 else "red", 
                                opacity=0.1, layer="below", line_width=0, annotation_text=stg["n"])
            st.plotly_chart(fig_t, use_container_width=True)

            # --- BIỂU ĐỒ 2: TBEC ---
            st.divider()
            col_e1, col_e2 = st.columns([3, 1])
            with col_e2: 
                err_ec = st.number_input("Sai số TBEC:", value=0.2, step=0.05, key="err_e")
            
            stgs_e = get_stages(df_s, "avg_ec", err_ec)
            fig_e = px.line(df_s.to_pandas(), x="Date", y="avg_ec", title="Biểu đồ Chỉ số TBEC", markers=True)
            for stg in stgs_e:
                fig_e.add_vrect(x0=stg["s"], x1=stg["e"], fillcolor="blue" if stg["c"]%2==0 else "orange", 
                                opacity=0.1, layer="below", line_width=0, annotation_text=stg["n"])
            st.plotly_chart(fig_e, use_container_width=True)

    else:
        st.error(msg)
