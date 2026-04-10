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
        st.subheader("🔍 Phân Tích Giai Đoạn Trực Quan")
            
            # --- CẤU HÌNH LỌC ---
            s_list = seasons.to_dicts()
            s_options = {f"Vụ {i+1} ({s['Start']} đến {s['End']})": s['s_id'] for i, s in enumerate(s_list)}
            
            c_sel1, c_sel2, c_sel3 = st.columns([2, 1, 1])
            with c_sel1:
                sel_s_label = st.selectbox("Chọn Vụ:", options=list(s_options.keys()))
                sel_s_id = s_options[sel_s_label]
            
            # Lấy dữ liệu vụ
            df_s = daily.filter(pl.col("s_id") == sel_s_id).sort("Date")
            
            with c_sel2:
                # Ngưỡng để quyết định chia giai đoạn (Ví dụ: lệch 2 lần tưới hoặc 20 đơn vị EC)
                threshold = st.number_input("Ngưỡng chia (Sai số):", value=3.0, step=0.5)
            with c_sel3:
                crit_col = st.selectbox("Chia theo:", ["turns", "avg_ec"], format_func=lambda x: "Số lần tưới" if x=="turns" else "TBEC")

            # --- THUẬT TOÁN CHIA GIAI ĐOẠN TỰ ĐỘNG ---
            dates = df_s["Date"].to_list()
            values = df_s[crit_col].to_list()
            stages = []
            if len(values) > 0:
                current_stage_start = dates[0]
                current_stage_values = [values[0]]
                stage_count = 1
                
                for i in range(1, len(values)):
                    avg_val = sum(current_stage_values) / len(current_stage_values)
                    # Nếu giá trị mới lệch quá ngưỡng so với trung bình giai đoạn hiện tại -> Cắt
                    if abs(values[i] - avg_val) > threshold:
                        stages.append({
                            "name": f"GĐ {stage_count}",
                            "start": current_stage_start,
                            "end": dates[i-1],
                            "color": "rgba(135, 206, 250, 0.3)" if stage_count % 2 == 0 else "rgba(255, 182, 193, 0.3)"
                        })
                        current_stage_start = dates[i]
                        current_stage_values = [values[i]]
                        stage_count += 1
                    else:
                        current_stage_values.append(values[i])
                
                # Add giai đoạn cuối cùng
                stages.append({
                    "name": f"GĐ {stage_count}",
                    "start": current_stage_start,
                    "end": dates[-1],
                    "color": "rgba(135, 206, 250, 0.3)" if stage_count % 2 == 0 else "rgba(255, 182, 193, 0.3)"
                })

            # --- VẼ BIỂU ĐỒ ---
            # Biểu đồ Số lần tưới
            fig1 = px.bar(df_s.to_pandas(), x="Date", y="turns", title=f"Giai đoạn dựa trên {crit_col}",
                          color_discrete_sequence=['#3366CC'])
            
            # Biểu đồ TBEC
            fig2 = px.line(df_s.to_pandas(), x="Date", y="avg_ec", title="Biến thiên TBEC & Các giai đoạn",
                           markers=True, line_shape="spline")

            # Thêm các vùng màu (Vrect) vào cả 2 biểu đồ
            for stg in stages:
                for f in [fig1, fig2]:
                    f.add_vrect(
                        x0=stg["start"], x1=stg["end"],
                        fillcolor=stg["color"], opacity=0.5,
                        layer="below", line_width=0,
                        annotation_text=stg["name"], 
                        annotation_position="top left"
                    )

            st.plotly_chart(fig1, use_container_width=True)
            st.plotly_chart(fig2, use_container_width=True)
            
            # Hiển thị bảng tóm tắt các giai đoạn đã chia
            st.write("**Tóm tắt thông số theo giai đoạn tự động:**")
            summary_data = []
            for stg in stages:
                mask = (df_s["Date"] >= stg["start"]) & (df_s["Date"] <= stg["end"])
                df_sub = df_s.filter(mask)
                summary_data.append({
                    "Giai đoạn": stg["name"],
                    "Từ ngày": stg["start"],
                    "Đến ngày": stg["end"],
                    "Số ngày": len(df_sub),
                    "Lần tưới TB": round(df_sub["turns"].mean(), 1),
                    "EC TB": round(df_sub["avg_ec"].mean(), 2)
                })
            st.table(summary_data)

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
