import streamlit as st
import polars as pl
import json
import re
import ast
import plotly.express as px
from datetime import datetime

# Cấu hình trang
st.set_page_config(page_title="Hệ Thống Quản Lý Tưới", layout="wide", page_icon="🌱")

# --- HÀM XỬ LÝ DỮ LIỆU GỐC ---
def parse_log_file(file_content):
    raw_text = file_content.getvalue().decode("utf-8").strip()
    # Làm sạch các lỗi JSON phổ biến
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

    # Lọc theo khu vực
    df = df.filter(pl.col("Tên khu").str.to_uppercase().str.contains(target_area.upper()))
    if df.is_empty(): 
        return None, f"Không tìm thấy dữ liệu cho {target_area}"

    # Tiền xử lý kiểu dữ liệu
    df = df.with_columns([
        pl.col("Thời gian").str.to_datetime("%Y-%m-%d %H-%M-%S", strict=False).alias("dt"),
        pl.col("TBEC").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False),
        pl.col("TBPH").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False)
    ]).drop_nulls(subset=["dt"]).sort("dt")

    # Xác định các lần tưới (Bật -> Tắt)
    df_on = df.filter(pl.col("Trạng thái").str.to_uppercase().str.strip_chars() == "BẬT")
    df_off = df.filter(pl.col("Trạng thái").str.to_uppercase().str.strip_chars() == "TẮT").with_columns(pl.col("dt").alias("dt_end"))

    df_pairs = df_on.join_asof(df_off, on="dt", strategy="forward", suffix="_end")
    df_pairs = df_pairs.with_columns([
        ((pl.col("dt_end") - pl.col("dt")).dt.total_seconds()).alias("duration_s"),
        pl.col("dt").dt.date().alias("Date"),
        pl.coalesce(["TBEC_end", "TBEC"]).alias("val_ec_goc"),
        pl.coalesce(["TBPH_end", "TBPH"]).alias("val_ph_goc")
    ]).filter((pl.col("duration_s") > 0) & (pl.col("duration_s") < 3600))

    # Tổng hợp theo ngày
    daily = df_pairs.group_by("Date").agg([
        pl.count().alias("turns"),
        pl.col("val_ec_goc").mean().alias("avg_ec"),
        pl.col("val_ph_goc").mean().alias("avg_ph")
    ]).sort("Date")

    # Chia vụ mùa dựa trên khoảng cách ngày (gap_limit)
    daily = daily.with_columns([(pl.col("Date").diff().dt.total_days() > gap_limit).fill_null(False).alias("is_new_season")])
    daily = daily.with_columns(pl.col("is_new_season").cum_sum().alias("s_id"))
    
    seasons = daily.group_by("s_id").agg([
        pl.col("Date").min().alias("Start"),
        pl.col("Date").max().alias("End"),
        ((pl.col("Date").max() - pl.col("Date").min()).dt.total_days() + 1).alias("Days")
    ]).filter(pl.col("Days") >= min_season_days).sort("Start")

    return (df_pairs, seasons, daily), "Thành công"

# --- GIAO DIỆN STREAMLIT ---
st.title("🚜 Hệ Thống Phân Tích Tưới Theo Giai Đoạn")

with st.sidebar:
    st.header("Cấu hình lọc")
    target_area = st.text_input("Mã khu vực:", "ANT-3").upper()
    gap_limit = st.slider("Ngắt vụ nếu nghỉ (ngày):", 1, 10, 2)
    min_days = st.number_input("Số ngày tối thiểu/vụ:", value=5)
    uploaded_file = st.file_uploader("Tải lên file Log (TXT/JSON)", type=['txt', 'json'])

if uploaded_file:
    res, msg = process_data(uploaded_file, target_area, gap_limit, min_days)
    
    if res:
        df_p, seasons, daily = res
        tab1, tab2, tab3 = st.tabs(["📋 Tổng Hợp Vụ", "📊 Biểu Đồ & Giai Đoạn", "📝 Chi Tiết Lần Tưới"])

        # TAB 1: DANH SÁCH VỤ MÙA
        with tab1:
            st.subheader("Các vụ canh tác phát hiện được")
            st.table(seasons.to_dicts())

        # TAB 2: BIỂU ĐỒ VÀ CHIA GIAI ĐOẠN TRỰC QUAN
        with tab2:
            st.subheader("Phân tích xấp xỉ theo Giai đoạn")
            
            # Lấy danh sách vụ để chọn
            s_list = seasons.to_dicts()
            s_options = {f"Vụ {i+1} ({s['Start']} đến {s['End']})": s['s_id'] for i, s in enumerate(s_list)}
            
            c1, c2, c3 = st.columns([2, 1, 1])
            with c1:
                sel_s_label = st.selectbox("Chọn Vụ:", options=list(s_options.keys()))
                sel_s_id = s_options[sel_s_label]
            with c2:
                # Ngưỡng xấp xỉ: Ví dụ lệch 3 lần tưới hoặc 30 đơn vị EC
                threshold = st.number_input("Sai số cho phép (Ngưỡng):", value=3.0, step=0.5)
            with c3:
                crit_col = st.selectbox("Gom nhóm theo:", ["turns", "avg_ec"], 
                                      format_func=lambda x: "Số lần tưới" if x=="turns" else "Chỉ số TBEC")

            # Lọc dữ liệu theo vụ đã chọn
            df_s = daily.filter(pl.col("s_id") == sel_s_id).sort("Date")
            
            # --- THUẬT TOÁN CHIA GIAI ĐOẠN DỰA TRÊN ĐỘ XẤP XỈ ---
            dates = df_s["Date"].to_list()
            values = df_s[crit_col].to_list()
            stages = []
            
            if len(values) > 0:
                current_start = dates[0]
                current_group = [values[0]]
                stg_idx = 1
                
                for i in range(1, len(values)):
                    avg_group = sum(current_group) / len(current_group)
                    # Nếu giá trị ngày hiện tại lệch quá ngưỡng so với trung bình nhóm -> Cắt giai đoạn
                    if abs(values[i] - avg_group) > threshold:
                        stages.append({
                            "name": f"GĐ {stg_idx}", "start": current_start, "end": dates[i-1],
                            "color": "rgba(100, 200, 100, 0.2)" if stg_idx % 2 == 0 else "rgba(200, 100, 100, 0.2)"
                        })
                        current_start = dates[i]
                        current_group = [values[i]]
                        stg_idx += 1
                    else:
                        current_group.append(values[i])
                
                # Giai đoạn cuối
                stages.append({
                    "name": f"GĐ {stg_idx}", "start": current_start, "end": dates[-1],
                    "color": "rgba(100, 200, 100, 0.2)" if stg_idx % 2 == 0 else "rgba(200, 100, 100, 0.2)"
                })

            # VẼ BIỂU ĐỒ
            fig_turns = px.bar(df_s.to_pandas(), x="Date", y="turns", title="Số lần tưới mỗi ngày (Phân đoạn màu)")
            fig_ec = px.line(df_s.to_pandas(), x="Date", y="avg_ec", title="Biến thiên TBEC (Phân đoạn màu)", markers=True)

            # Thêm các vùng màu nền (vrect) để trực quan hóa giai đoạn
            for stg in stages:
                for f in [fig_turns, fig_ec]:
                    f.add_vrect(
                        x0=stg["start"], x1=stg["end"], fillcolor=stg["color"], 
                        opacity=1, layer="below", line_width=0,
                        annotation_text=stg["name"], annotation_position="top left"
                    )

            st.plotly_chart(fig_turns, use_container_width=True)
            st.plotly_chart(fig_ec, use_container_width=True)

            # Bảng tóm tắt giai đoạn
            st.write("### 📋 Tóm tắt các giai đoạn đã chia")
            summary_list = []
            for stg in stages:
                mask = (df_s["Date"] >= stg["start"]) & (df_s["Date"] <= stg["end"])
                df_sub = df_s.filter(mask)
                summary_list.append({
                    "Giai đoạn": stg["name"],
                    "Bắt đầu": stg["start"],
                    "Kết thúc": stg["end"],
                    "Số ngày": len(df_sub),
                    "Lần tưới TB": round(df_sub["turns"].mean(), 1),
                    "EC TB": round(df_sub["avg_ec"].mean(), 2)
                })
            st.dataframe(summary_list, use_container_width=True, hide_index=True)

        # TAB 3: CHI TIẾT
        with tab3:
            st.subheader("Dữ liệu chi tiết từng lần đóng/ngắt")
            st.dataframe(df_p.to_pandas(), use_container_width=True)
    else:
        st.error(msg)
