import streamlit as st
import polars as pl
import json
import re
import ast
import plotly.express as px
from datetime import datetime, timedelta

st.set_page_config(page_title="Hệ Thống Quản Lý Tưới", layout="wide", page_icon="🌱")

# Hàm đọc file JSON chống lỗi (Tự động fix dấu phẩy thừa, thiếu phẩy hoặc nháy đơn)
def parse_log_file(file_content):
    raw_text = file_content.getvalue().decode("utf-8").strip()
    
    # Thử đọc chuẩn trước
    try:
        return json.loads(raw_text)
    except json.JSONDecodeError:
        pass 
        
    # Xử lý bọc mảng nếu thiếu
    if not raw_text.startswith('['):
        raw_text = "[" + raw_text + "]"
        
    # Fix các lỗi định dạng JSON phổ biến bằng Regex
    # 1. Thêm dấu phẩy bị thiếu giữa các object (Lỗi Expecting ',' delimiter)
    raw_text = re.sub(r'\}\s*\{', '},{', raw_text)
    # 2. Xóa dấu phẩy thừa ở cuối mảng/object
    raw_text = re.sub(r',\s*\]', ']', raw_text)
    raw_text = re.sub(r',\s*\}', '}', raw_text)
    
    try:
        return json.loads(raw_text)
    except json.JSONDecodeError as e:
        # Phương án cuối cùng: Đọc từng dòng bằng json.loads hoặc ast.literal_eval
        data = []
        for line in file_content.getvalue().decode("utf-8").strip().split('\n'):
            line = line.strip()
            if not line or line in ('[', ']'): continue
            if line.endswith(','): line = line[:-1]
            try:
                data.append(json.loads(line))
            except:
                try:
                    data.append(ast.literal_eval(line))
                except:
                    pass
        if data:
            return data
        raise Exception(f"Không thể tự động sửa lỗi cấu trúc file. Lỗi gốc: {e}")

def process_data(file_content, target_area, gap_limit, min_season_days):
    try:
        data = parse_log_file(file_content)
        df = pl.DataFrame(data)
    except Exception as e:
        return None, f"Lỗi đọc file log: {e}"

    needed_cols = ["Thời gian", "Tên khu", "TBEC", "TBPH", "Trạng thái"]
    df = df.select(needed_cols).filter(pl.col("Tên khu").str.contains(target_area.upper()))
    
    if df.is_empty():
        return None, f"Không tìm thấy dữ liệu cho khu vực: {target_area}"

    df = df.with_columns([
        pl.col("Thời gian").str.to_datetime("%Y-%m-%d %H-%M-%S").alias("dt"),
        pl.col("TBEC").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False),
        pl.col("TBPH").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False)
    ]).sort("dt")

    df_on = df.filter(pl.col("Trạng thái") == "Bật")
    df_off = df.filter(pl.col("Trạng thái") == "Tắt").with_columns(
        pl.col("dt").alias("dt_end")
    )

    df_pairs = df_on.join_asof(
        df_off,
        on="dt",
        strategy="forward", 
        suffix="_end"
    )

    df_pairs = df_pairs.filter(pl.col("dt_end").is_not_null())

    df_pairs = df_pairs.with_columns([
        ((pl.col("dt_end") - pl.col("dt")).dt.total_seconds()).alias("duration_s"),
        pl.col("dt").dt.date().alias("Date"),
        pl.coalesce(["TBEC_end", "TBEC"]).alias("val_ec_goc"),
        pl.coalesce(["TBPH_end", "TBPH"]).alias("val_ph_goc")
    ])

    df_pairs = df_pairs.filter((pl.col("duration_s") > 0) & (pl.col("duration_s") < 600))

    daily = df_pairs.group_by("Date").agg([
        pl.count().alias("turns")
    ]).sort("Date")

    daily = daily.with_columns([
        (pl.col("Date").diff().dt.total_days() > gap_limit).fill_null(False).alias("is_new_season")
    ])
    daily = daily.with_columns(pl.col("is_new_season").cum_sum().alias("s_id"))

    # Mapping s_id về df_pairs để xài cho việc tra cứu
    df_pairs = df_pairs.join(daily.select(["Date", "s_id"]), on="Date")

    seasons = daily.group_by("s_id").agg([
        pl.col("Date").min().alias("Start"),
        pl.col("Date").max().alias("End"),
        ((pl.col("Date").max() - pl.col("Date").min()).dt.total_days() + 1).alias("Days")
    ]).filter(pl.col("Days") >= min_season_days).sort("Start")

    return (df_pairs, seasons, daily), "Thành công"

# --- GIAO DIỆN ---
st.title("🚜 Nhật Ký Vận Hành & Phân Tích Tưới")

with st.sidebar:
    target_area = st.text_input("Khu vực:", "ANT-2").upper()
    gap_limit = st.slider("Ngắt vụ (ngày):", 1, 10, 2)
    min_days = st.number_input("Ngày tối thiểu/vụ:", value=10)
    uploaded_file = st.file_uploader("Tải file log tưới", type=['txt', 'json'])

if uploaded_file:
    res, msg = process_data(uploaded_file, target_area, gap_limit, min_days)
    
    if res:
        df_p, seasons, daily = res
        tab1, tab2, tab3 = st.tabs(["📋 Báo cáo Vụ & Nghỉ", "🔍 Tra cứu chi tiết đợt tưới", "🧪 Thống kê Châm Phân"])

        with tab1:
            st.subheader("Bảng tổng hợp chu kỳ canh tác")
            s_list = seasons.to_dicts()
            final_report = []
            for i, s in enumerate(s_list):
                final_report.append({
                    "Giai đoạn": f"VỤ MÙA {i+1}", "Bắt đầu": s["Start"], "Kết thúc": s["End"],
                    "Số ngày": s["Days"], "Trạng thái": "Hoàn thành"
                })
                if i < len(s_list) - 1:
                    r_s, r_e = s["End"] + timedelta(days=1), s_list[i+1]["Start"] - timedelta(days=1)
                    final_report.append({
                        "Giai đoạn": "🟢 NGHỈ ĐẤT", "Bắt đầu": r_s, "Kết thúc": r_e,
                        "Số ngày": (r_e - r_s).days + 1, "Trạng thái": "Nghỉ dưỡng"
                    })
            st.table(final_report)

        with tab2:
            st.subheader(f"Thống kê chi tiết từng ngày tưới - Khu {target_area}")
            
            if not seasons.is_empty():
                season_list = seasons.to_dicts()
                season_names = [f"Vụ {i+1} ({s['Start']} đến {s['End']})" for i, s in enumerate(season_list)]
                
                selected_season_name = st.selectbox("Chọn Vụ để xem chi tiết:", options=season_names)
                
                selected_idx = season_names.index(selected_season_name)
                selected_s_id = season_list[selected_idx]['s_id']
                season_start = season_list[selected_idx]['Start']
                
                df_season = df_p.filter(pl.col("s_id") == selected_s_id)
                
                if not df_season.is_empty():
                    daily_stats = df_season.group_by("Date").agg([
                        pl.count().alias("Số lần tưới"),
                        pl.col("duration_s").mean().round(0).alias("Thời gian tưới TB (giây)"),
                        pl.col("val_ec_goc").mean().round(2).alias("TBEC"),
                        pl.col("val_ph_goc").mean().round(2).alias("TBPH")
                    ]).sort("Date")
                    
                    daily_stats = daily_stats.with_columns([
                        ((pl.col("Date") - season_start).dt.total_days() + 1).alias("Ngày thứ")
                    ])
                    
                    # BIỂU ĐỒ SỐ LẦN TƯỚI
                    fig_season_turns = px.bar(
                        daily_stats.to_pandas(), 
                        x="Ngày thứ", 
                        y="Số lần tưới",
                        title=f"Biểu đồ Số lần tưới theo ngày - {selected_season_name}",
                        text="Số lần tưới",
                        color="Số lần tưới",
                        color_continuous_scale="Blues"
                    )
                    fig_season_turns.update_traces(textposition='outside')
                    fig_season_turns.update_layout(xaxis_title="Ngày thứ trong Vụ", yaxis_title="Số lần tưới")
                    st.plotly_chart(fig_season_turns, use_container_width=True)
                    
                    # BẢNG THỐNG KÊ CHI TIẾT
                    daily_stats_display = daily_stats.select([
                        "Ngày thứ", "Date", "Số lần tưới", "Thời gian tưới TB (giây)", "TBEC", "TBPH"
                    ]).rename({"Date": "Ngày thực tế"})
                    
                    st.write("Bảng dữ liệu chi tiết:")
                    st.dataframe(daily_stats_display, use_container_width=True, hide_index=True)
                else:
                    st.info("Không có dữ liệu chi tiết cho vụ này.")
            else:
                st.warning("Chưa có dữ liệu vụ canh tác nào đạt điều kiện.")

        with tab3:
            st.subheader("Phân tích dữ liệu châm phân (EC Yêu Cầu)")
            
            col1, col2 = st.columns(2)
            with col1:
                uploaded_cp_file = st.file_uploader("Tải file châm phân (JSON/TXT)", type=['txt', 'json'], key="cp_upload")
            with col2:
                target_tank = st.text_input("Tìm kiếm bồn:", "BỒN TG-ANT1").upper()

            if uploaded_cp_file:
                try:
                    data_cp = parse_log_file(uploaded_cp_file)
                    df_cp = pl.DataFrame(data_cp)
                    
                    tank_col = "Tên bồn" if "Tên bồn" in df_cp.columns else "Tên khu" if "Tên khu" in df_cp.columns else None
                    
                    if "EC yêu cầu" not in df_cp.columns or "Thời gian" not in df_cp.columns:
                        st.error("File không hợp lệ: Cần có các trường 'Thời gian' và 'EC yêu cầu'.")
                    elif not tank_col:
                        st.error("File không hợp lệ: Không tìm thấy trường 'Tên bồn' hoặc 'Tên khu'.")
                    else:
                        df_cp_filtered = df_cp.filter(pl.col(tank_col).str.contains(target_tank))
                        
                        if df_cp_filtered.is_empty():
                            st.warning(f"Không tìm thấy dữ liệu châm phân cho bồn: {target_tank}")
                        else:
                            df_cp_clean = df_cp_filtered.with_columns([
                                pl.col("Thời gian").str.to_datetime("%Y-%m-%d %H-%M-%S").dt.date().alias("Date"),
                                pl.col("EC yêu cầu").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False)
                            ])
                            
                            df_cp_daily = df_cp_clean.group_by("Date").agg([
                                pl.col("EC yêu cầu").mean().round(2).alias("Trung bình EC yêu cầu")
                            ]).sort("Date")
                            
                            st.success(f"Đã xử lý thành công dữ liệu cho **{target_tank}**")
                            
                            fig_cp = px.line(df_cp_daily.to_pandas(), x="Date", y="Trung bình EC yêu cầu", 
                                             title=f"Biểu đồ EC Yêu cầu trung bình theo ngày - {target_tank}",
                                             markers=True)
                            st.plotly_chart(fig_cp, use_container_width=True)
                            
                            st.write("Bảng thống kê chi tiết:")
                            st.dataframe(df_cp_daily, use_container_width=True, hide_index=True)
                            
                except Exception as e:
                    st.error(f"Lỗi xử lý file châm phân: {e}")
    else:
        st.error(msg)
