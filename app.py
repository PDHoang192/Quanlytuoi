import streamlit as st
import polars as pl
import json
import plotly.express as px
from datetime import datetime, timedelta

st.set_page_config(page_title="Hệ Thống Quản Lý Tưới", layout="wide", page_icon="🌱")

def process_data(file_content, target_area, gap_limit, min_season_days):
    raw_text = file_content.getvalue().decode("utf-8").strip()
    if not raw_text.startswith('['):
        raw_text = "[" + raw_text.replace('}{', '},{').replace('}\n{', '},{') + "]"
    
    try:
        data = json.loads(raw_text)
        df = pl.DataFrame(data)
    except Exception as e:
        return None, f"Lỗi đọc file: {e}"

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

    # Lọc bỏ tưới < 15 giây (rác) và > 600 giây (lỗi)
    df_pairs = df_pairs.filter((pl.col("duration_s") >= 15) & (pl.col("duration_s") < 600))

    # Nhóm theo ngày và tính Trung Bình Thời gian, TBEC, TBPH
    daily = df_pairs.group_by("Date").agg([
        pl.count().alias("turns"),
        pl.col("duration_s").mean().round(0).alias("avg_duration"),
        pl.col("val_ec_goc").mean().round(2).alias("avg_ec"),
        pl.col("val_ph_goc").mean().round(2).alias("avg_ph")
    ]).sort("Date")

    # Tính toán chia vụ
    daily = daily.with_columns([
        (pl.col("Date").diff().dt.total_days() > gap_limit).fill_null(False).alias("is_new_season")
    ])
    daily = daily.with_columns(pl.col("is_new_season").cum_sum().alias("s_id"))

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
    # ĐÃ SỬA: Cho phép chọn cả đuôi .txt và .json
    uploaded_file = st.file_uploader("Tải file log", type=['txt', 'json'])

if uploaded_file:
    res, msg = process_data(uploaded_file, target_area, gap_limit, min_days)
    
    if res:
        df_p, seasons, daily = res
        tab1, tab2 = st.tabs(["📋 Báo cáo Vụ & Nghỉ", "🔍 Phân tích chi tiết Vụ Mùa"])

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
            st.subheader(f"Thống kê vận hành khu {target_area}")
            
            daily_min_2 = daily.filter(pl.col("turns") >= 2)
            fig = px.bar(daily_min_2.to_pandas(), x="Date", y="turns", 
                         title="Các ngày có tần suất tưới >= 2 lần/ngày",
                         color="turns", color_continuous_scale="Viridis")
            st.plotly_chart(fig, use_container_width=True)
            
            st.divider()
            
            season_dicts = seasons.to_dicts()
            if season_dicts:
                season_options = {}
                for i, s in enumerate(season_dicts):
                    label = f"VỤ MÙA {i+1} ({s['Start'].strftime('%d/%m/%Y')} - {s['End'].strftime('%d/%m/%Y')})"
                    season_options[label] = s["s_id"]
                
                selected_season = st.selectbox("🔍 Chọn Vụ để xem phân tích từng ngày:", options=list(season_options.keys()))
                
                if selected_season:
                    sel_id = season_options[selected_season]
                    
                    season_daily_data = daily.filter(pl.col("s_id") == sel_id).sort("Date")
                    
                    display_df = season_daily_data.select([
                        pl.col("Date").dt.strftime("%d/%m/%Y").alias("Ngày"),
                        pl.col("turns").alias("Số lần tưới"),
                        pl.col("avg_duration").alias("TB Thời gian (giây)"),
                        pl.col("avg_ec").alias("TBEC"),
                        pl.col("avg_ph").alias("TBPH")
                    ]).to_pandas()
                    
                    display_df.insert(0, "STT", range(1, len(display_df) + 1))
                    
                    st.write(f"Phân tích chi tiết **{selected_season}**:")
                    st.dataframe(display_df, use_container_width=True, hide_index=True)
            else:
                st.info("Chưa có dữ liệu Vụ Mùa nào đủ điều kiện.")
    else:
        st.error(msg)
