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

    df_pairs = df_pairs.filter((pl.col("duration_s") >= 15) & (pl.col("duration_s") < 600))

    daily = df_pairs.group_by("Date").agg([
        pl.count().alias("turns"),
        pl.col("duration_s").mean().round(0).alias("avg_duration"),
        pl.col("val_ec_goc").mean().round(2).alias("avg_ec"),
        pl.col("val_ph_goc").mean().round(2).alias("avg_ph")
    ]).sort("Date")

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
    
    st.divider()
    st.markdown("**Cấu hình Phân tích Giai đoạn**")
    # Thêm thanh kéo để tùy chỉnh độ nhạy khi nhận diện giai đoạn qua EC
    ec_threshold = st.slider("Độ lệch EC để nhận diện chuyển giai đoạn:", 0.05, 0.50, 0.15, step=0.05)
    
    uploaded_file = st.file_uploader("Tải file log", type=['txt', 'json'])

if uploaded_file:
    res, msg = process_data(uploaded_file, target_area, gap_limit, min_days)
    
    if res:
        df_p, seasons, daily = res
        tab1, tab2 = st.tabs(["📋 Báo cáo Vụ & Nghỉ", "📈 Phân tích Giai đoạn Sinh trưởng"])

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
            st.subheader(f"Theo dõi dinh dưỡng & Sinh trưởng - Khu {target_area}")
            
            season_dicts = seasons.to_dicts()
            if season_dicts:
                season_options = {}
                for i, s in enumerate(season_dicts):
                    label = f"VỤ MÙA {i+1} ({s['Start'].strftime('%d/%m/%Y')} - {s['End'].strftime('%d/%m/%Y')})"
                    season_options[label] = s["s_id"]
                
                selected_season = st.selectbox("🔍 Chọn Vụ để phân tích:", options=list(season_options.keys()))
                
                if selected_season:
                    sel_id = season_options[selected_season]
                    
                    # Lấy dữ liệu của vụ được chọn
                    df_season = daily.filter(pl.col("s_id") == sel_id).sort("Date")
                    
                    # -- TÍNH TOÁN STT VÀ PHÂN CHIA GIAI ĐOẠN THEO EC --
                    # Tạo cột số thứ tự ngày trong vụ
                    df_season = df_season.with_columns(pl.int_range(1, pl.len() + 1).alias("STT_Ngày"))
                    
                    # Tính độ lệch EC so với ngày hôm trước
                    df_season = df_season.with_columns(
                        pl.col("avg_ec").diff().abs().fill_null(0).alias("ec_diff")
                    )
                    
                    # Đánh dấu Giai đoạn mới nếu độ lệch lớn hơn ngưỡng ec_threshold
                    df_season = df_season.with_columns(
                        (pl.col("ec_diff") >= ec_threshold).cast(pl.Int32).cum_sum().alias("stage_idx")
                    )
                    df_season = df_season.with_columns(
                        (pl.lit("Giai đoạn ") + (pl.col("stage_idx") + 1).cast(pl.Utf8)).alias("Giai_doan")
                    )
                    
                    pd_season = df_season.to_pandas()

                    # -- VẼ BIỂU ĐỒ --
                    st.markdown(f"#### Biểu đồ thay đổi Dinh dưỡng (EC) - Tự động nhận diện giai đoạn")
                    fig = px.line(pd_season, x="STT_Ngày", y="avg_ec", color="Giai_doan", markers=True,
                                  title="Biến thiên Trung bình EC theo ngày tuổi của cây",
                                  labels={"STT_Ngày": "Ngày tuổi (trong vụ)", "avg_ec": "Mức EC trung bình (mS/cm)", "Giai_doan": "Giai đoạn"},
                                  line_shape="spline") # spline giúp đường cong mượt mà hơn
                    
                    fig.update_layout(yaxis_range=[pd_season['avg_ec'].min()*0.8, pd_season['avg_ec'].max()*1.2])
                    st.plotly_chart(fig, use_container_width=True)

                    # -- BẢNG TỔNG HỢP GIAI ĐOẠN --
                    st.markdown("#### Bảng tóm tắt các Giai đoạn (Dựa trên công thức phân)")
                    stage_summary = df_season.group_by("Giai_doan").agg([
                        pl.col("STT_Ngày").min().alias("Ngày bắt đầu"),
                        pl.col("STT_Ngày").max().alias("Ngày kết thúc"),
                        pl.count().alias("Tổng số ngày"),
                        pl.col("avg_ec").mean().round(2).alias("EC trung bình"),
                        pl.col("avg_ph").mean().round(2).alias("pH trung bình")
                    ]).sort("Ngày bắt đầu").to_pandas()
                    
                    st.dataframe(stage_summary, use_container_width=True, hide_index=True)

                    st.divider()

                    # -- BẢNG CHI TIẾT TỪNG NGÀY --
                    with st.expander("Bấm vào đây để xem rà soát chi tiết từng ngày"):
                        display_df = df_season.select([
                            pl.col("STT_Ngày").alias("Ngày tuổi"),
                            pl.col("Date").dt.strftime("%d/%m/%Y").alias("Lịch thực tế"),
                            pl.col("Giai_doan").alias("Giai đoạn"),
                            pl.col("turns").alias("Số lần tưới"),
                            pl.col("avg_duration").alias("TB Thời gian (s)"),
                            pl.col("avg_ec").alias("TBEC"),
                            pl.col("avg_ph").alias("TBPH")
                        ]).to_pandas()
                        
                        st.dataframe(display_df, use_container_width=True, hide_index=True)
            else:
                st.info("Chưa có dữ liệu Vụ Mùa nào đủ điều kiện.")
    else:
        st.error(msg)
