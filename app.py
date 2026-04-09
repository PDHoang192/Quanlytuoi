import streamlit as st
import polars as pl
import json
import re
import ast
import plotly.express as px
from datetime import datetime, timedelta

st.set_page_config(page_title="Hệ Thống Quản Lý Tưới", layout="wide", page_icon="🌱")

# Hàm đọc file siêu cường: Chống mọi lỗi định dạng JSON
def parse_log_file(file_content):
    raw_text = file_content.getvalue().decode("utf-8").strip()
    
    # 1. Thử tiền xử lý và đọc bằng JSON chuẩn
    raw_text_clean = re.sub(r'"\s*\n\s*"', '",\n"', raw_text)
    raw_text_clean = re.sub(r',\s*\}', '}', raw_text_clean)
    raw_text_clean = re.sub(r',\s*\]', ']', raw_text_clean)
    raw_text_clean = re.sub(r'\}\s*\{', '},{', raw_text_clean)
    
    json_text = raw_text_clean if raw_text_clean.startswith('[') else f"[{raw_text_clean}]"
    
    try:
        return json.loads(json_text)
    except Exception:
        pass
        
    # 2. Thử đọc bằng AST 
    try:
        py_text = json_text.replace('true', 'True').replace('false', 'False').replace('null', 'None')
        return ast.literal_eval(py_text)
    except Exception:
        pass

    # 3. Phương án cuối cùng: Quét Regex trích xuất trực tiếp dữ liệu (Đã nâng cấp)
    records = []
    chunks = raw_text.split('{') 
    
    for chunk in chunks:
        if not chunk.strip(): continue
        record = {}
        
        # Cập nhật Regex để bắt được cả giá trị CÓ và KHÔNG CÓ dấu ngoặc kép
        time_match = re.search(r'"Thời gian"\s*:\s*"?([^",}]+)"?', chunk)
        if time_match: 
            record["Thời gian"] = time_match.group(1).strip('"')
        else:
            continue
            
        khu_match = re.search(r'"Tên khu"\s*:\s*"?([^",}]+)"?', chunk)
        if khu_match: record["Tên khu"] = khu_match.group(1).strip('"')
        
        bon_match = re.search(r'"Tên bồn"\s*:\s*"?([^",}]+)"?', chunk)
        if bon_match: record["Tên bồn"] = bon_match.group(1).strip('"')
        
        state_match = re.search(r'"Trạng thái"\s*:\s*"?([^",}]+)"?', chunk)
        if state_match: record["Trạng thái"] = state_match.group(1).strip('"')
        
        # Các thông số số học thường bị mất ngoặc kép trong các bản log cũ
        ec_req_match = re.search(r'"EC yêu cầu"\s*:\s*"?([^",}\s]+)"?', chunk)
        if ec_req_match: record["EC yêu cầu"] = ec_req_match.group(1)
        
        tbec_match = re.search(r'"TBEC"\s*:\s*"?([^",}\s]+)"?', chunk)
        if tbec_match: record["TBEC"] = tbec_match.group(1)
        
        tbph_match = re.search(r'"TBPH"\s*:\s*"?([^",}\s]+)"?', chunk)
        if tbph_match: record["TBPH"] = tbph_match.group(1)
        
        records.append(record)
            
    if records:
        return records
        
    raise Exception("File log bị hỏng cấu trúc quá nặng, không thể trích xuất dữ liệu tự động.")

def process_data(file_content, target_area, gap_limit, min_season_days):
    try:
        data = parse_log_file(file_content)
        df = pl.DataFrame(data)
    except Exception as e:
        return None, f"Lỗi đọc file log: {e}"

    needed_cols = ["Thời gian", "Tên khu", "TBEC", "TBPH", "Trạng thái"]
    missing_cols = [c for c in needed_cols if c not in df.columns]
    if missing_cols:
        return None, f"Dữ liệu thiếu các cột cơ bản: {', '.join(missing_cols)}"
        
    df = df.select(needed_cols).filter(
        pl.col("Tên khu").str.to_uppercase().str.contains(target_area.upper())
    )
    
    if df.is_empty():
        return None, f"Không tìm thấy dữ liệu cho khu vực: {target_area}"

    df = df.with_columns([
        pl.col("Thời gian").str.to_datetime("%Y-%m-%d %H-%M-%S", strict=False).alias("dt"),
        pl.col("TBEC").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False),
        pl.col("TBPH").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False)
    ]).drop_nulls(subset=["dt"]).sort("dt")

    df_on = df.filter(pl.col("Trạng thái").str.to_uppercase().str.strip_chars() == "BẬT")
    df_off = df.filter(pl.col("Trạng thái").str.to_uppercase().str.strip_chars() == "TẮT").with_columns(
        pl.col("dt").alias("dt_end")
    )

    df_pairs = df_on.join_asof(df_off, on="dt", strategy="forward", suffix="_end")
    df_pairs = df_pairs.filter(pl.col("dt_end").is_not_null())

    df_pairs = df_pairs.with_columns([
        ((pl.col("dt_end") - pl.col("dt")).dt.total_seconds()).alias("duration_s"),
        pl.col("dt").dt.date().alias("Date"),
        pl.coalesce(["TBEC_end", "TBEC"]).alias("val_ec_goc"),
        pl.coalesce(["TBPH_end", "TBPH"]).alias("val_ph_goc")
    ])

    df_pairs = df_pairs.filter((pl.col("duration_s") > 0) & (pl.col("duration_s") < 600))

    if df_pairs.is_empty():
        return None, "Không tìm thấy các chu kỳ bật/tắt hợp lệ trong khoảng thời gian cho phép."

    daily = df_pairs.group_by("Date").agg([
        pl.count().alias("turns")
    ]).sort("Date")

    daily = daily.with_columns([
        (pl.col("Date").diff().dt.total_days() > gap_limit).fill_null(False).alias("is_new_season")
    ])
    daily = daily.with_columns(pl.col("is_new_season").cum_sum().alias("s_id"))

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
        
        # FIXED: Thêm tab 4 vào danh sách khởi tạo
        tab1, tab2, tab3, tab4 = st.tabs([
            "📋 Báo cáo Vụ & Nghỉ", 
            "🔍 Tra cứu chi tiết đợt tưới", 
            "🧪 Thống kê Châm Phân", 
            "🌱 Phân tích Giai đoạn Cây trồng"
        ])

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
            
            # PHẦN 1: Biểu đồ (giữ nguyên theo ý bạn)
            daily_min_2 = daily.filter(pl.col("turns") >= 2)
            fig = px.bar(daily_min_2.to_pandas(), x="Date", y="turns", 
                         title="Các ngày có tần suất tưới >= 2 lần/ngày",
                         color="turns", color_continuous_scale="Viridis")
            st.plotly_chart(fig, use_container_width=True)
            
            # --- PHẦN 2: THÊM BIỂU ĐỒ VẼ MẬT ĐỘ TBEC TƯƠNG TỰ BÊN DƯỚI ---
            st.subheader("Biểu đồ Mật độ TBEC Trung bình theo Ngày")
            
            if not df_p.is_empty():
                # Gom nhóm theo Ngày (Date) và tính toán giá trị TBEC
                ec_summary = df_p.group_by("Date").agg([
                    pl.col("val_ec_goc").mean().round(2).alias("TBEC")
                ]).sort("Date")
                
                # Vẽ biểu đồ cột tương tự biểu đồ turns, sử dụng thang màu Viridis cho TBEC
                fig_ec = px.bar(ec_summary.to_pandas(), x="Date", y="TBEC",
                                 title="Biểu đồ phân bố TBEC trung bình",
                                 color="TBEC", color_continuous_scale="Viridis")
                st.plotly_chart(fig_ec, use_container_width=True)
            else:
                st.info("Không có dữ liệu EC để hiển thị biểu đồ.")
                
            st.divider()
            
            # PHẦN 3: Bảng số liệu chi tiết gom nhóm theo từng ngày
            st.subheader("Bảng số liệu tổng hợp từng ngày")
            
            if not df_p.is_empty():
                # Gom nhóm theo Ngày (Date) và tính toán các giá trị trung bình/tổng
                daily_summary = df_p.group_by("Date").agg([
                    pl.count().alias("Số lần tưới"),
                    pl.col("duration_s").mean().round(0).alias("Thời gian tưới TB (giây)"),
                    pl.col("val_ec_goc").mean().round(2).alias("TBEC")
                ]).sort("Date") # Sắp xếp thứ tự thời gian từ ngày đầu tiên đến ngày cuối cùng
                
                st.dataframe(daily_summary, use_container_width=True, hide_index=True)
            else:
                st.info("Không có dữ liệu để hiển thị bảng chi tiết.")

        # FIXED: Định dạng lại khoảng trắng (Space) chỗ khối Try - Except của Tab 3
        with tab3:
            st.subheader("Phân tích dữ liệu châm phân (EC Yêu Cầu)")
            
            col1, col2 = st.columns(2)
            with col1:
                uploaded_cp_file = st.file_uploader("Tải file châm phân (JSON/TXT)", type=['txt', 'json'], key="cp_upload")
            with col2:
                target_tank = st.text_input("Tìm kiếm bồn:", "BỒN TG-ANT1").upper()

            if uploaded_cp_file:
                try:
                    # 1. ĐỌC THẲNG FILE DƯỚI DẠNG TEXT (BỎ QUA MỌI LỖI JSON BỊ THIẾU DẤU PHẨY/NGOẶC)
                    content = uploaded_cp_file.getvalue().decode('utf-8', errors='ignore')
                    
                    daily_stats = {}
                    
                    # Cắt file thành từng dòng để quét
                    lines = content.split('\n')
                    
                    for line in lines:
                        # Lọc: Chỉ xử lý nếu dòng có chứa tên bồn đang tìm kiếm
                        if target_tank not in line.upper():
                            continue
                            
                        # 2. Dùng Regex "bới" lấy Ngày tháng (chỉ lấy YYYY-MM-DD)
                        date_match = re.search(r'["\']Thời gian["\']\s*:\s*["\'](\d{4}[-/]\d{2}[-/]\d{2})', line, re.IGNORECASE)
                        if not date_match:
                            continue
                        date_str = date_match.group(1).replace("/", "-") # Đưa về chuẩn YYYY-MM-DD
                        
                        # 3. Dùng Regex "bới" lấy con số EC yêu cầu
                        ec_match = re.search(r'["\']EC yêu cầu["\']\s*:\s*["\']?(\d+[.,]?\d*)', line, re.IGNORECASE)
                        if not ec_match:
                            continue
                            
                        # Đổi dấu phẩy thành chấm và chia 100 theo logic cũ
                        ec_val = float(ec_match.group(1).replace(",", ".")) / 100
                        
                        # 4. Cộng dồn vào từ điển theo ngày
                        if date_str not in daily_stats:
                            daily_stats[date_str] = {"sum": 0.0, "count": 0}
                        
                        daily_stats[date_str]["sum"] += ec_val
                        daily_stats[date_str]["count"] += 1

                    if not daily_stats:
                        st.warning(f"Không tìm thấy dữ liệu hợp lệ cho {target_tank}")
                    else:
                        import pandas as pd
                        import plotly.express as px
                        
                        # 5. Tính trung bình và tạo bảng
                        plot_data = []
                        for d, vals in daily_stats.items():
                            plot_data.append({
                                "Ngày": d,
                                "Trung bình EC yêu cầu": round(vals["sum"] / vals["count"], 2)
                            })
                        
                        df_plot = pd.DataFrame(plot_data)
                        df_plot["Date"] = pd.to_datetime(df_plot["Ngày"], errors='coerce')
                        df_plot = df_plot.dropna(subset=["Date"]).sort_values("Date")

                        st.success(f"Đã quét thành công toàn bộ chu kỳ cho **{target_tank}** (Bỏ qua hoàn toàn lỗi file ở dòng 2663)!")
                        
                        # Vẽ biểu đồ
                        fig_cp = px.line(
                            df_plot, 
                            x="Date", 
                            y="Trung bình EC yêu cầu", 
                            title=f"Biểu đồ mức EC mục tiêu trung bình theo ngày - {target_tank}",
                            markers=True
                        )
                        fig_cp.update_layout(xaxis_title="Thời gian (Ngày)", yaxis_title="Mức EC Yêu cầu (mS/cm)")
                        st.plotly_chart(fig_cp, use_container_width=True)
                        
                        # Bảng chi tiết
                        st.write("Bảng thống kê chi tiết toàn bộ chu kỳ:")
                        st.dataframe(df_plot[["Ngày", "Trung bình EC yêu cầu"]], use_container_width=True, hide_index=True)

                except Exception as e:
                    st.error(f"Lỗi xử lý hệ thống: {e}")

        # FIXED: Chỉnh lại thụt lề tab 4 thẳng hàng với các tab khác
        with tab4:
            st.subheader(f"Phân tích Giai đoạn Cây trồng - Khu {target_area}")
            
            if not seasons.is_empty():
                season_list = seasons.to_dicts()
                season_names = [f"Vụ {i+1} ({s['Start']} đến {s['End']})" for i, s in enumerate(season_list)]
                selected_season_name = st.selectbox("Chọn Vụ để phân tích giai đoạn:", options=season_names, key="tab4_season_select")
                
                analysis_option = st.radio("Phân tích giai đoạn dựa trên:", ["Số lần tưới", "TBEC"], horizontal=True)
                
                selected_idx = season_names.index(selected_season_name)
                selected_s_id = season_list[selected_idx]['s_id']
                
                # Lấy dữ liệu của vụ được chọn
                df_season_analysis = df_p.filter(pl.col("s_id") == selected_s_id)
                
                if not df_season_analysis.is_empty():
                    # Gom nhóm theo ngày để lấy thông số phân tích
                    stage_data = df_season_analysis.group_by("Date").agg([
                        pl.count().alias("Số lần tưới"),
                        pl.col("val_ec_goc").mean().round(2).alias("TBEC")
                    ]).sort("Date")
                    
                    # Xác định cột giá trị dựa trên option
                    val_col = "Số lần tưới" if analysis_option == "Số lần tưới" else "TBEC"
                    
                    # Logic chia giai đoạn: Khi giá trị thay đổi so với ngày trước đó
                    stage_data = stage_data.with_columns([
                        (pl.col(val_col) != pl.col(val_col).shift(1)).fill_null(True).alias("is_new_stage")
                    ])
                    stage_data = stage_data.with_columns(pl.col("is_new_stage").cum_sum().alias("stage_id"))
                    
                    # Tổng hợp thông tin từng giai đoạn
                    stages_summary = stage_data.group_by("stage_id").agg([
                        pl.col("Date").min().alias("Bắt đầu"),
                        pl.col("Date").max().alias("Kết thúc"),
                        pl.col(val_col).first().alias("Giá trị duy trì"),
                        pl.count().alias("Số ngày")
                    ]).sort("Bắt đầu")
                    
                    # Hiển thị biểu đồ giai đoạn
                    fig_stage = px.steppre(
                        stage_data.to_pandas(), 
                        x="Date", 
                        y=val_col,
                        title=f"Biến thiên {val_col} theo thời gian - {selected_season_name}",
                        markers=True
                    )
                    st.plotly_chart(fig_stage, use_container_width=True)
                    
                    # Hiển thị bảng phân chia giai đoạn
                    st.write(f"Danh sách các giai đoạn cây trồng (Dựa trên {analysis_option}):")
                    st.dataframe(stages_summary.drop("stage_id"), use_container_width=True, hide_index=True)
                else:
                    st.info("Không tìm thấy dữ liệu chi tiết cho vụ này.")
            else:
                st.warning("Chưa có dữ liệu vụ canh tác.")
