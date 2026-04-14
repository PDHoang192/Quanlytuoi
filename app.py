import streamlit as st
import polars as pl
import json
import re
import ast
import datetime
import plotly.express as px
import plotly.graph_objects as go

# --- CẤU HÌNH GIAO DIỆN ---
st.set_page_config(page_title="Hệ Thống Phân Tích Tưới", layout="wide", page_icon="🌱")

# ==========================================
# HÀM 1: SƠ CHẾ DỮ LIỆU LOG THÔ
# ==========================================
@st.cache_data
def doc_file_nhat_ky_co_bo_nho_dem(noi_dung_file_dạng_bytes):
    # Chuyển dữ liệu máy tính (bytes) thành văn bản tiếng Việt (utf-8)
    van_ban_tho = noi_dung_file_dạng_bytes.decode("utf-8").strip()
    
    # --- BỘ LỌC REGEX: Sửa các lỗi cú pháp thường gặp từ thiết bị gửi về ---
    van_ban_sach = re.sub(r'"\s*\n\s*"', '",\n"', van_ban_tho)
    van_ban_sach = re.sub(r',\s*\}', '}', van_ban_sach) # Xóa dấu phẩy thừa trước ngoặc nhọn
    van_ban_sach = re.sub(r',\s*\]', ']', van_ban_sach) # Xóa dấu phẩy thừa trước ngoặc vuông
    van_ban_sach = re.sub(r'\}\s*\{', '},{', van_ban_sach) # Thêm dấu phẩy giữa các cục dữ liệu
    
    # Đảm bảo toàn bộ văn bản được bọc trong ngoặc vuông (chuẩn danh sách JSON)
    van_ban_json = van_ban_sach if van_ban_sach.startswith('[') else f"[{van_ban_sach}]"
    
    try:
        # Thử dịch văn bản thành danh sách dữ liệu chuẩn
        return json.loads(van_ban_json)
    except:
        # Nếu lỗi (do có chữ true/false kiểu Python), dùng phương án dự phòng
        van_ban_python = van_ban_json.replace('true', 'True').replace('false', 'False').replace('null', 'None')
        return ast.literal_eval(van_ban_python)

# ==========================================
# HÀM 2: TRÁI TIM TÍNH TOÁN CỦA HỆ THỐNG
# ==========================================
def xu_ly_du_lieu_loi(bang_du_lieu, ngay_bat_dau, ngay_ket_thuc):
    # Cấu hình luật chia vụ mùa
    gioi_han_nghi_dat = 2 # Đứt quãng quá 2 ngày sẽ tính là vụ mới
    so_ngay_toi_thieu_mot_vu = 10 # Vụ nào ngắn hơn 10 ngày sẽ bị xóa bỏ
    
    # 1. Cắt dữ liệu theo khoảng thời gian người dùng chọn
    if ngay_bat_dau and ngay_ket_thuc:
        bang_du_lieu = bang_du_lieu.filter((pl.col("dt").dt.date() >= ngay_bat_dau) & (pl.col("dt").dt.date() <= ngay_ket_thuc))
    
    # Kiểm tra an toàn: Nếu bảng rỗng thì báo lỗi để dừng lại
    if bang_du_lieu.is_empty(): return None, "Không có dữ liệu."

    # 2. Tách làm 2 nhóm: Nhóm chỉ có chữ "BẬT" và Nhóm chỉ có chữ "TẮT"
    bang_bat = bang_du_lieu.filter(pl.col("Trạng thái").str.to_uppercase().str.strip_chars() == "BẬT")
    bang_tat = bang_du_lieu.filter(pl.col("Trạng thái").str.to_uppercase().str.strip_chars() == "TẮT").with_columns(pl.col("dt").alias("thoi_gian_tat"))
    
    # 3. Phép thuật ghép cặp: Tìm lần Tắt xuất hiện ngay sau lần Bật
    bang_ghep_cap = bang_bat.join_asof(bang_tat, on="dt", strategy="forward", suffix="_end")
    
    # 4. Tính toán giây chạy và Dọn rác
    bang_ghep_cap = bang_ghep_cap.with_columns([
        ((pl.col("thoi_gian_tat") - pl.col("dt")).dt.total_seconds()).alias("thoi_gian_chay_giay"),
        pl.col("dt").dt.date().alias("Ngay_chot"),
        pl.coalesce(["TBEC_end", "TBEC"]).alias("gia_tri_ec_chot") # Mất EC Tắt thì mượn EC Bật bù vào
    ]).filter((pl.col("thoi_gian_chay_giay") > 20) & (pl.col("thoi_gian_chay_giay") < 300)) # CHỈ LẤY: 20 giây < thời gian chạy < 5 phút

    # 5. Lập sổ tổng kết theo từng ngày
    bang_tong_hop_ngay = bang_ghep_cap.group_by("Ngay_chot").agg([
        pl.count().alias("so_lan_tuoi"),
        (pl.col("thoi_gian_chay_giay").sum() / 60).round(1).alias("tong_phut_tuoi"),
        pl.col("gia_tri_ec_chot").mean().alias("ec_trung_binh")
    ]).sort("Ngay_chot")

    # 6. Thuật toán cắt vụ (Gap Detection)
    # So sánh khoảng cách với ngày liền trước xem có lớn hơn 2 ngày không
    bang_tong_hop_ngay = bang_tong_hop_ngay.with_columns([(pl.col("Ngay_chot").diff().dt.total_days() > gioi_han_nghi_dat).fill_null(False).alias("la_vu_moi")])
    # Đánh mã ID cho từng vụ (Ví dụ: Vụ 1, Vụ 2...)
    bang_tong_hop_ngay = bang_tong_hop_ngay.with_columns(pl.col("la_vu_moi").cum_sum().alias("ma_vu"))
    
    # 7. Tổng hợp danh sách các Vụ Mùa
    bang_vu_mua = bang_tong_hop_ngay.group_by("ma_vu").agg([
        pl.col("Ngay_chot").min().alias("Bắt đầu"),
        pl.col("Ngay_chot").max().alias("Kết thúc"),
        ((pl.col("Ngay_chot").max() - pl.col("Ngay_chot").min()).dt.total_days() + 1).alias("Số ngày")
    ]).sort("Bắt đầu")
    
    # Gạch bỏ các vụ test máy linh tinh (ngắn hơn 10 ngày)
    bang_vu_mua = bang_vu_mua.filter(pl.col("Số ngày") >= so_ngay_toi_thieu_mot_vu)
    
    # Trả về 3 cái bảng để xuất ra Giao diện
    return (bang_ghep_cap, bang_vu_mua, bang_tong_hop_ngay), "Thành công"


# ==========================================
# KHU VỰC GIAO DIỆN (UI / STREAMLIT)
# ==========================================
with st.sidebar:
    st.header("⚙️ Nguồn Dữ Liệu")
    stt_khu_vuc_muc_tieu = st.selectbox("Chọn STT Khu vực:", [1, 2, 3, 4], index=0)
    file_tuoi_tai_len = st.file_uploader("1. Log Tưới (Chính)", type=['txt', 'json'])
    file_phan_tai_len = st.file_uploader("2. Log Châm Phân", type=['txt', 'json'])

# CHỈ CHẠY KHI CÓ FILE TẢI LÊN
if file_tuoi_tai_len:
    du_lieu_tho = doc_file_nhat_ky_co_bo_nho_dem(file_tuoi_tai_len.getvalue())
    bang_du_lieu_tho = pl.DataFrame(du_lieu_tho)
    tu_khoa_tim_kiem = str(stt_khu_vuc_muc_tieu)
    
    # Bước lọc Khu vực
    if "STT" in bang_du_lieu_tho.columns:
        bang_du_lieu_tho = bang_du_lieu_tho.filter(pl.col("STT").cast(pl.Utf8).str.contains(tu_khoa_tim_kiem))
        
    # Làm sạch dữ liệu trước khi đưa vào tính toán
    if not bang_du_lieu_tho.is_empty():
        bang_du_lieu_tho = bang_du_lieu_tho.with_columns([
            # Đổi chữ thành định dạng Thời gian chuẩn
            pl.col("Thời gian").str.to_datetime("%Y-%m-%d %H-%M-%S", strict=False).alias("dt"),
            # Xử lý dấu phẩy thập phân kiểu Việt Nam (2,5 thành 2.5)
            pl.col("TBEC").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False) if "TBEC" in bang_du_lieu_tho.columns else pl.lit(None)
        ]).drop_nulls(subset=["dt"]).sort("dt")
        
        # Thiết lập bộ chọn ngày tháng trên Sidebar
        ngay_cu_nhat, ngay_moi_nhat = bang_du_lieu_tho["dt"].min().date(), bang_du_lieu_tho["dt"].max().date()
        che_do_ngay = st.sidebar.radio("Phạm vi:", ["Toàn bộ", "Tùy chọn"])
        ngay_bat_dau, ngay_ket_thuc = ngay_cu_nhat, ngay_moi_nhat
        if che_do_ngay == "Tùy chọn":
            ngay_chon = st.sidebar.date_input("Chọn ngày:", [ngay_cu_nhat, ngay_moi_nhat], min_value=ngay_cu_nhat, max_value=ngay_moi_nhat)
            if len(ngay_chon) == 2: ngay_bat_dau, ngay_ket_thuc = ngay_chon
        
        # --- ĐƯA VÀO HÀM LÕI ---
        ket_qua, thong_bao = xu_ly_du_lieu_loi(bang_du_lieu_tho, ngay_bat_dau, ngay_ket_thuc)
        
        if ket_qua:
            bang_chi_tiet, bang_vu, bang_ngay = ket_qua
            
            # Tạo sẵn một cột EC Yêu cầu rỗng (phòng hờ không có file phân bón)
            bang_ngay = bang_ngay.with_columns(pl.lit(None).cast(pl.Float64).alias("ec_yeu_cau"))
            
            # Nếu có tải file Log Châm Phân lên
            if file_phan_tai_len:
                try:
                    bang_phan = pl.DataFrame(doc_file_nhat_ky_co_bo_nho_dem(file_phan_tai_len.getvalue()))
                    if "EC yêu cầu" in bang_phan.columns:
                        # Làm sạch ngày tháng và EC yêu cầu
                        bang_phan = bang_phan.with_columns([
                            pl.col("Thời gian").str.to_datetime("%Y-%m-%d %H-%M-%S", strict=False).dt.date().alias("Ngay_chot"),
                            pl.col("EC yêu cầu").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False)
                        ]).drop_nulls(subset=["Ngay_chot", "EC yêu cầu"])
                        
                        # Tính trung bình EC yêu cầu mỗi ngày
                        bang_phan_tb = bang_phan.group_by("Ngay_chot").agg([pl.col("EC yêu cầu").mean().alias("ec_yeu_cau_moi")])
                        
                        # Ghép bảng phân bón vào bảng tổng hợp ngày
                        bang_ngay = bang_ngay.join(bang_phan_tb, on="Ngay_chot", how="left")
                        bang_ngay = bang_ngay.with_columns(pl.coalesce(["ec_yeu_cau_moi", "ec_yeu_cau"]).alias("ec_yeu_cau")).drop("ec_yeu_cau_moi")
                except: pass

            # Chuyển bảng Vụ mùa thành dạng danh sách dễ đọc để hiển thị
            danh_sach_vu = bang_vu.to_dicts()
            
            if danh_sach_vu:
                lua_chon_vu = {f"Vụ {i+1} ({s['Bắt đầu']} -> {s['Kết thúc']})": s for i, s in enumerate(danh_sach_vu)}
                
                # --- VẼ GIAO DIỆN 3 TAB CHÍNH ---
                tab_1, tab_2, tab_3 = st.tabs(["📋 Danh Sách Vụ", "📊 Biểu Đồ", "🧠 Phân Tích Giai Đoạn"])

                # --- TAB 1: DANH SÁCH VỤ ---
                with tab_1:
                    danh_sach_hien_thi = []
                    for i, vu in enumerate(danh_sach_vu):
                        # Tính toán khoảng thời gian nghỉ đất giữa vụ cũ và vụ mới
                        if i > 0:
                            ngay_bat_dau_nghi = danh_sach_vu[i-1]["Kết thúc"] + datetime.timedelta(days=1)
                            ngay_ket_thuc_nghi = vu["Bắt đầu"] - datetime.timedelta(days=1)
                            if (ngay_ket_thuc_nghi - ngay_bat_dau_nghi).days >= 0:
                                danh_sach_hien_thi.append({"Đối tượng": "⏳ Nghỉ đất", "Từ": ngay_bat_dau_nghi, "Đến": ngay_ket_thuc_nghi, "Số ngày": (ngay_ket_thuc_nghi - ngay_bat_dau_nghi).days + 1})
                        
                        # Thêm vụ chính vào danh sách hiển thị
                        danh_sach_hien_thi.append({"Đối tượng": f"🌱 Vụ {i+1}", "Từ": vu["Bắt đầu"], "Đến": vu["Kết thúc"], "Số ngày": vu["Số ngày"]})
                    st.table(danh_sach_hien_thi)

                # --- TAB 2: BIỂU ĐỒ ---
                with tab_2:
                    vu_duoc_chon_tab2 = st.selectbox("Chọn Vụ:", list(lua_chon_vu.keys()), key="chon_vu_tab2")
                    # Lọc dữ liệu ngày theo vụ được chọn
                    bang_ngay_cua_vu = bang_ngay.filter(pl.col("ma_vu") == lua_chon_vu[vu_duoc_chon_tab2]["ma_vu"]).sort("Ngay_chot")
                    
                    # Vẽ biểu đồ 1: Số lần tưới (Cột) và Số phút tưới (Đường)
                    bieu_do_1 = go.Figure()
                    bieu_do_1.add_trace(go.Bar(x=bang_ngay_cua_vu["Ngay_chot"], y=bang_ngay_cua_vu["so_lan_tuoi"], name="Lần", marker_color='#3366CC', yaxis='y1'))
                    bieu_do_1.add_trace(go.Scatter(x=bang_ngay_cua_vu["Ngay_chot"], y=bang_ngay_cua_vu["tong_phut_tuoi"], name="Phút", marker_color='#FF3366', yaxis='y2'))
                    bieu_do_1.update_layout(yaxis2=dict(overlaying='y', side='right'), hovermode="x unified")
                    st.plotly_chart(bieu_do_1, use_container_width=True)
                    
                    # Vẽ biểu đồ 2: Đối chiếu Phân bón Thực tế vs Yêu cầu
                    bieu_do_2 = go.Figure()
                    bieu_do_2.add_trace(go.Scatter(x=bang_ngay_cua_vu["Ngay_chot"], y=bang_ngay_cua_vu["ec_trung_binh"], name="EC Thực", line=dict(color='#FF9900')))
                    if "ec_yeu_cau" in bang_ngay_cua_vu.columns and not bang_ngay_cua_vu["ec_yeu_cau"].null_count() == len(bang_ngay_cua_vu):
                        bieu_do_2.add_trace(go.Scatter(x=bang_ngay_cua_vu["Ngay_chot"], y=bang_ngay_cua_vu["ec_yeu_cau"], name="EC Yêu cầu", line=dict(dash='dash')))
                    st.plotly_chart(bieu_do_2, use_container_width=True)

                # --- TAB 3: PHÂN TÍCH GIAI ĐOẠN ---
                with tab_3:
                    ban_do_bien = {"Số lần tưới": "so_lan_tuoi", "TBEC thực tế": "ec_trung_binh", "EC yêu cầu": "ec_yeu_cau"}
                    cac_tieu_chi_hop_le = ["Số lần tưới", "TBEC thực tế"]
                    if "ec_yeu_cau" in bang_ngay.columns and bang_ngay["ec_yeu_cau"].null_count() < len(bang_ngay):
                        cac_tieu_chi_hop_le.append("EC yêu cầu")

                    cot_1, cot_2 = st.columns(2)
                    with cot_1:
                        vu_duoc_chon_tab3 = st.selectbox("Chọn Vụ:", list(lua_chon_vu.keys()), key="chon_vu_tab3")
                        cac_cot_xet_duyet = st.multiselect("Thông số xét duyệt:", cac_tieu_chi_hop_le, default=["Số lần tưới", "TBEC thực tế"])
                        che_do_logic = st.radio("Logic:", ["OR", "AND"], horizontal=True)
                    with cot_2:
                        nguong_lan = st.number_input("Ngưỡng Lần", value=2.0)
                        nguong_ec = st.number_input("Ngưỡng TBEC", value=30.0)
                        nguong_ec_yeu_cau = st.number_input("Ngưỡng EC yêu cầu", value=10.0)
                        ban_do_nguong = {"Số lần tưới": nguong_lan, "TBEC thực tế": nguong_ec, "EC yêu cầu": nguong_ec_yeu_cau}

                    # Lọc dữ liệu cho Tab 3
                    bang_du_lieu_tab3 = bang_ngay.filter(pl.col("ma_vu") == lua_chon_vu[vu_duoc_chon_tab3]["ma_vu"]).sort("Ngay_chot")
                    
                    if cac_cot_xet_duyet:
                        # Bỏ qua các ngày bị trống dữ liệu ở các cột đang xét
                        bang_sach = bang_du_lieu_tab3.drop_nulls(subset=[ban_do_bien[cot] for cot in cac_cot_xet_duyet])
                        
                        if not bang_sach.is_empty():
                            danh_sach_ngay = bang_sach["Ngay_chot"].to_list()
                            nhan_giai_doan, thong_tin_giai_doan = [], []
                            du_lieu_theo_bien = {cot: {"du_lieu": bang_sach[ban_do_bien[cot]].to_list(), "nhom_tam": []} for cot in cac_cot_xet_duyet}
                            
                            ngay_bat_dau_giai_doan = danh_sach_ngay[0]
                            chi_so_giai_doan = 1
                            
                            # Thuật toán vòng lặp quét đứt gãy đa biến
                            for i in range(len(danh_sach_ngay)):
                                cac_dieu_kien_cat = []
                                for cot in cac_cot_xet_duyet:
                                    if du_lieu_theo_bien[cot]["nhom_tam"]:
                                        trung_binh_nhom_tam = sum(du_lieu_theo_bien[cot]["nhom_tam"]) / len(du_lieu_theo_bien[cot]["nhom_tam"])
                                        # Kiểm tra xem giá trị hôm nay có lệch xa so với trung bình các ngày trước không
                                        cac_dieu_kien_cat.append(abs(du_lieu_theo_bien[cot]["du_lieu"][i] - trung_binh_nhom_tam) > ban_do_nguong[cot])
                                    else: 
                                        cac_dieu_kien_cat.append(False)
                                        
                                # Nếu thỏa mãn điều kiện cắt (hoặc OR / hoặc AND)
                                if (any(cac_dieu_kien_cat) if che_do_logic == "OR" else all(cac_dieu_kien_cat)):
                                    thong_tin_giai_doan.append({"Giai đoạn": f"GĐ {chi_so_giai_doan}", "Bắt đầu": ngay_bat_dau_giai_doan, "Kết thúc": danh_sach_ngay[i-1]})
                                    # Reset lại các biến cho Giai đoạn tiếp theo
                                    ngay_bat_dau_giai_doan = danh_sach_ngay[i]
                                    chi_so_giai_doan += 1
                                    for cot in cac_cot_xet_duyet: du_lieu_theo_bien[cot]["nhom_tam"] = []
                                    
                                # Tích lũy dữ liệu vào nhóm tạm
                                for cot in cac_cot_xet_duyet: du_lieu_theo_bien[cot]["nhom_tam"].append(du_lieu_theo_bien[cot]["du_lieu"][i])
                                nhan_giai_doan.append(f"GĐ {chi_so_giai_doan}")
                            
                            # Chốt sổ giai đoạn cuối cùng
                            thong_tin_giai_doan.append({"Giai đoạn": f"GĐ {chi_so_giai_doan}", "Bắt đầu": ngay_bat_dau_giai_doan, "Kết thúc": danh_sach_ngay[-1]})
                            
                            # Gán nhãn giai đoạn ngược lại vào bảng tính và vẽ biểu đồ màu
                            bang_bieu_do_tab3 = bang_sach.with_columns(pl.Series("Giai đoạn", nhan_giai_doan))
                            st.plotly_chart(px.bar(bang_bieu_do_tab3, x="Ngay_chot", y=ban_do_bien[cac_cot_xet_duyet[0]], color='Giai đoạn'))

                            st.divider()
                            giai_doan_duoc_chon = st.selectbox("Chọn Giai đoạn:", [s["Giai đoạn"] for s in thong_tin_giai_doan])
                            
                            # Cấu hình các cột chi tiết ép kiểu số thập phân
                            cac_cot_chi_tiet = [
                                pl.col("Ngay_chot").cast(pl.Utf8).alias("Ngày"),
                                pl.col("so_lan_tuoi").cast(pl.Float64).alias("Lần"),
                                pl.col("tong_phut_tuoi").cast(pl.Float64).alias("Phút"),
                                pl.col("ec_trung_binh").cast(pl.Float64).alias("EC thực")
                            ]
                            
                            # Cấu hình dòng Trung Bình cuối bảng
                            cac_cot_trung_binh = [
                                pl.lit("--- TRUNG BÌNH ---").alias("Ngày"),
                                pl.col("Lần").mean().cast(pl.Float64),
                                pl.col("Phút").mean().cast(pl.Float64),
                                pl.col("EC thực").mean().cast(pl.Float64)
                            ]

                            # Nếu có phân bón thì thêm cột vào bảng
                            if "ec_yeu_cau" in bang_bieu_do_tab3.columns:
                                cac_cot_chi_tiet.append(pl.col("ec_yeu_cau").cast(pl.Float64).alias("EC yêu cầu"))
                                cac_cot_trung_binh.append(pl.col("EC yêu cầu").mean().cast(pl.Float64))

                            # Lọc bảng chỉ lấy Giai đoạn được chọn
                            bang_chi_tiet_giai_doan = bang_bieu_do_tab3.filter(pl.col("Giai đoạn") == giai_doan_duoc_chon).select(cac_cot_chi_tiet)
                            dong_trung_binh = bang_chi_tiet_giai_doan.select(cac_cot_trung_binh)

                            # Nối dòng trung bình vào đáy bảng chi tiết
                            bang_cuoi_cung = pl.concat([bang_chi_tiet_giai_doan, dong_trung_binh])
                            
                            # Hiển thị và ép định dạng số cho gọn (1 hoặc 2 số sau dấu phẩy)
                            st.dataframe(
                                bang_cuoi_cung.to_pandas().style.format({
                                    "Lần": "{:.1f}", 
                                    "Phút": "{:.1f}", 
                                    "EC thực": "{:.2f}", 
                                    "EC yêu cầu": "{:.2f}"
                                }, na_rep="-"), 
                                use_container_width=True, 
                                hide_index=True
                            )
                        else:
                            st.warning("Dữ liệu không đủ để phân tích giai đoạn.")
        else:
            st.error(thong_bao)
