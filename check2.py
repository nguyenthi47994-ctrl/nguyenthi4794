    import tkinter as tk
from tkinter import filedialog, messagebox, ttk, simpledialog
import pandas as pd
import json
import os
import re
import unicodedata
import hashlib
import threading
from datetime import datetime

# =============================================================================
# 1. CẤU HÌNH & HẰNG SỐ
# =============================================================================

CONFIG_FILE = "config_system.json"

# Màu sắc giao diện
COLOR_BG_MAIN = "#F5F7FA"       
COLOR_SIDEBAR = "#263238"       
COLOR_ACCENT = "#29B6F6"        
COLOR_TEXT_SIDE = "#ECEFF1"     

# Màu trạng thái (Row Tags)
COLOR_ERR_THIEU = "#FFCDD2"     # Hồng (Thiếu)
COLOR_ERR_THUA = "#FFF9C4"      # Vàng (Thừa)
COLOR_ERR_SAI_MA = "#E1BEE7"    # Tím (Sai mã/Không đặt)
COLOR_INFO_GOP = "#BBDEFB"      # Xanh dương nhạt (Đơn gộp)
COLOR_TEXT_GOP = "#0D47A1"      # Chữ xanh đậm cho đơn gộp
COLOR_OK = "#FFFFFF"            # Trắng

# Cột hệ thống mặc định
SYSTEM_COLS = {
   "dh_code": "Mã Khách/ĐC (Đơn Hàng)",
   "dh_item": "Mã Hàng (Đơn Hàng)",
   "dh_name": "Tên Hàng (Đơn Hàng)",
   "dh_sl": "Số Lượng (Đơn Hàng)",
   "dh_so": "Số Đơn Hàng",
   "dh_note": "Ghi chú", # Thêm cột ghi chú nếu có
   
   "px_code": "Mã Khách/ĐC (Phiếu Xuất)",
   "px_item": "Mã Hàng (Phiếu Xuất)",
   "px_name": "Tên Hàng (Phiếu Xuất)",
   "px_sl_xuat": "SL Xuất (Kg/Thùng)",
   "px_sl_tui": "SL Túi/Con",
   "px_so": "Số Phiếu Xuất"
}

# =============================================================================
# 2. HỆ THỐNG BẢO MẬT & CẤU HÌNH
# =============================================================================

class SecurityManager:
    @staticmethod
    def hash_pin(pin):
        return hashlib.sha256(str(pin).encode()).hexdigest()

class ConfigManager:
    def __init__(self):
        self.data = {
            "pin_hash": SecurityManager.hash_pin("1234"),
            "paths": {"dh": "", "px": ""},
            "col_map": {},
            "bag_items": [],
            "alias_map": {},
            "tolerance": {"kg_min": 0.0, "kg_max": 0.0, "bag_diff": 0}
        }
        self.load()

    def load(self):
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                    loaded = json.load(f)
                    for k, v in loaded.items():
                        if k in self.data and isinstance(self.data[k], dict):
                            self.data[k].update(v)
                        else:
                            self.data[k] = v
            except: pass

    def save(self):
        try:
            with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, indent=4, ensure_ascii=False)
        except: pass

# =============================================================================
# 3. XỬ LÝ DỮ LIỆU (CORE LOGIC)
# =============================================================================

class DataProcessor:
    def __init__(self, config_mgr):
        self.cfg = config_mgr
        
        # Dữ liệu hiển thị (List of Dict)
        self.res_tab1 = [] 
        self.res_tab2 = [] 
        self.res_tab3 = [] 
        
        # Dữ liệu chi tiết cho Popup (Map: (Key, Item) -> {Orders:[], Exports:[]})
        self.detail_map = {} 

    def normalize(self, text):
        if pd.isna(text) or text == "": return ""
        t = str(text).strip().upper()
        t = " ".join(t.split())
        return unicodedata.normalize('NFC', t)

    def extract_key(self, text):
        s = self.normalize(text)
        match = re.search(r'((ST|DC|KH)\d+)', s)
        return match.group(1) if match else "UNKNOWN"

    def run_analysis(self):
        self.res_tab1, self.res_tab2, self.res_tab3 = [], [], []
        self.detail_map = {}
        
        p_dh = self.cfg.data["paths"]["dh"]
        p_px = self.cfg.data["paths"]["px"]
        cmap = self.cfg.data["col_map"] if self.cfg.data["col_map"] else SYSTEM_COLS
        bag_list = set(self.cfg.data["bag_items"])
        alias_map = self.cfg.data["alias_map"]

        # --- 1. ĐỌC FILE ---
        try:
            df_dh = pd.read_excel(p_dh, dtype=str)
            df_px = pd.read_excel(p_px, dtype=str)
        except Exception as e:
            return False, f"Lỗi đọc file: {str(e)}"

        # --- 2. XỬ LÝ ĐƠN HÀNG ---
        # Map cột
        dh_c_code = cmap.get("dh_code", "")
        dh_c_item = cmap.get("dh_item", "")
        dh_c_sl = cmap.get("dh_sl", "")
        dh_c_name = cmap.get("dh_name", "")
        dh_c_so = cmap.get("dh_so", "")
        dh_c_note = cmap.get("dh_note", "Ghi chú")

        # Temp Storage
        orders_agg = {} # (Key, Item) -> Total SL
        orders_count = {} # (Key, Item) -> Count lines (để check gộp)

        for idx, row in df_dh.iterrows():
            raw_key = str(row.get(dh_c_code, ''))
            key = self.extract_key(raw_key)
            
            raw_item = self.normalize(row.get(dh_c_item, ''))
            item = alias_map.get(raw_item, raw_item)
            
            try: sl = float(row.get(dh_c_sl, 0))
            except: sl = 0
            if sl <= 0: continue

            if key == "UNKNOWN":
                self.res_tab3.append({"Loại": "Đơn Hàng", "Lỗi": "Không định danh Khách", "Dữ liệu": f"{raw_key}|{raw_item}"})
                continue

            k = (key, item)
            
            # Cộng dồn
            orders_agg[k] = orders_agg.get(k, 0) + sl
            orders_count[k] = orders_count.get(k, 0) + 1
            
            # Lưu chi tiết vào detail_map
            if k not in self.detail_map: self.detail_map[k] = {'orders': [], 'exports': []}
            self.detail_map[k]['orders'].append({
                "SoDH": row.get(dh_c_so, ''),
                "Name": row.get(dh_c_name, ''),
                "SL": sl,
                "Note": row.get(dh_c_note, '')
            })

        # --- 3. XỬ LÝ PHIẾU XUẤT ---
        exports_agg = {}
        list_px_lines = []

        px_c_code = cmap.get("px_code", "")
        px_c_item = cmap.get("px_item", "")
        px_c_sl_x = cmap.get("px_sl_xuat", "")
        px_c_sl_t = cmap.get("px_sl_tui", "")
        px_c_so = cmap.get("px_so", "")
        px_c_name = cmap.get("px_name", "")

        for idx, row in df_px.iterrows():
            raw_key = str(row.get(px_c_code, ''))
            key = self.extract_key(raw_key)
            raw_item = self.normalize(row.get(px_c_item, ''))
            item = alias_map.get(raw_item, raw_item)
            
            try: sl_x = float(row.get(px_c_sl_x, 0))
            except: sl_x = 0
            try: sl_t = float(row.get(px_c_sl_t, 0))
            except: sl_t = 0
            
            if key == "UNKNOWN":
                self.res_tab3.append({"Loại": "Phiếu Xuất", "Lỗi": "Không định danh Khách", "Dữ liệu": f"{raw_key}|{raw_item}|PX:{row.get(px_c_so,'')}"})
                continue
            if item == "":
                self.res_tab3.append({"Loại": "Phiếu Xuất", "Lỗi": "Mã hàng rỗng", "Dữ liệu": str(row.values)})
                continue

            k = (key, item)
            
            # Cộng dồn
            if k not in exports_agg: exports_agg[k] = {'Kg': 0.0, 'Tui': 0.0}
            exports_agg[k]['Kg'] += sl_x
            exports_agg[k]['Tui'] += sl_t

            # Lưu chi tiết
            if k not in self.detail_map: self.detail_map[k] = {'orders': [], 'exports': []}
            self.detail_map[k]['exports'].append({
                "SoPX": row.get(px_c_so, ''),
                "Name": row.get(px_c_name, ''),
                "SL_Xuat": sl_x,
                "SL_Tui": sl_t
            })

            # Lưu dòng để tính Tab 2
            list_px_lines.append({
                "SoPX": str(row.get(px_c_so, '')),
                "Key": key, "Item": item, "Name": str(row.get(px_c_name, '')),
                "SL_Xuat": sl_x, "SL_Tui": sl_t
            })

        # --- 4. TÍNH TOÁN TAB 1 (TỔNG HỢP) ---
        all_keys = set(orders_agg.keys()) | set(exports_agg.keys())
        
        tol_min = self.cfg.data["tolerance"]["kg_min"]
        tol_max = self.cfg.data["tolerance"]["kg_max"]
        tol_bag = self.cfg.data["tolerance"]["bag_diff"]

        for k, item in all_keys:
            sl_dat = orders_agg.get((k, item), 0)
            ex_data = exports_agg.get((k, item), {'Kg': 0, 'Tui': 0})
            
            is_bag = item in bag_list
            unit = "Túi" if is_bag else "Kg"
            
            sl_xuat_final = ex_data['Tui'] if is_bag else ex_data['Kg']
            lech = sl_xuat_final - sl_dat
            
            # Logic Trạng Thái
            status = "ĐỦ"
            tag = "ok"
            
            if is_bag:
                if abs(lech) > tol_bag:
                    if sl_dat == 0: status = "KHÔNG ĐẶT MÀ XUẤT"; tag = "tim"
                    elif lech < 0: status = f"THIẾU {abs(lech):.0f}"; tag = "do"
                    else: status = f"THỪA {abs(lech):.0f}"; tag = "vang"
            else:
                if lech < tol_min: status = f"THIẾU {abs(lech):.2f}"; tag = "do"
                elif lech > tol_max:
                    if sl_dat == 0: status = "KHÔNG ĐẶT MÀ XUẤT"; tag = "tim"
                    else: status = f"THỪA {abs(lech):.2f}"; tag = "vang"
            
            # Check Gộp
            is_merged = orders_count.get((k, item), 0) > 1
            if is_merged:
                tag = "gop" if tag == "ok" else tag # Nếu lỗi thì ưu tiên màu lỗi, nếu đủ thì màu gộp
                # Nhưng yêu cầu là màu xanh dương cho dòng gộp. 
                # Ta sẽ xử lý hiển thị icon ở giao diện.
            
            self.res_tab1.append({
                "Key": k, "Item": item, "Unit": unit,
                "SL_Dat": sl_dat, "SL_Xuat": sl_xuat_final, "Lech": lech,
                "Status": status, "Tag": tag, "IsMerged": is_merged
            })

        # --- 5. TÍNH TOÁN TAB 2 (CHI TIẾT) ---
        for row in list_px_lines:
            k = row['Key']; item = row['Item']
            is_bag = item in bag_list
            unit = "Túi" if is_bag else "Kg"
            
            total_dat = orders_agg.get((k, item), 0)
            total_xuat = exports_agg.get((k, item), {'Kg':0, 'Tui':0})
            val_xuat_total = total_xuat['Tui'] if is_bag else total_xuat['Kg']
            
            lech_tong = val_xuat_total - total_dat
            
            status = ""
            tag = "ok"
            # Logic tương tự Tab 1 nhưng gán cho dòng
            if is_bag:
                if abs(lech_tong) > tol_bag:
                    if total_dat == 0: status = "SAI MÃ / KHÔNG ĐẶT"; tag = "tim"
                    elif lech_tong < 0: status = "TỔNG THIẾU"; tag = "do"
                    else: status = "TỔNG THỪA"; tag = "vang"
            else:
                if lech_tong < tol_min: status = "TỔNG THIẾU"; tag = "do"
                elif lech_tong > tol_max:
                    if total_dat == 0: status = "SAI MÃ / KHÔNG ĐẶT"; tag = "tim"
                    else: status = "TỔNG THỪA"; tag = "vang"

            row_out = row.copy()
            row_out.update({
                "Unit": unit, "SL_Dong": row['SL_Tui'] if is_bag else row['SL_Xuat'],
                "Total_Dat": total_dat, "Total_Xuat": val_xuat_total,
                "Lech_Tong": lech_tong, "Status": status, "Tag": tag
            })
            self.res_tab2.append(row_out)

        return True, "Xử lý hoàn tất!"

# =============================================================================
# 4. SMART POPUP (CỬA SỔ CHI TIẾT 2 BÊN)
# =============================================================================

class SmartPopup:
    def __init__(self, parent_root, title, data_left, data_right, is_bag):
        self.top = tk.Toplevel(parent_root)
        self.top.title(title)
        self.top.geometry("900x400")
        self.top.configure(bg="white")
        # Luôn nổi trên cùng
        self.top.attributes('-topmost', True)
        
        self.pinned = False
        
        # Header + Pin Button
        f_head = tk.Frame(self.top, bg="#ECEFF1", padx=5, pady=5)
        f_head.pack(fill="x")
        self.btn_pin = tk.Button(f_head, text="📌 Ghim cửa sổ", command=self.toggle_pin, bg="white", relief="flat")
        self.btn_pin.pack(side="right")
        tk.Label(f_head, text=title, font=("Arial", 11, "bold"), bg="#ECEFF1").pack(side="left")

        # Layout Split
        paned = tk.PanedWindow(self.top, orient=tk.HORIZONTAL, bg="white")
        paned.pack(fill="both", expand=True, padx=5, pady=5)
        
        # --- LEFT: ĐƠN ĐẶT ---
        f_left = tk.LabelFrame(paned, text="📦 NGUỒN ĐẶT (Đơn Hàng)", bg="white", fg="blue")
        paned.add(f_left)
        
        cols_l = ["Số ĐH", "Tên Hàng Gốc", "SL Đặt", "Ghi chú"]
        tree_l = ttk.Treeview(f_left, columns=cols_l, show="headings", height=8)
        for c in cols_l: 
            tree_l.heading(c, text=c)
            tree_l.column(c, width=80 if c != "Tên Hàng Gốc" else 150)
        tree_l.pack(fill="both", expand=True)
        
        total_dat = 0
        for item in data_left:
            sl = item.get('SL', 0)
            total_dat += sl
            tree_l.insert("", "end", values=(item.get('SoDH'), item.get('Name'), f"{sl:g}", item.get('Note')))
        
        tk.Label(f_left, text=f"TỔNG ĐẶT: {total_dat:g}", font=("Arial", 10, "bold"), fg="blue", bg="white").pack(anchor="e")

        # --- RIGHT: PHIẾU XUẤT ---
        f_right = tk.LabelFrame(paned, text="🚚 NGUỒN XUẤT (Thực tế)", bg="white", fg="red")
        paned.add(f_right)
        
        cols_r = ["Số PX", "Tên Hàng Xuất", "SL Xuất", "SL Túi"]
        tree_r = ttk.Treeview(f_right, columns=cols_r, show="headings", height=8)
        for c in cols_r: 
            tree_r.heading(c, text=c)
            tree_r.column(c, width=80 if "SL" in c else 150)
        tree_r.pack(fill="both", expand=True)
        
        total_xuat = 0
        for item in data_right:
            val = item.get('SL_Tui', 0) if is_bag else item.get('SL_Xuat', 0)
            total_xuat += val
            tree_r.insert("", "end", values=(item.get('SoPX'), item.get('Name'), f"{item.get('SL_Xuat',0):g}", f"{item.get('SL_Tui',0):g}"))
            
        tk.Label(f_right, text=f"TỔNG XUẤT ({'Túi' if is_bag else 'Kg'}): {total_xuat:g}", font=("Arial", 10, "bold"), fg="red", bg="white").pack(anchor="e")

        # --- EVENTS ---
        # Rê chuột ra khỏi cửa sổ -> Đóng (Nếu chưa ghim)
        self.top.bind("<Leave>", self.check_close)
        
    def toggle_pin(self):
        self.pinned = not self.pinned
        if self.pinned:
            self.btn_pin.config(bg="yellow", text="📍 Đã Ghim")
        else:
            self.btn_pin.config(bg="white", text="📌 Ghim cửa sổ")
            
    def check_close(self, event):
        # Kiểm tra xem chuột có thực sự ra khỏi toplevel không (tránh sự kiện con kích hoạt)
        if self.pinned: return
        x, y = self.top.winfo_pointerxy()
        widget = self.top.winfo_containing(x, y)
        if str(widget).startswith(str(self.top)):
            return # Vẫn đang trong cửa sổ hoặc con của nó
        self.top.destroy()

# =============================================================================
# 5. GIAO DIỆN CHÍNH (MAIN APP)
# =============================================================================

class MainApp:
    def __init__(self, root):
        self.root = root
        self.cfg = ConfigManager()
        
        # Login (Nếu muốn bỏ qua khi test, comment 3 dòng dưới)
        # login = LoginDialog(root, self.cfg) # (Cần class LoginDialog như cũ)
        # if not login.success:
        #    root.destroy(); return
            
        self.setup_ui()
        self.processor = DataProcessor(self.cfg)
        
        # Biến lưu dữ liệu đang hiển thị trên lưới (để xuất Excel đúng cái đang thấy)
        self.current_view_data = [] 

    def setup_ui(self):
        self.root.title("CHECK ĐƠN HÀNG PRO v1.0")
        self.root.geometry("1400x850")
        self.root.configure(bg=COLOR_BG_MAIN)
        
        style = ttk.Style()
        style.theme_use("clam")
        style.configure("Treeview.Heading", font=("Segoe UI", 10, "bold"), background="#CFD8DC")
        style.configure("Treeview", rowheight=28, font=("Segoe UI", 10))
        
        # --- LEFT SIDEBAR ---
        self.f_side = tk.Frame(self.root, bg=COLOR_SIDEBAR, width=260)
        self.f_side.pack(side="left", fill="y")
        self.f_side.pack_propagate(False)
        
        tk.Label(self.f_side, text="HỆ THỐNG\nĐỐI CHIẾU KHO", bg=COLOR_SIDEBAR, fg="white", font=("Arial", 14, "bold")).pack(pady=20)
        
        self.create_input("File Đơn Hàng:", "dh")
        self.create_input("File Phiếu Xuất:", "px")
        
        tk.Label(self.f_side, text="--------------", bg=COLOR_SIDEBAR, fg="gray").pack(pady=10)
        tk.Button(self.f_side, text="📦 QUẢN LÝ TÚI/KG", bg="#FF9800", fg="black", font=("Arial", 10, "bold"), command=self.open_bag_manager).pack(fill="x", padx=10, pady=5)
        tk.Button(self.f_side, text="▶ BẮT ĐẦU CHẠY", bg=COLOR_ACCENT, fg="white", font=("Arial", 12, "bold"), height=2, command=self.run_process).pack(fill="x", padx=10, pady=20)
        
        # --- MAIN AREA ---
        f_main = tk.Frame(self.root, bg=COLOR_BG_MAIN)
        f_main.pack(side="right", fill="both", expand=True)
        
        # TOOLBAR (Search + Export)
        f_tool = tk.Frame(f_main, bg="white", pady=8, padx=10)
        f_tool.pack(fill="x")
        
        # Ô Tìm kiếm
        tk.Label(f_tool, text="🔍 Tìm nhanh:", bg="white").pack(side="left")
        self.entry_search = tk.Entry(f_tool, width=30, font=("Arial", 10))
        self.entry_search.pack(side="left", padx=5)
        self.entry_search.bind("<KeyRelease>", self.on_search) # Lọc real-time
        
        # Nút In/Xuất
        tk.Button(f_tool, text="🖨️ XUẤT EXCEL (WYSIWYG)", bg="#4CAF50", fg="white", font=("Arial", 10, "bold"), command=self.export_excel).pack(side="right")
        
        # Checkbox Focus
        self.var_focus = tk.BooleanVar(value=False)
        tk.Checkbutton(f_tool, text="🔥 Chỉ hiện lỗi", variable=self.var_focus, bg="white", command=self.refresh_views).pack(side="right", padx=10)

        # NOTEBOOK TABS
        self.nb = ttk.Notebook(f_main)
        self.nb.pack(fill="both", expand=True, padx=10, pady=10)
        
        # Tab 1
        self.tree1 = self.create_tree(self.nb, "TAB 1: TỔNG HỢP", 
                                      ["Key", "Mã Hàng", "Đơn Vị", "SL Đặt", "SL Xuất", "LỆCH", "TRẠNG THÁI"])
        self.tree1.bind("<Double-1>", self.on_popup_trigger)
        self.tree1.bind("<Button-3>", self.on_right_click)
        
        # Tab 2
        self.tree2 = self.create_tree(self.nb, "TAB 2: CHI TIẾT PHIẾU",
                                      ["Số PX", "Key", "Mã Hàng", "Tên Hàng", "Đơn Vị", "SL Dòng", "Tổng Đặt", "Tổng Xuất", "LỆCH TỔNG", "TRẠNG THÁI"])
        self.tree2.bind("<Double-1>", self.on_popup_trigger)
        
        # Tab 3
        self.tree3 = self.create_tree(self.nb, "TAB 3: NGOẠI LỆ", ["Loại", "Lỗi", "Dữ liệu"])
        
        # Status Bar
        self.lbl_status = tk.Label(f_main, text="Sẵn sàng.", relief=tk.SUNKEN, anchor="w", bg="#ECEFF1")
        self.lbl_status.pack(side="bottom", fill="x")
        
        # Menu Chuột phải
        self.context_menu = tk.Menu(self.root, tearoff=0)
        self.context_menu.add_command(label="👀 Xem Chi tiết (2 bên)", command=self.on_popup_menu)
        self.context_menu.add_separator()
        self.context_menu.add_command(label="➕ Thêm vào Hàng Tính Túi", command=self.quick_add_bag)

    def create_input(self, label, key):
        tk.Label(self.f_side, text=label, bg=COLOR_SIDEBAR, fg=COLOR_TEXT_SIDE).pack(anchor="w", padx=10, pady=(10,0))
        f = tk.Frame(self.f_side, bg=COLOR_SIDEBAR)
        f.pack(fill="x", padx=10)
        e = tk.Entry(f); e.pack(side="left", fill="x", expand=True)
        e.insert(0, self.cfg.data["paths"][key])
        tk.Button(f, text="...", width=3, command=lambda: self.browse(e, key)).pack(side="right")
        setattr(self, f"e_{key}", e)

    def create_tree(self, parent, title, cols):
        f = tk.Frame(parent); parent.add(f, text=title)
        tree = ttk.Treeview(f, columns=cols, show="headings")
        sb = ttk.Scrollbar(f, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=sb.set)
        tree.pack(side="left", fill="both", expand=True); sb.pack(side="right", fill="y")
        
        for c in cols:
            tree.heading(c, text=c)
            w = 80 if "SL" in c else 150
            tree.column(c, width=w)
            
        # Config màu sắc tags
        tree.tag_configure('do', background=COLOR_ERR_THIEU)
        tree.tag_configure('vang', background=COLOR_ERR_THUA)
        tree.tag_configure('tim', background=COLOR_ERR_SAI_MA)
        tree.tag_configure('gop', background=COLOR_INFO_GOP, foreground=COLOR_TEXT_GOP) # Màu đơn gộp
        tree.tag_configure('ok', background=COLOR_OK)
        return tree

    def browse(self, entry, key):
        p = filedialog.askopenfilename(filetypes=[("Excel", "*.xlsx *.xls")])
        if p:
            entry.delete(0, tk.END); entry.insert(0, p)
            self.cfg.data["paths"][key] = p; self.cfg.save()

    def run_process(self):
        self.cfg.data["paths"]["dh"] = self.e_dh.get()
        self.cfg.data["paths"]["px"] = self.e_px.get()
        self.cfg.save()
        
        self.lbl_status.config(text="Đang xử lý...")
        self.root.update()
        threading.Thread(target=self._run_thread).start()

    def _run_thread(self):
        ok, msg = self.processor.run_analysis()
        self.root.after(0, lambda: [self.refresh_views(), messagebox.showinfo("Kết quả", msg) if ok else messagebox.showerror("Lỗi", msg)])
        self.root.after(0, lambda: self.lbl_status.config(text="Sẵn sàng."))

    def refresh_views(self):
        self.on_search(None) # Gọi hàm Search để nạp dữ liệu (vì search sẽ nạp dữ liệu gốc nếu ô search rỗng)

    def on_search(self, event):
        """Hàm lọc dữ liệu & Hiển thị"""
        keyword = self.normalize_search(self.entry_search.get())
        focus_err = self.var_focus.get()
        
        # --- TAB 1 ---
        self.tree1.delete(*self.tree1.get_children())
        raw_t1 = self.processor.res_tab1
        
        # Sort ưu tiên: Lỗi -> Gộp -> OK
        def sort_prio(x):
            if x['Tag'] in ['do', 'vang', 'tim']: return 0
            if x['Tag'] == 'gop': return 1
            return 2
        
        sorted_t1 = sorted(raw_t1, key=sort_prio)
        
        self.current_view_data_tab1 = [] # Lưu để xuất excel
        
        for r in sorted_t1:
            # Filter Focus
            if focus_err and r['Tag'] == 'ok' and r['Tag'] != 'gop': continue
            
            # Filter Search
            search_str = f"{r['Key']} {r['Item']} {r['Status']}".upper()
            if keyword and keyword not in search_str: continue
            
            # Thêm icon cho đơn gộp
            item_display = r['Item']
            if r.get('IsMerged', False):
                item_display = "📦+ " + item_display
                
            vals = (r['Key'], item_display, r['Unit'], f"{r['SL_Dat']:g}", f"{r['SL_Xuat']:g}", f"{r['Lech']:g}", r['Status'])
            self.tree1.insert("", "end", values=vals, tags=(r['Tag'],))
            self.current_view_data_tab1.append(vals)

        # --- TAB 2 ---
        self.tree2.delete(*self.tree2.get_children())
        raw_t2 = self.processor.res_tab2
        sorted_t2 = sorted(raw_t2, key=lambda x: 0 if x['Tag'] != 'ok' else 1)
        
        self.current_view_data_tab2 = []
        
        for r in sorted_t2:
            if focus_err and r['Tag'] == 'ok': continue
            search_str = f"{r['Key']} {r['Item']} {r['SoPX']} {r['Status']}".upper()
            if keyword and keyword not in search_str: continue
            
            vals = (r['SoPX'], r['Key'], r['Item'], r['Name'], r['Unit'], f"{r['SL_Dong']:g}", f"{r['Total_Dat']:g}", f"{r['Total_Xuat']:g}", f"{r['Lech_Tong']:g}", r['Status'])
            self.tree2.insert("", "end", values=vals, tags=(r['Tag'],))
            self.current_view_data_tab2.append(vals)

        # --- TAB 3 ---
        self.tree3.delete(*self.tree3.get_children())
        for r in self.processor.res_tab3:
            search_str = str(r).upper()
            if keyword and keyword not in search_str: continue
            self.tree3.insert("", "end", values=(r['Loại'], r['Lỗi'], r['Dữ liệu']))

    def normalize_search(self, txt):
        return unicodedata.normalize('NFC', txt.strip().upper())

    # --- POPUP LOGIC ---
    def on_right_click(self, event):
        item = self.tree1.identify_row(event.y)
        if item:
            self.tree1.selection_set(item)
            self.context_menu.post(event.x_root, event.y_root)

    def on_popup_menu(self):
        self.on_popup_trigger(None)

    def on_popup_trigger(self, event):
        # Xác định đang ở Tab nào
        current_tab = self.nb.index(self.nb.select())
        tree = self.tree1 if current_tab == 0 else self.tree2
        
        sel = tree.selection()
        if not sel: return
        vals = tree.item(sel[0], "values")
        
        # Tab 1: Key=0, Item=1; Tab 2: Key=1, Item=2
        key = vals[0] if current_tab == 0 else vals[1]
        item_raw = vals[1] if current_tab == 0 else vals[2]
        
        # Bỏ icon 📦+ nếu có
        item = item_raw.replace("📦+ ", "")
        
        details = self.processor.detail_map.get((key, item))
        if not details: return
        
        is_bag = item in self.cfg.data["bag_items"]
        
        SmartPopup(self.root, f"CHI TIẾT: {key} - {item}", details['orders'], details['exports'], is_bag)

    # --- EXPORT EXCEL ---
    def export_excel(self):
        current_tab = self.nb.index(self.nb.select())
        
        if current_tab == 0:
            cols = ["Key", "Mã Hàng", "Đơn Vị", "SL Đặt", "SL Xuất", "LỆCH", "TRẠNG THÁI"]
            data = self.current_view_data_tab1
            sheet_name = "TongHop"
        elif current_tab == 1:
            cols = ["Số PX", "Key", "Mã Hàng", "Tên Hàng", "Đơn Vị", "SL Dòng", "Tổng Đặt", "Tổng Xuất", "LỆCH TỔNG", "TRẠNG THÁI"]
            data = self.current_view_data_tab2
            sheet_name = "ChiTiet"
        else:
            messagebox.showinfo("Info", "Tab Ngoại lệ chưa hỗ trợ xuất in đẹp. Hãy copy trực tiếp.")
            return

        if not data:
            messagebox.showwarning("Trống", "Không có dữ liệu để xuất!")
            return

        # Tạo file
        timestamp = datetime.now().strftime("%H%M%S")
        fname = f"BaoCao_{sheet_name}_{timestamp}.xlsx"
        
        df = pd.DataFrame(data, columns=cols)
        try:
            df.to_excel(fname, index=False)
            os.startfile(fname) # Mở file ngay (Windows)
        except Exception as e:
            messagebox.showerror("Lỗi Xuất File", str(e))

    # --- TIỆN ÍCH KHÁC ---
    def open_bag_manager(self):
        items = set()
        if self.processor.detail_map:
             items = {k[1] for k in self.processor.detail_map.keys()}
        BagManagerDialog(self.root, self.cfg, items) # (Cần class BagManagerDialog như cũ)

    def quick_add_bag(self):
        sel = self.tree1.selection()
        if not sel: return
        val = self.tree1.item(sel[0], "values")[1].replace("📦+ ", "")
        if val not in self.cfg.data["bag_items"]:
            self.cfg.data["bag_items"].append(val)
            self.cfg.save()
            messagebox.showinfo("OK", f"Đã thêm {val} vào tính Túi.")

# =============================================================================
# CÁC CLASS PHỤ (LOGIN, BAG MANAGER) - GIỮ NGUYÊN TỪ VERSION TRƯỚC
# =============================================================================
class BagManagerDialog:
    def __init__(self, parent, config_mgr, all_items):
        self.top = tk.Toplevel(parent)
        self.top.title("QUẢN LÝ HÀNG TÍNH TÚI")
        self.top.geometry("700x500")
        self.cfg = config_mgr
        self.all_items = sorted(list(all_items))
        self.current_bags = set(self.cfg.data["bag_items"])
        f = tk.Frame(self.top); f.pack(fill="both", expand=True, padx=10, pady=10)
        f1 = tk.LabelFrame(f, text="Hàng tính KG (Mặc định)"); f1.pack(side="left", fill="both", expand=True)
        self.lb_kg = tk.Listbox(f1, selectmode=tk.EXTENDED); self.lb_kg.pack(fill="both", expand=True)
        fb = tk.Frame(f); fb.pack(side="left", padx=5)
        tk.Button(fb, text=">>", command=self.to_bag).pack(pady=5)
        tk.Button(fb, text="<<", command=self.to_kg).pack(pady=5)
        f2 = tk.LabelFrame(f, text="Hàng tính TÚI"); f2.pack(side="left", fill="both", expand=True)
        self.lb_bag = tk.Listbox(f2, selectmode=tk.EXTENDED); self.lb_bag.pack(fill="both", expand=True)
        tk.Button(self.top, text="LƯU CẤU HÌNH", bg="green", fg="white", command=self.save).pack(pady=5)
        self.refresh()
    def refresh(self):
        self.lb_kg.delete(0, tk.END); self.lb_bag.delete(0, tk.END)
        for i in self.all_items:
            if i in self.current_bags: self.lb_bag.insert(tk.END, i)
            else: self.lb_kg.insert(tk.END, i)
    def to_bag(self):
        for s in [self.lb_kg.get(i) for i in self.lb_kg.curselection()]: self.current_bags.add(s)
        self.refresh()
    def to_kg(self):
        for s in [self.lb_bag.get(i) for i in self.lb_bag.curselection()]: 
            if s in self.current_bags: self.current_bags.remove(s)
        self.refresh()
    def save(self):
        self.cfg.data["bag_items"] = list(self.current_bags); self.cfg.save(); self.top.destroy()

if __name__ == "__main__":
    root = tk.Tk()
    app = MainApp(root)
    root.mainloop()