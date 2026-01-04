import os
import sys

# 1. Tự động tìm Project Root 
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))

# 2. Thêm vào sys.path nếu chưa có
if project_root not in sys.path:
    sys.path.append(project_root)

# 3. Import Config và "xuất khẩu" (export) nó ra để file khác dùng
try:
    from configs import config
except ImportError:
    raise ImportError("Khong tim thay module 'configs'. Vui long kiem tra cau truc thu muc.")