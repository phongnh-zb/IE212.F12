#!/bin/bash

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}>>> [SERVICES] KIỂM TRA & KHỞI ĐỘNG HỆ THỐNG...${NC}"

# 1. KHỞI ĐỘNG HADOOP
if ! jps | grep -q "NameNode"; then
    echo "-> Đang khởi động Hadoop..."
    start-all.sh
else
    echo "✔ Hadoop đã chạy."
fi

# 2. KHỞI ĐỘNG HBASE
if ! jps | grep -q "HMaster"; then
    echo "-> Đang khởi động HBase..."
    start-hbase.sh
else
    echo "✔ HBase Master đã chạy."
fi

# 3. RESTART THRIFT SERVER & CHỜ KẾT NỐI (QUAN TRỌNG)
echo "-> Refreshing HBase Thrift Server..."
jps | grep ThriftServer | awk '{print $1}' | xargs -r kill -9 2>/dev/null
sleep 2

# Khởi động ngầm
hbase thrift start -p 9090 --infoport 9095 > /dev/null 2>&1 &

echo -ne "${YELLOW}⏳ Đang đợi Thrift Server mở port 9090...${NC}"

# Vòng lặp kiểm tra Port 9090 (Tối đa 30 giây)
for i in {1..30}; do
    # Dùng Python để check port (vì không phải máy nào cũng có netcat/nc)
    if python3 -c "import socket; s = socket.socket(socket.AF_INET, socket.SOCK_STREAM); result = s.connect_ex(('127.0.0.1', 9090)); s.close(); exit(result)" 2>/dev/null; then
        echo -e "\n${GREEN}✅ Thrift Server đã sẵn sàng kết nối!${NC}"
        exit 0
    fi
    echo -ne "."
    sleep 1
done

echo -e "\n${RED}❌ Lỗi: Thrift Server không phản hồi sau 30s. Hãy kiểm tra log!${NC}"
exit 1