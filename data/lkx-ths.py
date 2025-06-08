import re
import csv
import json
import pandas as pd
from playwright.sync_api import sync_playwright
from seatable_api import Base
from datetime import datetime
import os

# 从环境变量获取配置，如果没有则使用默认值
API_TOKEN = os.getenv('SEATABLE_API_TOKEN', "6c264ebbcd8ee911db00e4ca69afce6270bd4c72")
SERVER_URL = os.getenv('SEATABLE_SERVER_URL', "https://cloud.seatable.cn")
TABLE_NAME = "龙虎榜"

# 文件名
FRONT_FILENAME = "longhu_data.csv"
BACK_FILENAME = "longhu_detail.csv"
THIRD_FILENAME = "longhu_rank.csv"

def is_chinese(text):
    return bool(re.search(r'[\u4e00-\u9fff]', text))

def fetch_longhu_data():
    data = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()

        url_main = "https://data.10jqka.com.cn/market/longhu/"
        page.goto(url_main)
        page.wait_for_selector("table.m-table", timeout=60000)
        rows_main = page.query_selector_all("table.m-table tbody tr")
        for row in rows_main:
            cells = row.query_selector_all("td")
            row_data = [cell.inner_text() for cell in cells]
            data.append(row_data)
            
        url_rank = "https://data.10jqka.com.cn/ifmarket/lhbyyb/type/1/tab/sbcs/field/sbcs/sort/desc/page/3/"
        page.goto(url_rank)
        page.wait_for_selector("table.m-table", timeout=60000)
        rows_rank = page.query_selector_all("table.m-table tbody tr")
        for row in rows_rank:
            cells = row.query_selector_all("td")
            row_data = [cell.inner_text() for cell in cells]
            data.append(row_data)
            
        browser.close()
    return data

def split_data_extended(data):
    front, back, third = [], [], []
    for row in data:
        if len(row) > 1 and is_chinese(row[1]):
            third.append(row)
        elif len(row) > 5 and row[5].strip() != "":
            front.append(row)
        else:
            back.append(row)
    return front, back, third

def save_to_separated_csv_extended(data, 
                                   front_filename=FRONT_FILENAME, 
                                   back_filename=BACK_FILENAME, 
                                   third_filename=THIRD_FILENAME):
    front, back, third = split_data_extended(data)
    
    # 获取当前日期
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    # 保存 front 数据到 front_filename（保持原样）
    with open(front_filename, mode="w", newline="", encoding="utf-8") as front_file:
        writer = csv.writer(front_file)
        writer.writerow(["时效", "代码", "名称", "现价", "涨跌幅", "成交金额", "净买入额", "更新时间"])
        for row in front:
            writer.writerow(row + [current_date])
    
    # 保存 back 数据到 back_filename
    with open(back_filename, mode="w", newline="", encoding="utf-8") as back_file:
        writer = csv.writer(back_file)
        writer.writerow(["大客户名称", "买入额", "卖出额", "净额/万", "更新时间"])
        for row in back:
            writer.writerow(row + [current_date])

    # 保存 third 数据到 third_filename
    with open(third_filename, mode="w", newline="", encoding="utf-8") as third_file:
        writer = csv.writer(third_file)
        writer.writerow(["序号", "营业部名称", "上榜次数", "合计动用资金", "年内上榜次数", "年内买入股票支数", "年内3日跟买成功率", "更新时间"])
        for row in third:
            writer.writerow(row + [current_date])
    print(f"已生成文件：{front_filename}、{back_filename} 和 {third_filename}")

def upload_to_seatable(front_filename="longhu_data.csv", 
                      table_name=TABLE_NAME, 
                      server_url=SERVER_URL):
    try:
        # 认证
        base = Base(API_TOKEN, SERVER_URL)
        base.auth()

        # 读取 CSV 文件
        try:
            df = pd.read_csv(front_filename, dtype=str)
            df = df.fillna('')  # 将 NaN 替换为空字符串
            csv_columns = df.columns.tolist()

            # 转换为字典列表
            rows = df.to_dict('records')
            
            # 检查表是否存在，不存在则创建
            try:
                metadata = base.get_metadata()
                table_exists = any(table['name'] == table_name for table in metadata['tables'])
                
                if not table_exists:
                    print(f"表 {table_name} 不存在，开始创建...")
                    # 创建表和列
                    columns = []
                    for col_name in csv_columns:
                        columns.append({
                            "name": col_name,
                            "type": "text"
                        })
                    base.add_table(table_name, columns)
                    print(f"表 {table_name} 创建成功")
                    # 重新获取元数据
                    metadata = base.get_metadata()

                # 获取列信息
                table_info = next(table for table in metadata['tables'] if table['name'] == table_name)
                seatable_columns = [col['name'] for col in table_info['columns']]

                # 检查列名是否一致，不一致则更新 SeaTable 列名
                if csv_columns != seatable_columns:
                    print("检测到列名不一致，开始更新 SeaTable 列名...")
                    for i, csv_col in enumerate(csv_columns):
                        if i < len(seatable_columns):
                            try:
                                base.rename_column(table_name, seatable_columns[i], csv_col)
                                print(f"列名 {seatable_columns[i]} 已更新为 {csv_col}")
                            except Exception as e:
                                print(f"更新列名失败: {e}")
                                return False
                        else:
                            print(f"CSV 文件列数超出 SeaTable 列数，请手动添加列")
                    print("SeaTable 列名更新完成")

                # 如果表存在则清空
                try:
                    existing_rows = base.list_rows(table_name)
                    if existing_rows:
                        base.delete_rows(table_name, [row['_id'] for row in existing_rows])
                        print("已清空现有数据")
                except:
                    print("获取现有数据失败")

                # 批量上传数据
                batch_size = 50
                for i in range(0, len(rows), batch_size):
                    batch = rows[i:i + batch_size]
                    base.batch_append_rows(table_name, batch)
                    print(f"已上传: {min(i + batch_size, len(rows))}/{len(rows)} 条记录")

                print(f"数据更新完成，共更新 {len(rows)} 条记录")
                return True

            except Exception as e:
                print(f"表操作失败: {e}")
                return False

        except Exception as e:
            print(f"数据导入失败: {e}")
            return False

    except Exception as e:
        print(f"连接失败: {e}")
        return False

if __name__ == "__main__":
    print("龙虎榜数据抓取尝试运行,有头模式，请勿关闭弹出的浏览器")
    data = fetch_longhu_data()
    if data:
        save_to_separated_csv_extended(data)
        print("数据已保存到扩展后的 JSON 以及分离的 CSV 文件")
        
        # 上传数据到 SeaTable
        if upload_to_seatable():
            print("数据已成功上传到 SeaTable")
        else:
            print("数据上传到 SeaTable失败")
    else:
        print("未能获取数据")
