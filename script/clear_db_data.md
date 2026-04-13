# 清空数据库数据（保留表结构）

脚本路径：`script/clear_db_data.ps1`

## 功能

- 自动连接 SQLite 数据库
- 自动枚举所有非系统表（排除 `sqlite_%`）
- 在事务内清空所有表数据
- 保留全部字段和表结构
- 输出各表清理后的记录数

## 默认数据库路径

`E:\CodeBase\Classifier\Classifier\.local\config\classifier.db`

## 用法

在仓库根目录执行：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\script\clear_db_data.ps1
```

指定数据库路径：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\script\clear_db_data.ps1 -DbPath "E:\path\to\your.db"
```

## 注意

- 该脚本会删除表内全部数据，请先确认目标库路径。
- 脚本不会删除表，也不会变更字段结构。
