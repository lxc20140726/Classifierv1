import os
from pathlib import Path

def generate_directory_structure(
    start_path: str, 
    output_file: str, 
    max_items_per_level: int = 4,
    ignore_dirs: list = None,
    ignore_files: list = None
):
    """
    扫描目录结构并生成 Markdown 格式的树状图。
    
    Args:
        start_path: 要扫描的根目录路径
        output_file: 输出的 Markdown 文件路径
        max_items_per_level: 同层级最大显示项目数（文件+目录）
        ignore_dirs: 要忽略的目录名列表
        ignore_files: 要忽略的文件名列表
    """
    if ignore_dirs is None:
        ignore_dirs = ['.git', '__pycache__', '.idea', '.vscode', 'node_modules', 'venv', 'env']
    if ignore_files is None:
        ignore_files = ['.DS_Store', 'Thumbs.db']
        
    start_path = Path(start_path).resolve()
    lines = [f"# Directory Structure: {start_path.name}", ""]
    
    # 获取当前脚本的文件名，用于过滤
    current_script_name = Path(__file__).name

    def _scan(current_path: Path, prefix: str = ""):
        try:
            # 获取当前目录下的所有条目
            entries = list(current_path.iterdir())
        except PermissionError:
            return

        # 分离目录和文件，并进行过滤
        dirs = []
        files = []
        
        for entry in entries:
            if entry.is_dir():
                # 忽略明确指定的目录和所有以 . 开头的隐藏目录
                if entry.name not in ignore_dirs and not entry.name.startswith('.'):
                    dirs.append(entry)
            else:
                # 忽略列表、输出文件、以及脚本自身
                # 同时也忽略以 . 开头的隐藏文件
                entry_lower = entry.name.lower()
                script_lower = current_script_name.lower()
                output_lower = Path(output_file).name.lower()
                
                if (entry.name not in ignore_files and 
                    not entry.name.startswith('.') and
                    entry_lower != output_lower and 
                    entry_lower != script_lower and
                    entry_lower != 'generate_directory_structure.py'):
                    files.append(entry)
        
        # 排序
        dirs.sort(key=lambda x: x.name.lower())
        files.sort(key=lambda x: x.name.lower())
        
        # 目录全部显示，文件按扩展名分类显示
        # 定义常见的文件类型分组
        VIDEO_EXTS = {'.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v', '.ts'}
        IMAGE_EXTS = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp'}
        ARCHIVE_EXTS = {'.zip', '.rar', '.7z', '.tar', '.gz'}
        
        # 将文件按类型分组
        files_by_type = {
            'Videos': [],
            'Images': [],
            'Archives': [],
            'Others': []
        }
        
        for f in files:
            ext = f.suffix.lower()
            if ext in VIDEO_EXTS:
                files_by_type['Videos'].append(f)
            elif ext in IMAGE_EXTS:
                files_by_type['Images'].append(f)
            elif ext in ARCHIVE_EXTS:
                files_by_type['Archives'].append(f)
            else:
                files_by_type['Others'].append(f)
        
        # 构建显示列表
        display_items = []
        
        # 1. 先添加目录
        for d in dirs:
            display_items.append({'type': 'dir', 'obj': d})
            
        # 2. 按类别添加文件
        for category, category_files in files_by_type.items():
            if not category_files:
                continue
                
            # 显示类别标题
            # display_items.append({'type': 'header', 'text': f"[{category}]"})
            
            # 如果文件数量超过限制，显示部分文件和统计信息
            if len(category_files) > max_items_per_level:
                # 显示前N个
                for f in category_files[:max_items_per_level]:
                    display_items.append({'type': 'file', 'obj': f})
                # 显示省略信息
                display_items.append({'type': 'more', 'count': len(category_files) - max_items_per_level, 'category': category})
            else:
                # 全部显示
                for f in category_files:
                    display_items.append({'type': 'file', 'obj': f})

        for item in display_items:
            if item['type'] == 'dir':
                lines.append(f"{prefix}├── {item['obj'].name}/")
                _scan(item['obj'], prefix + "│   ")
            elif item['type'] == 'file':
                lines.append(f"{prefix}├── {item['obj'].name}")
            elif item['type'] == 'more':
                lines.append(f"{prefix}└── ... ({item['count']} more {item['category']} files)")
            # elif item['type'] == 'header':
            #     lines.append(f"{prefix}├── -- {item['text']} --")

    _scan(Path(start_path))
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    print(f"Directory structure generated: {output_file}")

def analyze_directory(path: str):
    """
    分析目录特征，返回统计信息，用于判断处理策略。
    """
    path = Path(path).resolve()
    stats = {
        'total_files': 0,
        'video_count': 0,
        'image_count': 0,
        'archive_count': 0,
        'subdir_count': 0,
        'root_video_count': 0,
        'root_image_count': 0,
        'root_archive_count': 0,
        'max_depth': 0,
        'has_nested_videos': False
    }
    
    VIDEO_EXTS = {'.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v', '.ts'}
    IMAGE_EXTS = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp'}
    ARCHIVE_EXTS = {'.zip', '.rar', '.7z', '.tar', '.gz'}
    
    for root, dirs, files in os.walk(path):
        # 忽略隐藏目录
        dirs[:] = [d for d in dirs if not d.startswith('.')]
        
        rel_path = Path(root).relative_to(path)
        depth = len(rel_path.parts)
        stats['max_depth'] = max(stats['max_depth'], depth)
        
        is_root = depth == 0
        if is_root:
            stats['subdir_count'] = len(dirs)
            
        for file in files:
            if file.startswith('.'): continue
            
            stats['total_files'] += 1
            ext = Path(file).suffix.lower()
            
            if ext in VIDEO_EXTS:
                stats['video_count'] += 1
                if is_root:
                    stats['root_video_count'] += 1
                elif depth >= 1:
                    stats['has_nested_videos'] = True
                    
            elif ext in IMAGE_EXTS:
                stats['image_count'] += 1
                if is_root:
                    stats['root_image_count'] += 1
                    
            elif ext in ARCHIVE_EXTS:
                stats['archive_count'] += 1
                if is_root:
                    stats['root_archive_count'] += 1
                    
    return stats

if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="Generate directory structure markdown.")
    parser.add_argument("path", nargs="?", default=".", help="Root directory to scan (default: current directory)")
    parser.add_argument("-o", "--output", default="DIRECTORY_STRUCTURE.md", help="Output file name (default: DIRECTORY_STRUCTURE.md)")
    parser.add_argument("-m", "--max", type=int, default=4, help="Max items per level (default: 4)")
    
    args = parser.parse_args()
    
    # 确保输出文件路径是绝对路径，或者相对于当前工作目录
    # 如果 args.path 是指定的，我们通常希望输出文件也在那个目录下，或者在当前目录下
    # 这里保持简单，输出文件默认在当前运行目录下，除非用户指定了路径
    
    # 但是，为了方便，如果用户指定了 path，我们可以将输出文件默认放到 path 下
    target_path = Path(args.path).resolve()
    if args.output == "DIRECTORY_STRUCTURE.md":
         output_path = target_path / args.output
    else:
         output_path = Path(args.output)

    print(f"Scanning directory: {target_path}")
    print(f"Output file: {output_path}")
    
    generate_directory_structure(str(target_path), str(output_path), args.max)
