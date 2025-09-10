# calculate_dataset_stats.py

import os
import argparse
import json
from pathlib import Path

# --- 帮助函数 ---

def load_jsonl(path):
    """加载一个 JSONL 文件。"""
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return [json.loads(l) for l in f if l.strip()]
    except (json.JSONDecodeError, FileNotFoundError) as e:
        print(f"    - ⚠️  读取或解析文件 {path} 时出错: {e}")
        return []

# --- 核心逻辑函数 ---

def find_dataset_folders(base_path):
    """
    在指定的基础路径下递归查找所有的数据集文件夹。
    一个“数据集文件夹”被定义为同时包含 'videos', 'meta', 和 'data' 这三个子文件夹的目录。
    """
    required_subdirs = {'videos', 'meta', 'data'}
    dataset_paths = []
    print(f"\n🔍 开始在 '{os.path.abspath(base_path)}' 中搜索数据集...\n")
    for root, dirs, _ in os.walk(base_path):
        dir_set = set(dirs)
        if required_subdirs.issubset(dir_set):
            dataset_path = Path(root)
            dataset_paths.append(dataset_path)
            print(f"  [✅ 找到!] -> {dataset_path}")
    return dataset_paths

def calculate_stats_for_dataset(dataset_path: Path):
    """
    计算单个数据集的 episode 数量和总帧数。
    """
    episodes_path = dataset_path / "meta" / "episodes.jsonl"
    
    if not episodes_path.exists():
        print(f"    - ❌ 错误: 找不到元数据文件 {episodes_path}。")
        return 0, 0

    episodes_data = load_jsonl(episodes_path)
    
    num_episodes = len(episodes_data)
    total_frames = 0
    
    for episode in episodes_data:
        # 使用 .get() 方法安全地获取 'length' 键，如果不存在则默认为 0
        total_frames += episode.get('length', 0)
        
    return num_episodes, total_frames


def main():
    parser = argparse.ArgumentParser(
        description="自动化查找 LeRobot 数据集并统计其总 episode 数量和总帧数。",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "--src_base_path", type=str, required=True,
        help="要开始搜索的源根目录路径。\n例如: /pfs/pfs-ahGxdf/data/collect_data/so101"
    )
    parser.add_argument(
        "--search_dirs", type=str, default="*",
        help="在 src_base_path 下要搜索的子目录，用逗号分隔。\n支持通配符 '*'。默认为 '*' (搜索所有子目录)。\n例如: blk0,blk3"
    )
    
    args = parser.parse_args()

    # 将逗号分隔的搜索目录转换为列表
    search_dirs = [d.strip() for d in args.search_dirs.split(',')]

    all_found_datasets = []
    for directory in search_dirs:
        # 使用 Path.glob 支持通配符
        search_path_pattern = Path(args.src_base_path) / directory
        # glob() 不会直接返回路径本身，所以如果不是通配符模式，需要单独处理
        if '*' not in directory and '?' not in directory:
             if search_path_pattern.is_dir():
                all_found_datasets.extend(find_dataset_folders(search_path_pattern))
        else:
            # 搜索匹配通配符的目录
            for matching_dir in Path(args.src_base_path).glob(directory):
                if matching_dir.is_dir():
                    all_found_datasets.extend(find_dataset_folders(matching_dir))
    
    if not all_found_datasets:
        print("\n❌ 未找到任何符合条件的数据集文件夹。请检查 --src_base_path 和 --search_dirs 参数。")
        return
        
    print(f"\n✨ 总共找到 {len(all_found_datasets)} 个数据集，即将开始统计...\n" + "="*80)

    grand_total_episodes = 0
    grand_total_frames = 0

    for i, src_path in enumerate(all_found_datasets):
        if 'merged' in str(src_path):
            print(f"\n({i+1}/{len(all_found_datasets)}) 跳过合并数据集: {src_path}")
            continue
        print(f"\n({i+1}/{len(all_found_datasets)}) 正在统计: {src_path}")
        print("-" * 60)

        num_episodes, total_frames = calculate_stats_for_dataset(src_path)
        
        if num_episodes > 0 or total_frames > 0:
            print(f"    - 本数据集 Episode 数量: {num_episodes}")
            print(f"    - 本数据集总帧数: {total_frames}")
            grand_total_episodes += num_episodes
            grand_total_frames += total_frames
        else:
            print(f"    - 未能从此数据集中统计到有效数据。")


    print("\n" + "="*80)
    print("🎉 统计完成！")
    print(f"   - 总共扫描数据集数量: {len(all_found_datasets)}")
    print(f"   - 所有数据集总 Episode 数量: {grand_total_episodes}")
    print(f"   - 所有数据集总帧数 (Total Frames): {grand_total_frames}")
    print("="*80)


if __name__ == "__main__":
    main()