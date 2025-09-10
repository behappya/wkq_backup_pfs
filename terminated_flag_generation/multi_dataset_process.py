# process_datasets_recursively.py

import os
import shutil
import json
import numpy as np
import pandas as pd
import argparse
from pathlib import Path

# ==============================================================================
# --- 帮助函数 (来自 process.py) ---
# ==============================================================================

def calc_terminated_flag(actions, threshold=5.0):
    """
    从后向前计算终止标志。
    最后一帧flag为1，遍历过程中如果当前action与后一帧action的diff大于阈值，
    则当前帧及之前所有帧的flag都设置为0。
    """
    if not actions:
        return []
    n_actions = len(actions)
    flags = [1] * n_actions  # 默认全部为1
    for i in range(n_actions - 2, -1, -1):  # 从倒数第二帧往前
        curr = np.array(actions[i])
        next_ = np.array(actions[i + 1])
        diff = np.linalg.norm(curr - next_)
        if diff > threshold:
            # 当前及之前所有帧都置为0
            for j in range(i + 1):
                flags[j] = 0
            break
    return flags

def update_action_stats(stats, terminated_flags):
    """
    更新action统计信息，为新添加的终止标志维度添加统计数据。
    """
    arr = np.array(terminated_flags)
    stats['min'].append(int(arr.min()) if arr.size > 0 else 0)
    stats['max'].append(int(arr.max()) if arr.size > 0 else 0)
    stats['mean'].append(float(arr.mean()) if arr.size > 0 else 0.0)
    stats['std'].append(float(arr.std()) if arr.size > 0 else 0.0)
    return stats

def process_parquet_file(src, dst, threshold=5.0):
    """
    读取parquet文件，为action添加终止标志维度，并保存到新路径。
    """
    df = pd.read_parquet(src)
    
    if 'action' not in df.columns or df['action'].empty:
        shutil.copy2(src, dst)
        return []

    actions = df['action'].tolist()

    if not isinstance(actions[0], (list, np.ndarray)) or len(actions[0]) != 6:
        shutil.copy2(src, dst)
        return []

    terminated_flags = calc_terminated_flag(actions, threshold)
    
    new_actions = [list(original_action) + [flag] for original_action, flag in zip(actions, terminated_flags)]

    output_df = df.copy()
    output_df['action'] = new_actions

    if len(output_df['action'].iloc[0]) != 7:
        raise RuntimeError(f"处理文件 {src} 后，action维度错误，应为7。")

    output_df.to_parquet(dst, index=False)
    
    return terminated_flags

def process_stats_line(line, terminated_flags):
    """
    处理单行episodes_stats.jsonl数据。
    """
    data = json.loads(line)
    if 'action' in data.get('stats', {}):
        stats = data['stats']['action']
        if len(stats['min']) == 6:
            update_action_stats(stats, terminated_flags)
    return json.dumps(data, ensure_ascii=False)

def update_info_json(src_file, dst_file):
    """
    读取info.json，更新action的shape和names，并保存到新路径。
    """
    if not src_file.exists():
        print("    - ⚠️  警告: info.json 不存在，已跳过。")
        return

    with open(src_file, 'r', encoding='utf-8') as f:
        info_data = json.load(f)

    if 'features' in info_data and 'action' in info_data['features']:
        action_feature = info_data['features']['action']
        
        if action_feature.get('shape') == [6]:
            action_feature['shape'] = [7]
        if 'names' in action_feature and 'flag' not in action_feature['names']:
            action_feature['names'].append('flag')

    with open(dst_file, 'w', encoding='utf-8') as f:
        json.dump(info_data, f, indent=2, ensure_ascii=False)
    print("    - info.json 更新完成。")

def update_modality_json(src_file, dst_file):
    """
    读取modality.json，为action添加flag条目，并保存到新路径。
    """
    if not src_file.exists():
        print("    - ⚠️  警告: modality.json 不存在，已跳过。")
        return

    with open(src_file, 'r', encoding='utf-8') as f:
        modality_data = json.load(f)

    if 'action' in modality_data and 'flag' not in modality_data['action']:
        modality_data['action']['flag'] = {"start": 6, "end": 7}

    with open(dst_file, 'w', encoding='utf-8') as f:
        json.dump(modality_data, f, indent=4, ensure_ascii=False)
    print("    - modality.json 更新完成。")


# ==============================================================================
# --- 核心逻辑函数 (合并后的) ---
# ==============================================================================

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
            # 防止重复查找子目录中的数据集
            dirs[:] = [d for d in dirs if d not in required_subdirs]
    return dataset_paths

def process_single_dataset(src_path: Path, dst_path: Path, threshold: float):
    """
    对单个源数据集进行处理，并将结果保存到目标路径。
    """
    if not src_path.is_dir():
        print(f"    - ❌ 错误: 输入路径 {src_path} 不是一个有效的目录。")
        return

    # 1. 复制 videos 文件夹
    src_videos = src_path / 'videos'
    if src_videos.is_dir():
        shutil.copytree(src_videos, dst_path / 'videos', dirs_exist_ok=True)
        print("    - 'videos' 文件夹已复制。")

    # 2. 处理 data 文件夹
    src_chunk_dir = src_path / 'data' / 'chunk-000'
    dst_chunk_dir = dst_path / 'data' / 'chunk-000'
    dst_chunk_dir.mkdir(parents=True, exist_ok=True)
    
    episode_flags = {}
    if src_chunk_dir.is_dir():
        print("    - 正在处理 parquet 文件...")
        for parquet_file in sorted(src_chunk_dir.glob('episode_*.parquet')):
            dst_file = dst_chunk_dir / parquet_file.name
            flags = process_parquet_file(parquet_file, dst_file, threshold)
            if flags:
                ep_idx = int(parquet_file.stem.split('_')[1])
                episode_flags[ep_idx] = flags
        print("    - Parquet 文件处理完成。")

    # 3. 处理 meta 文件夹
    src_meta_dir = src_path / 'meta'
    dst_meta_dir = dst_path / 'meta'
    dst_meta_dir.mkdir(parents=True, exist_ok=True)

    if src_meta_dir.is_dir():
        # 复制不需修改的文件
        for fn in ['tasks.jsonl', 'episodes.jsonl']:
            if (src_meta_dir / fn).exists():
                shutil.copy2(src_meta_dir / fn, dst_meta_dir / fn)

        # 更新 info.json, modality.json, episodes_stats.jsonl
        update_info_json(src_meta_dir / 'info.json', dst_meta_dir / 'info.json')
        update_modality_json(src_meta_dir / 'modality.json', dst_meta_dir / 'modality.json')
        
        src_stats_file = src_meta_dir / 'episodes_stats.jsonl'
        dst_stats_file = dst_meta_dir / 'episodes_stats.jsonl'
        if src_stats_file.exists():
            print("    - 正在更新 episodes_stats.jsonl...")
            with open(src_stats_file, 'r', encoding='utf-8') as fin, open(dst_stats_file, 'w', encoding='utf-8') as fout:
                for line in fin:
                    data = json.loads(line)
                    ep_idx = data.get('episode_index')
                    if ep_idx is not None and ep_idx in episode_flags:
                        new_line = process_stats_line(line, episode_flags[ep_idx])
                        fout.write(new_line + '\n')
                    else:
                        fout.write(line)
            print("    - episodes_stats.jsonl 更新完成。")

def main():
    parser = argparse.ArgumentParser(
        description="自动化查找并处理 LeRobot 数据集，为 action 添加终止标志。",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "--src_base_path", type=str, required=True,
        help="要开始搜索的源根目录路径。\n例如: /path/to/raw_data"
    )
    parser.add_argument(
        "--dst_base_path", type=str, required=True,
        help="用于存储处理后数据集的目标根目录路径。\n将保持与源目录相同的子目录结构。\n例如: /path/to/processed_data"
    )
    parser.add_argument(
        "--search_dirs", type=str, default="*",
        help="在 src_base_path 下要搜索的子目录，用逗号分隔。\n支持通配符 '*'。默认为 '*' (搜索所有子目录)。\n例如: blk0,blk3,task_*"
    )
    parser.add_argument(
        "--threshold", type=float, default=3.0,
        help="用于判断终止的 action 差异阈值。默认: 3.0"
    )
    
    args = parser.parse_args()

    # 查找所有数据集
    search_dirs = [d.strip() for d in args.search_dirs.split(',')]
    all_found_datasets = []
    for directory in search_dirs:
        search_path_pattern = Path(args.src_base_path) / directory
        if '*' not in directory and '?' not in directory:
            if search_path_pattern.is_dir():
                all_found_datasets.extend(find_dataset_folders(search_path_pattern))
        else:
            for matching_dir in Path(args.src_base_path).glob(directory):
                if matching_dir.is_dir():
                    all_found_datasets.extend(find_dataset_folders(matching_dir))
    
    if not all_found_datasets:
        print("\n❌ 未找到任何符合条件的数据集文件夹。请检查 --src_base_path 和 --search_dirs 参数。")
        return
        
    print(f"\n✨ 总共找到 {len(all_found_datasets)} 个数据集，即将开始处理...\n" + "="*80)

    processed_count = 0
    skipped_count = 0

    for i, src_dataset_path in enumerate(all_found_datasets):
        print(f"\n({i+1}/{len(all_found_datasets)}) 检查数据集: {src_dataset_path}")
        print("-" * 60)

        # 计算目标路径，保持目录结构
        relative_path = src_dataset_path.relative_to(args.src_base_path)
        dst_dataset_path = Path(args.dst_base_path) / relative_path

        # 跳过特殊或已存在的目标
        if 'merged' in str(src_dataset_path):
            print(f"    - [➡️ 跳过] 原因: 数据集名称包含 'merged'。")
            skipped_count += 1
            continue
        if dst_dataset_path.exists():
            print(f"    - [➡️ 跳过] 原因: 目标路径 {dst_dataset_path} 已存在。")
            skipped_count += 1
            continue
        
        print(f"    - [⚙️ 处理中] -> 输出到: {dst_dataset_path}")
        
        try:
            process_single_dataset(src_dataset_path, dst_dataset_path, args.threshold)
            processed_count += 1
            print(f"    - [✅ 完成]")
        except Exception as e:
            print(f"    - [❌ 失败] 处理过程中发生错误: {e}")
            # 如果处理失败，可以选择删除不完整的输出目录
            if dst_dataset_path.exists():
                print(f"    - 正在清理不完整的输出目录: {dst_dataset_path}")
                shutil.rmtree(dst_dataset_path)


    print("\n" + "="*80)
    print("🎉 全部处理完成！")
    print(f"   - 总共找到数据集: {len(all_found_datasets)}")
    print(f"   - 成功处理数据集: {processed_count}")
    print(f"   - 跳过的数据集:   {skipped_count}")
    print("="*80)


if __name__ == "__main__":
    main()