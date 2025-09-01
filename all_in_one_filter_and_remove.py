# process_datasets.py

import os
import argparse
import json
import shutil
import pandas as pd
from pathlib import Path
import subprocess
import sys

# --- 帮助函数 (来自 clean_and_copy_lerobot.py) ---

def load_jsonl(path):
    """加载一个 JSONL 文件。"""
    with open(path, 'r') as f:
        return [json.loads(l) for l in f if l.strip()]

def save_jsonl(path, lines):
    """保存数据到 JSONL 文件。"""
    with open(path, 'w') as f:
        for line in lines:
            f.write(json.dumps(line) + '\n')

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
            dataset_paths.append(Path(root))
            print(f"  [✅ 找到!] -> {root}")
    return dataset_paths

def run_video_validation(dataset_path: Path, validator_script_path: str):
    """
    运行外部视频验证脚本来生成 low_quality.txt。
    如果验证脚本路径为空，则只创建一个空的 low_quality.txt。
    """
    output_txt = dataset_path / "low_quality.txt"
    print(f"  STEP 1: 运行视频质检...")
    if validator_script_path and Path(validator_script_path).exists():
        try:
            print(f"    - 执行脚本: python {validator_script_path} {dataset_path}")
            # 注意：此命令会覆盖现有的 low_quality.txt
            subprocess.run(
                [sys.executable, validator_script_path, str(dataset_path)],
                check=True,
                capture_output=True,
                text=True
            )
            print(f"    - 视频质检完成, 结果保存在: {output_txt}")
        except subprocess.CalledProcessError as e:
            print(f"    - ⚠️ 视频质检脚本执行失败: {e}")
            print(f"    - STDOUT: {e.stdout}")
            print(f"    - STDERR: {e.stderr}")
            print(f"    - 将创建一个空的 low_quality.txt 文件继续执行。")
            output_txt.touch() # 创建一个空文件以防脚本失败
    else:
        print(f"    - 未提供有效的质检脚本路径，将创建一个空的 low_quality.txt。")
        output_txt.touch() # 创建空文件
    return output_txt

def combine_manual_removals(remove_txt_path: Path, manual_ids_str: str):
    """
    将手动指定的 IDs 追加到自动生成的 remove_txt 文件中。
    """
    if not manual_ids_str:
        print("  STEP 2: 无需添加手动移除的 episodes。")
        return

    print(f"  STEP 2: 添加手动指定的移除 IDs: {manual_ids_str}")
    # 读取已有的 IDs
    existing_ids = set()
    if remove_txt_path.exists():
        with open(remove_txt_path, "r") as f:
            existing_ids = set(line.strip() for line in f if line.strip())

    # 解析并添加新的 IDs
    manual_ids = {id.strip() for id in manual_ids_str.split(',') if id.strip()}
    all_ids = sorted(list(existing_ids.union(manual_ids)), key=int)

    # 写回文件
    with open(remove_txt_path, "w") as f:
        for episode_id in all_ids:
            f.write(f"{episode_id}\n")
    print(f"    - 成功合并移除列表到: {remove_txt_path}")


def clean_and_copy_dataset(src_root: Path, dst_root: Path, remove_txt: Path, cams: str, modality_file_path: Path):
    """
    清理并复制单个 LeRobot 数据集，同时更新 episode_index。
    这是 `clean_and_copy_lerobot.py` 的核心逻辑。
    """
    print(f"  STEP 3: 开始清理和复制...")
    print(f"    - 源路径: {src_root}")
    print(f"    - 目标路径: {dst_root}")

    cam_list = [cam.strip() for cam in cams.split(",") if cam.strip()]

    # 源路径
    src_data = src_root / "data/chunk-000"
    src_videos = {cam: src_root / f"videos/chunk-000/observation.images.{cam}" for cam in cam_list}
    src_meta = src_root / "meta"

    # 目标路径
    dst_data = dst_root / "data/chunk-000"
    dst_videos = {cam: dst_root / f"videos/chunk-000/observation.images.{cam}" for cam in cam_list}
    dst_meta = dst_root / "meta"

    # 创建目标目录
    for p in [dst_data, dst_meta] + list(dst_videos.values()):
        p.mkdir(parents=True, exist_ok=True)

    # 加载需要删除的 episode id 列表
    remove_ids = set()
    if remove_txt.exists():
        with open(remove_txt, "r") as f:
            # 格式化为6位补零字符串以便匹配
            remove_ids = set(f"{int(line.strip()):06d}" for line in f if line.strip())
        print(f"    - 将移除 {len(remove_ids)} 个 episodes: {sorted(list(remove_ids))}")
    else:
        print(f"    - 未找到移除列表文件 '{remove_txt}', 将复制所有 episodes。")

    # 加载 meta 文件
    episodes_path = src_meta / "episodes.jsonl"
    if not episodes_path.exists():
        print(f"    - ❌ 错误: 找不到元数据文件 {episodes_path}。跳过此数据集。")
        return
    episodes = load_jsonl(episodes_path)

    stats_path = src_meta / "episodes_stats.jsonl"
    if not stats_path.exists():
        print(f"    - ❌ 错误: 找不到元数据文件 {stats_path}。跳过此数据集。")
        return
    stats = load_jsonl(stats_path)

    # 保留未删除的 entries
    filtered = [
        (ep, st) for ep, st in zip(episodes, stats)
        if f"{ep['episode_index']:06d}" not in remove_ids
    ]
    
    if not filtered:
        print(f"    - ⚠️ 警告: 过滤后没有剩余的 episodes。跳过此数据集。")
        return

    # 按顺序处理剩下的 episode
    for new_idx, (ep, st) in enumerate(filtered):
        old_idx_str = f"{ep['episode_index']:06d}"
        new_idx_str = f"{new_idx:06d}"

        # 更新 JSON 中的 episode_index 字段
        ep["episode_index"] = new_idx
        st["episode_index"] = new_idx

        # 修改 parquet 中的 episode_index 字段
        old_parquet = src_data / f"episode_{old_idx_str}.parquet"
        new_parquet = dst_data / f"episode_{new_idx_str}.parquet"
        if old_parquet.exists():
            df = pd.read_parquet(old_parquet)
            if "episode_index" in df.columns:
                df["episode_index"] = new_idx
            else:
                print(f"    - ⚠️ 警告: 'episode_index' not found in {old_parquet.name}")
            df.to_parquet(new_parquet)

        # 拷贝对应视频文件
        for cam in cam_list:
            old_mp4 = src_videos[cam] / f"episode_{old_idx_str}.mp4"
            new_mp4 = dst_videos[cam] / f"episode_{new_idx_str}.mp4"
            if old_mp4.exists():
                shutil.copy2(old_mp4, new_mp4)

    # 保存更新后的 meta 文件
    save_jsonl(dst_meta / "episodes.jsonl", [ep for ep, _ in filtered])
    save_jsonl(dst_meta / "episodes_stats.jsonl", [st for _, st in filtered])

    # 复制其他元数据文件
    if modality_file_path.exists():
        shutil.copy2(modality_file_path, dst_meta / "modality.json")
    if (src_meta / "tasks.jsonl").exists():
        shutil.copy2(src_meta / "tasks.jsonl", dst_meta / "tasks.jsonl")

    # 更新 info.json
    info_path_src = src_meta / "info.json"
    info_path_dst = dst_meta / "info.json"
    if info_path_src.exists():
        with open(info_path_src, 'r') as f:
            info = json.load(f)
        info["total_episodes"] = len(filtered)
        info["total_videos"] = len(cam_list) * len(filtered)
        info["splits"]["train"] = f"0:{len(filtered)}"
        with open(info_path_dst, 'w') as f:
            json.dump(info, f, indent=2)

    print(f"    - ✔️ 清理和复制完成！共保留 {len(filtered)} 个 episodes。")
    print(f"    - ❗ 请再次检查 {dst_meta / 'tasks.jsonl'} 的映射是否正确。")


def main():
    parser = argparse.ArgumentParser(
        description="自动化查找、质检和清理 LeRobot 数据集的流水线。",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "--src_base_path", type=str, required=True,
        help="要开始搜索的源根目录路径。\n例如: /pfs/pfs-ahGxdf/data/collect_data/so101"
    )
    parser.add_argument(
        "--dst_base_path", type=str, required=True,
        help="用于存放已处理数据集的目标根目录路径。\n例如: /pfs/pfs-ahGxdf/data/xiezhengyuan/backup/generalizable_pick_place_processed"
    )
    parser.add_argument(
        "--search_dirs", type=str, default="*",
        help="在 src_base_path 下要搜索的子目录，用逗号分隔。\n支持通配符 '*'。默认为 '*' (搜索所有子目录)。\n例如: blk0,blk3"
    )
    parser.add_argument(
        "--modality_path", type=str, required=True,
        help="通用的 modality.json 文件路径。\n例如: /path/to/modality.json"
    )
    parser.add_argument(
        "--cams", type=str, default="front,wrist",
        help="逗号分隔的相机名称列表，默认为 'front,wrist'。"
    )
    parser.add_argument(
        "--validator_script", type=str, default=None,
        help="(可选) 用于视频质检的 Python 脚本路径。\n该脚本应接受一个数据集路径作为参数，并在该路径下生成 'low_quality.txt'。\n例如: video_check/validate_videos.py"
    )
    parser.add_argument(
        "--manual_remove", type=json.loads, default={},
        help="一个JSON字符串，用于指定手动移除的 episode ID。\n键是相对于 src_base_path 的数据集路径，值是逗号分隔的ID字符串。\n示例: '{\"blk0/20250825_blk0\": \"10,25\", \"blk3/another_data\": \"5\"}'"
    )
    
    args = parser.parse_args()

    # 将逗号分隔的搜索目录转换为列表
    search_dirs = [d.strip() for d in args.search_dirs.split(',')]

    all_found_datasets = []
    for directory in search_dirs:
        search_path = Path(args.src_base_path) / directory
        if '*' not in directory and '?' not in directory:
             all_found_datasets.extend(find_dataset_folders(search_path))
        else:
            for matching_dir in Path(args.src_base_path).glob(directory):
                if matching_dir.is_dir():
                    all_found_datasets.extend(find_dataset_folders(matching_dir))
    
    if not all_found_datasets:
        print("\n❌ 未找到任何符合条件的数据集文件夹。请检查 --src_base_path 和 --search_dirs 参数。")
        return
        
    print(f"\n✨ 总共找到 {len(all_found_datasets)} 个数据集，即将开始处理...\n" + "="*80)

    for i, src_path in enumerate(all_found_datasets):
        print(f"\n({i+1}/{len(all_found_datasets)}) 正在处理: {src_path}")
        print("-" * 60)

        # ### 新增逻辑 ###
        # 1. 构建目标路径并检查是否已存在。如果存在，则跳过。
        relative_path_str = str(src_path.relative_to(args.src_base_path))
        dst_path = Path(args.dst_base_path) / relative_path_str
        
        if dst_path.is_dir():
            print(f"  🟡 目标目录已存在，跳过处理: {dst_path}")
            continue
        # ### 逻辑结束 ###

        # 2. 运行视频质检，获取 remove_txt 路径
        remove_txt_path = run_video_validation(src_path, args.validator_script)

        # 3. 结合手动指定的移除列表
        manual_ids_for_this_dataset = args.manual_remove.get(relative_path_str, "")
        combine_manual_removals(remove_txt_path, manual_ids_for_this_dataset)

        # 4. 执行清理和复制
        try:
            clean_and_copy_dataset(
                src_root=src_path,
                dst_root=dst_path,
                remove_txt=remove_txt_path,
                cams=args.cams,
                modality_file_path=Path(args.modality_path)
            )
        except Exception as e:
            print(f"    - ❌ 处理数据集 {src_path} 时发生严重错误: {e}")
            import traceback
            traceback.print_exc()

    print("\n" + "="*80 + f"\n🎉 全部处理完成！共处理了 {len(all_found_datasets)} 个数据集。")


if __name__ == "__main__":
    main()