import argparse
from pathlib import Path
import json
import shutil
import pandas as pd
import random

def load_jsonl(path):
    with open(path, 'r') as f:
        return [json.loads(l) for l in f if l.strip()]


def save_jsonl(path, lines):
    with open(path, 'w') as f:
        for line in lines:
            f.write(json.dumps(line) + '\n')


def main(args):
    # 路径设置
    src_root = Path(args.src_root)
    dst_root = Path(args.dst_root)
    raise NotImplementedError("与clean and copy进行同步")

    # 源路径
    src_data = src_root / "data/chunk-000"
    src_front = src_root / "videos/chunk-000/observation.images.front"
    src_wrist = src_root / "videos/chunk-000/observation.images.wrist"
    src_meta = src_root / "meta"

    # 目标路径
    dst_data = dst_root / "data/chunk-000"
    dst_front = dst_root / "videos/chunk-000/observation.images.front"
    dst_wrist = dst_root / "videos/chunk-000/observation.images.wrist"
    dst_meta = dst_root / "meta"

    # 创建目标目录
    for p in [dst_data, dst_front, dst_wrist, dst_meta]:
        p.mkdir(parents=True, exist_ok=True)



    # 加载 meta 文件
    episodes = load_jsonl(src_meta / "episodes.jsonl")
    stats = load_jsonl(src_meta / "episodes_stats.jsonl")
    # import pdb; pdb.set_trace()
    save_ids = random.sample(range(len(episodes)), args.num)
    save_ids = [f"{i:06d}" for i in save_ids]
    filtered = [
        (ep, st) for ep, st in zip(episodes, stats)
        if f"{ep['episode_index']:06d}" in save_ids
    ]


    # 按顺序处理剩下的 episode
    for new_idx, (ep, st) in enumerate(filtered):
        old_idx_str = f"{ep['episode_index']:06d}"
        new_idx_str = f"{new_idx:06d}"

        # 更新 JSON 中的 episode_index 字段
        ep["episode_index"] = new_idx
        st["episode_index"] = new_idx

        # === 修改 parquet 中的 episode_index 字段 ===
        old_parquet = src_data / f"episode_{old_idx_str}.parquet"
        new_parquet = dst_data / f"episode_{new_idx_str}.parquet"
        if old_parquet.exists():
            df = pd.read_parquet(old_parquet)
            if "episode_index" in df.columns:
                df["episode_index"] = new_idx
            else:
                print(f"⚠️ Warning: 'episode_index' not found in {old_parquet.name}")
            df.to_parquet(new_parquet)
            print(f"✔️ Saved {new_parquet.name} with updated episode_index = {new_idx}")

        # === 拷贝对应视频文件（不做修改） ===
        for cam_src, cam_dst in [(src_front, dst_front), (src_wrist, dst_wrist)]:
            old_mp4 = cam_src / f"episode_{old_idx_str}.mp4"
            new_mp4 = cam_dst / f"episode_{new_idx_str}.mp4"
            if old_mp4.exists():
                shutil.copy2(old_mp4, new_mp4)

    # === 保存更新后的 meta 文件 ===
    save_jsonl(dst_meta / "episodes.jsonl", [ep for ep, _ in filtered])
    save_jsonl(dst_meta / "episodes_stats.jsonl", [st for _, st in filtered])

    # === 更新 info.json 中 total_episodes 字段 ===
    info_path_src = src_meta / "info.json"
    info_path_dst = dst_meta / "info.json"
    with open(info_path_src, 'r') as f:
        info = json.load(f)
    info["total_episodes"] = len(filtered)
    with open(info_path_dst, 'w') as f:
        json.dump(info, f, indent=2)

    print(f"\n✅ 清理和复制完成！共保留 {len(filtered)} 个 episodes，编号从 000000 开始。")
    print(f"📁 输出保存路径: {dst_root}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clean and copy LeRobot dataset with episode_index updated")
    parser.add_argument("--src_root", type=str, required=True, help="Path to source LeRobot folder")
    parser.add_argument("--dst_root", type=str, required=True, help="Path to output folder")
    parser.add_argument("--num", type=int, required=True, help="subset size")

    args = parser.parse_args()
    main(args)
