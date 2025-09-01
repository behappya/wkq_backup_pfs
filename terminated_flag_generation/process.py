import os
import shutil
import json
import numpy as np
import pandas as pd
from pathlib import Path

# def calc_terminated_flag(actions, threshold=5.0):
#     """
#     从后向前计算终止标志。
#     如果当前action与第一帧action的diff小于阈值，则设置为1。
#     一旦出现大于阈值的情况，则当前action的flag设置为0，并且之前所有action的flag都为0。
#     """
#     if not actions:
#         return []
    
#     n_actions = len(actions)
#     flags = [0] * n_actions
#     first_act = np.array(actions[0])

#     for i in range(n_actions - 1, 0, -1):
#         current_act = np.array(actions[i])
#         diff = np.linalg.norm(current_act - first_act)
        
#         if diff < threshold:
#             flags[i] = 1
#         else:
#             flags[i] = 0
#             break
            
#     return flags
def calc_terminated_flag(actions, threshold=5.0):
    """
    从后向前计算终止标志。
    最后一帧flag为1，遍历过程中如果当前action与后一帧action的diff大于阈值，则设置为0，并且之前所有action的flag都为0。
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
        print("警告: info.json 不存在，已跳过。")
        return

    with open(src_file, 'r') as f:
        info_data = json.load(f)

    if 'features' in info_data and 'action' in info_data['features']:
        action_feature = info_data['features']['action']
        
        if action_feature.get('shape') == [6]:
            action_feature['shape'] = [7]
            print("info.json: action shape 已更新为 [7]。")

        if 'names' in action_feature and 'flag' not in action_feature['names']:
            action_feature['names'].append('flag')
            print("info.json: action names 已添加 'flag'。")

    with open(dst_file, 'w') as f:
        json.dump(info_data, f, indent=2)
    print("info.json 更新完成。")

def update_modality_json(src_file, dst_file):
    """
    读取modality.json，为action添加flag条目，并保存到新路径。
    """
    if not src_file.exists():
        print("警告: modality.json 不存在，已跳过。")
        return

    with open(src_file, 'r') as f:
        modality_data = json.load(f)

    if 'action' in modality_data and 'flag' not in modality_data['action']:
        modality_data['action']['flag'] = {
            "start": 6,
            "end": 7
        }
        print("modality.json: action modality 已更新，添加 'flag'。")

    with open(dst_file, 'w') as f:
        json.dump(modality_data, f, indent=4)
    print("modality.json 更新完成。")


def main(src_root, dst_root, threshold=5.0):
    """
    主处理函数。
    """
    src_path = Path(src_root)
    dst_path = Path(dst_root)

    if not src_path.is_dir():
        print(f"错误: 输入路径 {src_root} 不是一个有效的目录。")
        return

    # 1. 复制 videos 文件夹
    src_videos = src_path / 'videos'
    if src_videos.is_dir():
        shutil.copytree(src_videos, dst_path / 'videos', dirs_exist_ok=True)
        print("已复制 'videos' 文件夹。")

    # 2. 处理 data/chunk-000
    src_chunk_dir = src_path / 'data' / 'chunk-000'
    dst_chunk_dir = dst_path / 'data' / 'chunk-000'
    dst_chunk_dir.mkdir(parents=True, exist_ok=True)
    
    episode_flags = {}
    if src_chunk_dir.is_dir():
        print("正在处理 parquet 文件...")
        for parquet_file in sorted(src_chunk_dir.glob('episode_*.parquet')):
            dst_file = dst_chunk_dir / parquet_file.name
            flags = process_parquet_file(parquet_file, dst_file, threshold)
            if flags:
                ep_idx = int(parquet_file.stem.split('_')[1])
                episode_flags[ep_idx] = flags
        print("Parquet 文件处理完成。")

    # 3. 处理 meta 文件夹
    src_meta_dir = src_path / 'meta'
    dst_meta_dir = dst_path / 'meta'
    dst_meta_dir.mkdir(parents=True, exist_ok=True)

    if src_meta_dir.is_dir():
        # 复制不需修改的文件
        for fn in ['tasks.jsonl', 'episodes.jsonl']:
            if (src_meta_dir / fn).exists():
                shutil.copy2(src_meta_dir / fn, dst_meta_dir / fn)

        # 更新 info.json
        update_info_json(src_meta_dir / 'info.json', dst_meta_dir / 'info.json')

        # 更新 modality.json
        update_modality_json(src_meta_dir / 'modality.json', dst_meta_dir / 'modality.json')

        # 更新 episodes_stats.jsonl
        src_stats_file = src_meta_dir / 'episodes_stats.jsonl'
        dst_stats_file = dst_meta_dir / 'episodes_stats.jsonl'
        if src_stats_file.exists():
            print("正在更新 episodes_stats.jsonl...")
            with open(src_stats_file, 'r') as fin, open(dst_stats_file, 'w') as fout:
                for line in fin:
                    data = json.loads(line)
                    ep_idx = data.get('episode_index')
                    if ep_idx is not None and ep_idx in episode_flags:
                        new_line = process_stats_line(line, episode_flags[ep_idx])
                        fout.write(new_line + '\n')
                    else:
                        fout.write(line)
            print("episodes_stats.jsonl 更新完成。")

    print(f"\n处理完成！输出路径：{dst_root}")

if __name__ == "__main__":
    src_root = "/pfs/pfs-ahGxdf/data/xiezhengyuan/backup/block/tmp/0818-blk4-stack-triangle"
    dst_root = "/pfs/pfs-ahGxdf/data/xiezhengyuan/backup/block/terminate/0818-blk4-stack-triangle"
    main(src_root, dst_root, threshold=3.0)
# import json

# # 我有一个lerobot格式的数据集，下面分别包含三个folder：data、meta和videos。data下面只有一个子文件夹chunk-000，其中包含若干个parquet文件，文件名如同episode_000000.parquet，episode_000001.parquet...，每个parquet文件的每一行如下：
# # {"action":[-0.7029227018356323,-99.74500274658203,99.81859588623047,72.47386932373047,2.745098114013672,1.2638230323791504],"observation.state":[-5.301914691925049,-98.4803695678711,98.56437683105469,73.20490264892578,1.7516340017318726,0],"timestamp":0,"frame_index":0,"episode_index":0,"index":0,"task_index":0}。
# # 你首先需要为每行的action增加一个维度（取值为0或者1），用于判断当前episode是否终止，判断的条件为当前行与前一行的action的差值大于某个阈值，如果差值小于阈值，则当前episode终止（取值设置为1），否则当前episode不终止（取值设置为0）。parquet文件的其他字段不要更改。之后对于meta文件夹，tasks.jsonl和episodes.jsonl不修改，episodes_stats.jsonl每一行为对应的parquet的统计值，如下：{"episode_index": 0, "stats": {"action": {"min": [-38.91151428222656, -100.0, -57.33695602416992, -26.058488845825195, -1.0465724468231201, 0.0], "max": [17.80821990966797, 59.087066650390625, 99.909423828125, 75.29463195800781, 79.01622009277344, 29.86279296875], "mean": [-2.3179147243499756, -25.059614181518555, 40.52698516845703, 28.075695037841797, 38.99290084838867, 11.377933502197266], "std": [18.972305297851562, 42.388832092285156, 39.75901412963867, 37.7896614074707, 30.439119338989258, 10.472332954406738], "count": [402]}, "observation.state": {"min": [-38.954345703125, -99.3246078491211, -57.31047058105469, -24.7069034576416, -1.3041210174560547, 1.9554955959320068], "max": [17.23122215270996, 58.80118179321289, 99.36823272705078, 72.73121643066406, 78.61241149902344, 28.11867904663086], "mean": [-2.4512925148010254, -24.016870498657227, 41.51826477050781, 28.53316879272461, 38.91809844970703, 12.660223960876465], "std": [18.88246726989746, 42.80131530761719, 39.383018493652344, 37.202064514160156, 30.35059928894043, 8.9399995803833], "count": [402]}, "observation.images.front": {"min": [[[0.0]], [[0.0]], [[0.0]]], "max": [[[1.0]], [[1.0]], [[0.984313725490196]]], "mean": [[[0.4562639440359477]], [[0.45875765522875817]], [[0.4510968075980392]]], "std": [[[0.1688743508105245]], [[0.16425321618344368]], [[0.16930943527379153]]], "count": [100]}, "observation.images.wrist": {"min": [[[0.0]], [[0.0]], [[0.0]]], "max": [[[1.0]], [[1.0]], [[1.0]]], "mean": [[[0.5688105596405229]], [[0.5665782414215687]], [[0.5607094914215686]]], "std": [[[0.2846790629979371]], [[0.2833822538704273]], [[0.29036826555199224]]], "count": [100]}, "timestamp": {"min": [0.0], "max": [13.366666666666667], "mean": [6.683333333333333], "std": [3.8682348352628155], "count": [402]}, "frame_index": {"min": [0], "max": [401], "mean": [200.5], "std": [116.04704505788446], "count": [402]}, "episode_index": {"min": [1], "max": [1], "mean": [1.0], "std": [0.0], "count": [402]}, "index": {"min": [719], "max": [1120], "mean": [919.5], "std": [116.04704505788446], "count": [402]}, "task_index": {"min": [0], "max": [0], "mean": [0.0], "std": [0.0], "count": [402]}}}，请你将刚刚添加的那一个维度也更新到里面。video文件夹不做任何修改。将修改后的文件/文件夹以及没有修改的文件夹保存到新路径。

# import os
# import shutil
# import json
# import numpy as np
# import pandas as pd
# from pathlib import Path

# def calc_terminated_flag(actions, threshold=5.0):
#     """
#     从后向前计算终止标志。
#     """
#     if not actions:
#         return []
#     flags = [0] * len(actions)
#     for i in range(len(actions) - 1, 0, -1):
#         current_act = np.array(actions[i])
#         prev_act = np.array(actions[i - 1])
#         diff = np.linalg.norm(current_act - prev_act)
#         print(diff)
#         if diff < threshold:
#             flags[i] = 1
#     return flags

# def update_action_stats(stats, terminated_flags):
#     """
#     更新action统计信息，为新添加的终止标志维度添加统计数据。
#     """
#     arr = np.array(terminated_flags)
#     stats['min'].append(int(arr.min()) if arr.size > 0 else 0)
#     stats['max'].append(int(arr.max()) if arr.size > 0 else 0)
#     stats['mean'].append(float(arr.mean()) if arr.size > 0 else 0.0)
#     stats['std'].append(float(arr.std()) if arr.size > 0 else 0.0)
#     # 'count' 保持不变，因为它代表episode的长度
#     return stats

# def process_parquet_file(src, dst, threshold=5.0):
#     """
#     读取parquet文件，为action添加终止标志维度，并保存到新路径。
#     此版本更健壮，通过创建副本并正确处理数据类型来避免错误。
#     """
#     df = pd.read_parquet(src)
    
#     if 'action' not in df.columns or df['action'].empty:
#         print(f"警告: 文件 {src} 中没有 'action' 列或该列为空，直接复制。")
#         shutil.copy2(src, dst)
#         return []

#     actions = df['action'].tolist()

#     if not isinstance(actions[0], (list, np.ndarray)) or len(actions[0]) != 6:
#         print(f"警告: 文件 {src} 的 action 维度不为 6 或格式不正确，已跳过。")
#         shutil.copy2(src, dst)
#         return []

#     terminated_flags = calc_terminated_flag(actions, threshold)
    
#     # --- 关键修复 ---
#     # 显式地将每个action转换为list，以确保执行的是列表拼接而不是NumPy的按元素加法
#     new_actions = [list(original_action) + [flag] for original_action, flag in zip(actions, terminated_flags)]

#     output_df = df.copy()
#     output_df['action'] = new_actions

#     # 在保存前进行最终验证
#     final_dim = len(output_df['action'].iloc[0])
#     if final_dim != 7:
#         raise RuntimeError(f"处理文件 {src} 后，action维度错误，应为7，实际为 {final_dim}。")

#     output_df.to_parquet(dst, index=False)
    
#     return terminated_flags

# def process_stats_line(line, terminated_flags):
#     """
#     处理单行episodes_stats.jsonl数据。
#     """
#     data = json.loads(line)
#     if 'action' in data.get('stats', {}):
#         stats = data['stats']['action']
#         # 确保只更新一次
#         if len(stats['min']) == 6:
#             update_action_stats(stats, terminated_flags)
#     return json.dumps(data, ensure_ascii=False)

# def main(src_root, dst_root, threshold=5.0):
#     """
#     主处理函数。
#     """
#     src_path = Path(src_root)
#     dst_path = Path(dst_root)

#     if not src_path.is_dir():
#         print(f"错误: 输入路径 {src_root} 不是一个有效的目录。")
#         return

#     # 1. 复制 videos 文件夹
#     src_videos = src_path / 'videos'
#     if src_videos.is_dir():
#         shutil.copytree(src_videos, dst_path / 'videos', dirs_exist_ok=True)
#         print("已复制 'videos' 文件夹。")

#     # 2. 处理 data/chunk-000
#     src_chunk_dir = src_path / 'data' / 'chunk-000'
#     dst_chunk_dir = dst_path / 'data' / 'chunk-000'
#     dst_chunk_dir.mkdir(parents=True, exist_ok=True)
    
#     episode_flags = {}
#     if src_chunk_dir.is_dir():
#         print("正在处理 parquet 文件...")
#         for parquet_file in sorted(src_chunk_dir.glob('episode_*.parquet')):
#             dst_file = dst_chunk_dir / parquet_file.name
#             flags = process_parquet_file(parquet_file, dst_file, threshold)
#             if flags:
#                 ep_idx = int(parquet_file.stem.split('_')[1])
#                 episode_flags[ep_idx] = flags
#         print("Parquet 文件处理完成。")

#     # 3. 处理 meta 文件夹
#     src_meta_dir = src_path / 'meta'
#     dst_meta_dir = dst_path / 'meta'
#     dst_meta_dir.mkdir(parents=True, exist_ok=True)

#     if src_meta_dir.is_dir():
#         # 复制不需修改的文件
#         for fn in ['tasks.jsonl', 'episodes.jsonl']:
#             if (src_meta_dir / fn).exists():
#                 shutil.copy2(src_meta_dir / fn, dst_meta_dir / fn)

#         # 处理 episodes_stats.jsonl
#         src_stats_file = src_meta_dir / 'episodes_stats.jsonl'
#         dst_stats_file = dst_meta_dir / 'episodes_stats.jsonl'
#         if src_stats_file.exists():
#             print("正在更新 episodes_stats.jsonl...")
#             with open(src_stats_file, 'r') as fin, open(dst_stats_file, 'w') as fout:
#                 for line in fin:
#                     data = json.loads(line)
#                     ep_idx = data.get('episode_index')
#                     if ep_idx is not None and ep_idx in episode_flags:
#                         new_line = process_stats_line(line, episode_flags[ep_idx])
#                         fout.write(new_line + '\n')
#                     else:
#                         fout.write(line)
#             print("episodes_stats.jsonl 更新完成。")

#     print(f"\n处理完成！输出路径：{dst_root}")

# if __name__ == "__main__":
#     src_root = "/pfs/pfs-ahGxdf/data/xiezhengyuan/backup/block/0814-clean-blk0-copy"
#     dst_root = "/pfs/pfs-ahGxdf/data/xiezhengyuan/backup/block/terminate/0814-clean-blk0"
#     main(src_root, dst_root, threshold=5.0)