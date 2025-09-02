import os

def find_dataset_folders(base_path):
    """
    在指定的基础路径下递归查找所有的数据集文件夹。
    一个“数据集文件夹”被定义为同时包含 'videos', 'meta', 和 'data' 这三个子文件夹的目录。

    Args:
        base_path (str): 要开始搜索的根目录路径。

    Returns:
        list: 包含所有找到的数据集文件夹绝对路径的列表。
    """
    # 定义一个数据集文件夹必须包含的子文件夹名称集合
    required_subdirs = {'videos', 'meta', 'data'}
    dataset_paths = []

    # os.walk() 会遍历指定路径下的所有目录和文件
    # root: 当前正在遍历的文件夹路径
    # dirs: root文件夹中包含的子文件夹列表
    # files: root文件夹中包含的文件列表
    print(f"开始在 '{os.path.abspath(base_path)}' 中搜索...\n")
    for root, dirs, files in os.walk(base_path):
        # 将当前文件夹的子目录列表转换为集合，以便高效查询
        dir_set = set(dirs)
        
        # 检查必需的子文件夹是否都是当前目录的子集
        if required_subdirs.issubset(dir_set):
            # 如果是，说明 root 就是一个我们想要的数据集文件夹
            dataset_paths.append(root)
            print(f"  [找到!] -> {root}")

    return dataset_paths

# --- 使用示例 ---
if __name__ == "__main__":
    # ！！！请将这里的路径替换为你的实际路径！！！
    # 例如在 Windows 上: search_path = r"D:\my_data_collection"
    # 例如在 Linux/macOS 上: search_path = "/home/user/data"

    your_base_path = "/pfs/pfs-ahGxdf/data/collect_data/so101"  # 使用 "." 代表在当前目录下进行搜索作为演示
    NAME_LIST = ['blk0', 'blk3']
    for name in NAME_LIST:
        found_folders = find_dataset_folders(your_base_path + "/" + name)

