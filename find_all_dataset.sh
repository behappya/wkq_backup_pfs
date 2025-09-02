#!/bin/bash

PARENT_BASE_PATH="/pfs/pfs-ahGxdf/data/collect_data/so101"
PARENT_TARGET_PATH="/pfs/pfs-ahGxdf/data/xiezhengyuan/backup/generalizable_pick_place_processed"
# 定义要遍历的子目录名称列表
NAME_LIST=(
    "blk0"
    "blk3"
    # 添加更多需要处理的名称
)

# 遍历列表中的每个名字
for NAME in "${NAME_LIST[@]}"; do
    BASE_PATH="${PARENT_BASE_PATH}/${NAME}"

    echo "处理路径: $BASE_PATH"

    # 检查提供的路径是否存在并且是一个目录
    if [ ! -d "$BASE_PATH" ]; then
        echo "错误: 路径 '$BASE_PATH' 不存在或不是一个目录。"
        echo "跳过此路径..."
        echo "--------------------------------------------------"
        continue
    fi

    # 获取绝对路径以方便查看
    ABS_PATH=$(realpath "$BASE_PATH")
    echo "将在 '$ABS_PATH' 目录中进行搜索..."
    echo "--------------------------------------------------"

    # ==============================================================================
    # 方法一：查找所有符合条件的数据集文件夹（无深度限制）
    # ==============================================================================
    echo "方法一：查找所有数据集文件夹（不限嵌套深度）"

    # 使用 mapfile + process substitution 收集 find 结果
    mapfile -t found_folders < <(find "$BASE_PATH" -type d \
        -exec test -d "{}/videos" \; \
        -exec test -d "{}/meta" \; \
        -exec test -d "{}/data" \; \
        -print)

    # 打印结果
    if [ ${#found_folders[@]} -gt 0 ]; then
        echo "在 '$ABS_PATH' 中找到了 ${#found_folders[@]} 个数据集文件夹："
        for folder in "${found_folders[@]}"; do
            echo "  - $folder"
        done
    else
        echo "在 '$ABS_PATH' 中未找到任何符合条件的数据集文件夹。"
    fi

    echo "--------------------------------------------------"
done

echo "所有路径处理完成。"



# #!/bin/bash

# # --- 脚本功能 ---
# # 在指定的根目录下递归查找所有的数据集文件夹。
# # 数据集文件夹的定义是：一个同时包含 'videos', 'meta', 和 'data' 三个子目录的文件夹。

# # --- 检查输入参数 ---
# # 如果用户没有提供路径，则默认使用当前目录


# PARENT_BASE_PATH="/pfs/pfs-ahGxdf/data/collect_data/so101" 
# BASE_PATH=xxxxxx

# # 检查提供的路径是否存在并且是一个目录
# if [ ! -d "$BASE_PATH" ]; then
#     echo "错误: 路径 '$BASE_PATH' 不存在或不是一个目录。"
#     echo "用法: $0 [要搜索的路径]"
#     exit 1
# fi

# # 获取绝对路径以方便查看
# ABS_PATH=$(realpath "$BASE_PATH")
# echo "将在 '$ABS_PATH' 目录中进行搜索..."
# echo "--------------------------------------------------"


# # ==============================================================================
# # 方法一：查找所有符合条件的数据集文件夹（无深度限制）
# # ==============================================================================
# echo "方法一：查找所有数据集文件夹（不限嵌套深度）"

# # - `find "$BASE_PATH" -type d` : 查找指定路径下的所有目录。
# # - `-exec test -d "{}/videos" \;` : 对找到的每个目录(`{}`)，测试其下是否存在名为 "videos" 的子目录。
# # - `test -d "{}/meta"` 和 `test -d "{}/data"` 同理。
# # - `find` 会按顺序执行这些测试，只有所有测试都通过的目录才会被 `-print` 打印出来。
# #   这种方法比管道和循环更高效，因为它完全由 `find` 内部处理。

# # 使用一个数组来存储结果
# mapfile -t found_folders < <(find "$BASE_PATH" -type d \
#     -exec test -d "{}/videos" \; \
#     -exec test -d "{}/meta" \; \
#     -exec test -d "{}/data" \; \
#     -print)

# # 打印结果
# if [ ${#found_folders[@]} -gt 0 ]; then
#     echo "找到了 ${#found_folders[@]} 个数据集文件夹："
#     for folder in "${found_folders[@]}"; do
#         echo "  - $folder"
#     done
# else
#     echo "未找到任何符合条件的数据集文件夹。"
# fi

# echo "--------------------------------------------------"







# # FOLDER_NAME=blk0/20250825_blk0
# # DATA_PATH=/pfs/pfs-ahGxdf/data/collect_data/so101/${FOLDER_NAME} # 需要填，原始路径
# # TARGET_PATH=/pfs/pfs-ahGxdf/data/xiezhengyuan/backup/generalizable_pick_place_processed/${FOLDER_NAME} # 需要填，待生成的目标路径




# # MODALITY_PATH=/pfs/pfs-ahGxdf/data/xiezhengyuan/backup/modality_files/so101_front_wrist_state-dim6_action-dim6/modality.json # 通常不需要改
# # # 手工检查出错的视频id
# # MANUALLY_CHECK_WRONG_VIDEOS="" # 需要填，质检出问题的编号，如果没问题则改为""


# # filename="${DATA_PATH}/low_quality.txt" # 通常不需要改

# # #============================================================以下无需修改

# # # 在原文件夹下创建low_quality.txt文件，存储解析出问题的视频id
# # python video_check/validate_videos.py ${DATA_PATH}


# # # 检查字符串是否非空，且只包含数字和逗号
# # if [[ -n "$MANUALLY_CHECK_WRONG_VIDEOS" ]] && [[ "$MANUALLY_CHECK_WRONG_VIDEOS" =~ ^[0-9,]+$ ]]; then
# #     # 追加到文件，每行一个数字
# #     IFS=',' read -ra num_array <<< "$MANUALLY_CHECK_WRONG_VIDEOS"
# #     for num in "${num_array[@]}"; do
# #         echo "$num" >> "$filename"
# #     done
# #     echo "成功追加数字到 $filename"
# # elif [[ -z "$MANUALLY_CHECK_WRONG_VIDEOS" ]]; then
# #     echo "输入为空，跳过写入。"
# # else
# #     echo "错误：输入包含非数字字符。"
# #     exit 1
# # fi

# # python filter_remove/clean_and_copy_lerobot.py \
# #   --src_root ${DATA_PATH} \
# #   --dst_root ${TARGET_PATH} \
# #   --remove_txt ${DATA_PATH}/low_quality.txt \
# #   --cams front,wrist \
# #   --modality_file_path ${MODALITY_PATH}