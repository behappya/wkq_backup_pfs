DATA_PATH=/pfs/pfs-ahGxdf/data/xiezhengyuan/backup/pens=241+212_tapes=233+218/ # 需要填，原始路径
TARGET_PATH=/pfs/pfs-ahGxdf/data/xiezhengyuan/backup/debug/ # 需要填，待生成的目标路径
MODALITY_PATH=/pfs/pfs-ahGxdf/data/xiezhengyuan/backup/modality_files/so101_front_wrist_state-dim6_action-dim6/modality.json # 通常不需要改
# 手工检查出错的视频id
MANUALLY_CHECK_WRONG_VIDEOS="143,234" # 需要填，质检出问题的编号，如果没问题则改为""


filename="${DATA_PATH}/low_quality.txt" # 通常不需要改

#============================================================以下无需修改

# 在原文件夹下创建low_quality.txt文件，存储解析出问题的视频id
python video_check/validate_videos.py ${DATA_PATH}


# 检查字符串是否非空，且只包含数字和逗号
if [[ -n "$MANUALLY_CHECK_WRONG_VIDEOS" ]] && [[ "$MANUALLY_CHECK_WRONG_VIDEOS" =~ ^[0-9,]+$ ]]; then
    # 追加到文件，每行一个数字
    IFS=',' read -ra num_array <<< "$MANUALLY_CHECK_WRONG_VIDEOS"
    for num in "${num_array[@]}"; do
        echo "$num" >> "$filename"
    done
    echo "成功追加数字到 $filename"
elif [[ -z "$MANUALLY_CHECK_WRONG_VIDEOS" ]]; then
    echo "输入为空，跳过写入。"
else
    echo "错误：输入包含非数字字符。"
    exit 1
fi

python filter_remove/clean_and_copy_lerobot.py \
  --src_root ${DATA_PATH} \
  --dst_root ${TARGET_PATH} \
  --remove_txt ${DATA_PATH}/low_quality.txt \
  --cams front,wrist \
  --modality_file_path ${MODALITY_PATH}