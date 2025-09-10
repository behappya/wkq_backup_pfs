python all_in_one_filter_and_remove.py \
  --src_base_path /pfs/pfs-ahGxdf/data/xiezhengyuan/backup/ori_data \
  --dst_base_path /pfs/pfs-ahGxdf/data/xiezhengyuan/backup/ \
  --search_dirs "0904-three-pen-right-left,0904-two-pen-right-left" \
  --modality_path /pfs/pfs-ahGxdf/data/xiezhengyuan/backup/modality_files/so101_front_wrist_state-dim6_action-dim6/modality.json \
  --cams "front,wrist" \
  --validator_script "video_check/validate_videos.py"
#   --manual_remove '{"blk0/20250825_blk0": "15,22", "blk3/some_other_dataset": "4"}'