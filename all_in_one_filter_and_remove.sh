python all_in_one_filter_and_remove.py \
  --src_base_path /pfs/pfs-ahGxdf/data/collect_data/so101 \
  --dst_base_path /pfs/pfs-ahGxdf/data/xiezhengyuan/backup/generalizable_pick_place_processed \
  --search_dirs "blk0,blk3" \
  --modality_path /pfs/pfs-ahGxdf/data/xiezhengyuan/backup/modality_files/so101_front_wrist_state-dim6_action-dim6/modality.json \
  --cams "front,wrist" \
  --validator_script "video_check/validate_videos.py"
#   --manual_remove '{"blk0/20250825_blk0": "15,22", "blk3/some_other_dataset": "4"}'