# python clean_and_copy_lerobot.py \
#   --src_root /pfs/pfs-ahGxdf/data/xiezhengyuan/so101_data/ori_data/0805-pour-blk4 \
#   --dst_root /pfs/pfs-ahGxdf/data/xiezhengyuan/so101_data/block/0805-pour-blk4 \
#   --remove_txt /pfs/pfs-ahGxdf/data/xiezhengyuan/so101_data/ori_data/0805-pour-blk4/low_quality.txt
python clean_and_copy_lerobot.py \
  --src_root /pfs/pfs-ahGxdf/data/xiezhengyuan/backup/ori_data/0818-blk4-stack-triangle \
  --dst_root /pfs/pfs-ahGxdf/data/xiezhengyuan/backup/debug/0818-blk4-stack-triangle \
  --remove_txt /pfs/pfs-ahGxdf/data/xiezhengyuan/backup/ori_data/0818-blk4-stack-triangle/low_quality.txt \
  --cams front,wrist \
  --modality_file_path /pfs/pfs-ahGxdf/data/xiezhengyuan/backup/modality_files/so101_front_wrist_state-dim6_action-dim6/modality.json