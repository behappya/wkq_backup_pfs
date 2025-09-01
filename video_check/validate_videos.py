import os
import argparse
import torchvision
import av
from tqdm import tqdm
import torch

def find_video_files(directory, extensions):
    """Recursively finds all video files in a directory with given extensions."""
    video_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.lower().endswith(tuple(extensions)):
                video_files.append(os.path.join(root, file))
    return video_files

def validate_videos_with_seek(directory, extensions):
    """
    Validates video files by mimicking a seek-and-read pattern, which is more
    robust for finding corruption related to non-sequential access.
    """
    # 1. Set the video backend to be consistent with your training code.
    try:
        torchvision.set_video_backend("pyav")
    except Exception as e:
        print(f"Warning: Could not set torchvision backend to 'pyav'. It might not be available. Error: {e}")

    print(f"Scanning for video files in: {directory}")
    print(f"Looking for extensions: {', '.join(extensions)}")

    video_paths = find_video_files(directory, extensions)
    
    if not video_paths:
        print("No video files found. Exiting.")
        return

    print(f"Found {len(video_paths)} video files. Starting advanced validation (spot-checking with seek)...")

    problematic_files = {}

    for video_path in tqdm(video_paths, desc="Validating videos"):
        reader = None
        try:
            # # Check for zero-sized files first, as they are always invalid.
            # if os.path.getsize(video_path) == 0:
            #     raise ValueError("File size is 0 bytes.")
                
            # # 2. Initialize the VideoReader, just like in your code.
            # # The 'Cannot allocate memory' error for AV1 happens here.
            # reader = torchvision.io.VideoReader(video_path, "video")
            
            # video_metadata = reader.get_metadata()
            # duration = video_metadata.get("video", {}).get("duration", [0.0])[0]

            # if duration <= 0:
            #     # If duration is invalid, just try to read the first frame as a basic check.
            #     _ = next(reader, None)
            #     continue

            # # 3. Define checkpoints for spot-checking using seek.
            # # We check the start, middle, and near the end of the video.
            # # Timestamps are in seconds.
            # checkpoints = [0.0]
            # if duration > 1.0:
            #      checkpoints.append(duration / 2.0)
            # if duration > 2.0:
            #      checkpoints.append(duration * 0.9) # 90% mark

            # for seek_time in checkpoints:
            #     # 4. Perform the seek operation.
            #     reader.seek(seek_time)
                
            #     # 5. Try to read the next frame after seeking.
            #     frame_data = next(reader, None)
                
            #     # Check if frame reading was successful.
            #     if frame_data is None or not isinstance(frame_data.get('data'), torch.Tensor):
            #         raise RuntimeError(f"Failed to read a valid frame after seeking to {seek_time:.2f}s.")
            torchvision.set_video_backend("pyav")
            # set a video stream reader
            reader = torchvision.io.VideoReader(video_path, "video")
            loaded_frames = []
            for frame in reader:
                current_ts = frame["pts"]
                loaded_frames.append(frame["data"])
                
            reader.container.close()
            reader = None

        except (av.error.InvalidDataError, RuntimeError, ValueError, TypeError) as e:
            # This catches corruption errors, seek errors, 0-byte files, etc.
            error_message = f"{type(e).__name__}: {e}"
            # No need to print here, tqdm handles newlines well.
            problematic_files[video_path] = error_message
        except Exception as e:
            # This will catch other errors, including the 'Cannot allocate memory' one.
            error_message = f"An unexpected error occurred: {e}"
            problematic_files[video_path] = error_message
        finally:
            # 6. Explicitly close the container to release resources, matching your code.
            if reader and hasattr(reader, 'container') and reader.container:
                try:
                    reader.container.close()
                except Exception:
                    pass  # Ignore errors on close

    print("\n" + "="*50)
    print("Advanced Validation Complete.")
    print("="*50)
    error_ids = []
    if not problematic_files:
        print(f"\nSuccess! All {len(video_paths)} videos passed the spot-checking validation.")
    else:
        print(f"\nFound {len(problematic_files)} problematic video(s) out of {len(video_paths)} total:")
        
        for i, (path, error) in enumerate(problematic_files.items()):
            print(f"  {i+1}. File: {path}")
            print(f"     Error: {error}\n")
            error_id = path.split("/")[-1].split(".")[0].split("_")[-1]
            assert error_id.isdigit(), f"Error ID should be a number, but got: {error_id}"
            error_ids.append(error_id) 
    return set(error_ids)
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Validate video files by spot-checking with seek operations to mimic training loaders.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    # ... (argparse part is the same as before) ...
    parser.add_argument("data_directory", type=str, help="The root directory containing your video files.")
    parser.add_argument("--extensions", nargs='+', default=['.mp4', '.avi', '.mov', '.mkv', '.webm'], help="List of video file extensions.")
    args = parser.parse_args()
    normalized_extensions = [ext if ext.startswith('.') else f'.{ext}' for ext in args.extensions]
    

    # 检查args.data_directory + "videos"下面有chunk-000
    assert os.path.exists(args.data_directory + "/videos/chunk-000/"), args.data_directory + "videos/chunk-000/"
    
    video_dirs = os.listdir(args.data_directory + "/videos/chunk-000/")

    all_error_ids = set([])
    for video_dir in video_dirs:
        
        error_ids = validate_videos_with_seek(args.data_directory + "/videos/chunk-000/" +video_dir, normalized_extensions)
        all_error_ids  = all_error_ids | error_ids
    print(f"All error ids: {all_error_ids}")
    # 将all_error_ids写入到args.data_directory + "low_quality.txt",每个id一行，去除前导0，如果txt文件已存在则追加
    if not os.path.exists(args.data_directory + "/low_quality.txt"):
        with open(args.data_directory + "/low_quality.txt", "w") as f:
            pass
    with open(args.data_directory + "/low_quality.txt", "a+") as f:
        for error_id in all_error_ids:
            f.write(str(int(error_id)) + "\n")

       
    