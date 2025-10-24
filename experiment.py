import os
import shutil
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3


def write_large_file(file_path, size_gb):
    """Write a large file of specified size in GB using efficient method"""
    size_bytes = int(size_gb * 1024 * 1024 * 1024)

    # Method 2: Zero-filled (fast and uses actual disk space)
    chunk_size = 10 * 1024 * 1024  # 10MB chunks
    zero_chunk = b'\0' * chunk_size

    with open(file_path, 'wb') as f:
        written = 0
        while written < size_bytes:
            to_write = int(min(chunk_size, size_bytes - written))
            if to_write == chunk_size:
                f.write(zero_chunk)
            else:
                f.write(b'\0' * to_write)
            written += to_write


def write_small_file(file_path, size_kb):
    """Write a small file of specified size in KB"""
    size_bytes = int(size_kb * 1024)
    with open(file_path, 'wb') as f:
        f.write(os.urandom(size_bytes))


def copy_file(src_path, dst_path, chunk_size_kb=1024):
    """Copy a file from source to destination using streaming

    Args:
        src_path: Source file path
        dst_path: Destination file path
        chunk_size_kb: Chunk size in KB (default: 1024)
    """
    chunk_size = chunk_size_kb * 1024  # Convert KB to bytes
    with open(src_path, 'rb') as src:
        with open(dst_path, 'wb') as dst:
            while True:
                chunk = src.read(chunk_size)
                if not chunk:
                    break
                dst.write(chunk)


def read_file(file_path, chunk_size_kb=10240):
    """Read a file using streaming

    Args:
        file_path: File path to read
        chunk_size_kb: Chunk size in KB (default: 10240)
    """
    chunk_size = chunk_size_kb * 1024  # Convert KB to bytes
    with open(file_path, 'rb') as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break


def upload_file_to_s3(s3_client, bucket_name, object_key, local_file_path):
    """Upload a local file to S3 by streaming (memory efficient)"""
    # Open file and upload directly - boto3 will stream it
    with open(local_file_path, 'rb') as f:
        s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=f)


def download_file_from_s3(s3_client, bucket_name, object_key, chunk_size_kb=10240):
    """Download a file from S3 by streaming (memory efficient)

    Args:
        s3_client: boto3 S3 client
        bucket_name: S3 bucket name
        object_key: S3 object key
        chunk_size_kb: Chunk size in KB for streaming read (default: 10240)
    """
    chunk_size = chunk_size_kb * 1024  # Convert KB to bytes
    response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    # Stream the body and discard (just measure read speed)
    for chunk in response['Body'].iter_chunks(chunk_size=chunk_size):
        pass  # Just read and discard


def test_files_s3(bucket_name='test-bucket', num_files=10, size_kb=1048576, parallel_writes=None, temp_dir='/tmp/s3_test'):
    """Unified function to test writing files to MinIO/S3 in parallel

    Args:
        bucket_name: S3 bucket name
        num_files: Number of files to upload
        size_kb: Size of each file in KB (use 1048576 for 1GB, etc.)
        parallel_writes: Max parallel uploads (defaults to num_files if None)
        temp_dir: Temporary directory for pre-created files

    Returns:
        dict: {'write_speed_mbs': float, 'read_speed_mbs': float}
    """
    if parallel_writes is None:
        parallel_writes = num_files

    size_mb = size_kb / 1024
    size_gb = size_kb / (1024 * 1024)
    file_prefix = 'large_file' if size_kb >= 100 * 1024 else 'small_file'

    print(f"\n=== Testing S3 Upload: {num_files} files x {size_mb:.2f}MB ({parallel_writes} parallel) ===")

    # Step 1: Pre-create files on disk (NOT timed)
    print(f"Pre-creating {num_files} files on local disk...")
    os.makedirs(temp_dir, exist_ok=True)

    file_paths = []
    for i in range(num_files):
        file_path = os.path.join(temp_dir, f'{file_prefix}_{i}.dat')
        file_paths.append(file_path)

        # Create file
        if size_kb >= 100 * 1024:  # >= 100MB
            write_large_file(file_path, size_gb)
            print(f"  Creating file {i+1}/{num_files}: {file_path}")
        else:
            write_small_file(file_path, size_kb)
            # Progress indicator for many small files
            if (i + 1) % 1000 == 0:
                print(f"  Created {i+1}/{num_files} files...")

    print("Files created. Starting upload test...")

    # Step 2: Setup S3 client
    access_key = os.environ.get('AWS_ACCESS_KEY_ID', 'minioadmin')
    secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY', 'minioadmin')
    endpoint_url = 'http://minio.service.consul:9000'

    s3_client = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    # Create bucket if it doesn't exist
    try:
        s3_client.create_bucket(Bucket=bucket_name)
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        pass
    except Exception as e:
        print(f"Note: {e}")

    # Step 3: Upload files to S3 (TIMED)
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=parallel_writes) as executor:
        futures = []
        for i in range(num_files):
            object_key = f'{file_prefix}_{i}.dat'
            local_file_path = file_paths[i]
            futures.append(executor.submit(upload_file_to_s3, s3_client, bucket_name, object_key, local_file_path))

        for future in as_completed(futures):
            future.result()

    elapsed = time.time() - start_time
    total_mb = num_files * size_mb
    total_gb = total_mb / 1024
    speed_mbs = total_mb / elapsed

    print(f"Upload time taken: {elapsed:.2f} seconds")
    print(f"Total data written: {total_gb:.2f} GB ({total_mb} MB)")
    print(f"Write speed: {speed_mbs / 1024:.2f} GB/s ({speed_mbs:.2f} MB/s)")
    print(f"Files per second: {num_files / elapsed:.2f}")

    # Step 4: Download files from S3 (TIMED)
    print("\nStarting download test...")
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=parallel_writes) as executor:
        futures = []
        for i in range(num_files):
            object_key = f'{file_prefix}_{i}.dat'
            futures.append(executor.submit(download_file_from_s3, s3_client, bucket_name, object_key, 10 * 1024))  # 10MB in KB

        for future in as_completed(futures):
            future.result()

    elapsed = time.time() - start_time
    read_speed_mbs = total_mb / elapsed

    print(f"Download time taken: {elapsed:.2f} seconds")
    print(f"Total data read: {total_gb:.2f} GB ({total_mb} MB)")
    print(f"Read speed: {read_speed_mbs / 1024:.2f} GB/s ({read_speed_mbs:.2f} MB/s)")
    print(f"Files per second: {num_files / elapsed:.2f}")

    # Step 5: Cleanup S3
    print("\nCleaning up S3...")
    for i in range(num_files):
        try:
            s3_client.delete_object(Bucket=bucket_name, Key=f'{file_prefix}_{i}.dat')
        except:
            pass

    try:
        s3_client.delete_bucket(Bucket=bucket_name)
    except:
        pass

    # Step 6: Cleanup local files
    print("Cleaning up local files...")
    for file_path in file_paths:
        try:
            os.remove(file_path)
        except:
            pass

    try:
        os.rmdir(temp_dir)
    except:
        pass

    return {
        'write_speed_mbs': speed_mbs,
        'read_speed_mbs': read_speed_mbs
    }


def test_large_files(output_dir='test_large', num_files=10, size_gb=1, temp_dir='/tmp/local_test'):
    """Test writing large files in parallel with separate directory per thread

    Returns:
        dict: {'write_speed_mbs': float, 'read_speed_mbs': float}
    """
    print(f"\n=== Testing Large Files: {num_files} files x {size_gb}GB ===")

    # Step 1: Pre-create files on local disk (NOT timed)
    print(f"Pre-creating {num_files} files on local disk...")
    os.makedirs(temp_dir, exist_ok=True)

    source_files = []
    for i in range(num_files):
        file_path = os.path.join(temp_dir, f'large_file_{i}.dat')
        source_files.append(file_path)
        print(f"  Creating file {i+1}/{num_files}: {file_path}")
        write_large_file(file_path, size_gb)

    print("Files created. Starting copy test...")

    # Step 2: Create output directory and subdirectories for each thread
    os.makedirs(output_dir, exist_ok=True)
    target_dirs = []
    for i in range(num_files):
        thread_dir = os.path.join(output_dir, f'thread_{i}')
        os.makedirs(thread_dir, exist_ok=True)
        target_dirs.append(thread_dir)

    # Step 3: Copy files to separate directories in parallel (TIMED)
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=num_files) as executor:
        futures = []
        for i in range(num_files):
            src_path = source_files[i]
            dst_path = os.path.join(target_dirs[i], f'large_file_{i}.dat')
            futures.append(executor.submit(copy_file, src_path, dst_path, 10 * 1024))  # 10MB in KB

        for future in as_completed(futures):
            future.result()

    elapsed = time.time() - start_time
    total_gb = num_files * size_gb
    speed = total_gb / elapsed

    print(f"Write time taken: {elapsed:.2f} seconds")
    print(f"Total data written: {total_gb} GB")
    print(f"Write speed: {speed:.2f} GB/s ({speed * 1024:.2f} MB/s)")

    # Step 4: Read files in parallel (TIMED)
    print("\nStarting read test...")
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=num_files) as executor:
        futures = []
        for i in range(num_files):
            file_path = os.path.join(target_dirs[i], f'large_file_{i}.dat')
            futures.append(executor.submit(read_file, file_path, 10 * 1024))  # 10MB in KB

        for future in as_completed(futures):
            future.result()

    elapsed = time.time() - start_time
    read_speed = total_gb / elapsed

    print(f"Read time taken: {elapsed:.2f} seconds")
    print(f"Total data read: {total_gb} GB")
    print(f"Read speed: {read_speed:.2f} GB/s ({read_speed * 1024:.2f} MB/s)")

    # Step 5: Cleanup target files and directories
    print("\nCleaning up target files...")
    for i in range(num_files):
        try:
            os.remove(os.path.join(target_dirs[i], f'large_file_{i}.dat'))
        except:
            pass
        try:
            os.rmdir(target_dirs[i])
        except:
            pass

    try:
        os.rmdir(output_dir)
    except:
        pass

    # Step 6: Cleanup source files
    print("Cleaning up source files...")
    for file_path in source_files:
        try:
            os.remove(file_path)
        except:
            pass

    try:
        os.rmdir(temp_dir)
    except:
        pass

    return {
        'write_speed_mbs': speed * 1024,
        'read_speed_mbs': read_speed * 1024
    }


def copy_dir_to_hdfs_with_threads(local_dir, hdfs_dir, num_threads):
    """Copy all files from a local directory to HDFS using hdfs dfs -copyFromLocal command with -t flag

    Args:
        local_dir: Local directory path containing files to copy
        hdfs_dir: HDFS destination directory
        num_threads: Number of threads for HDFS to use (-t flag)
    """
    # Build command: hdfs dfs -copyFromLocal -t <threads> local_dir hdfs_dir/
    cmd = ['hdfs', 'dfs', '-copyFromLocal', '-t', str(num_threads), local_dir, hdfs_dir]
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        raise Exception(f"Failed to copy directory {local_dir} to {hdfs_dir}: {result.stderr}")


def copy_dir_from_hdfs_with_threads(hdfs_dir, local_dir, num_threads):
    """Copy all files from HDFS to a local directory using hdfs dfs -copyToLocal command with -t flag

    Args:
        hdfs_dir: HDFS source directory
        local_dir: Local destination directory path
        num_threads: Number of threads for HDFS to use (-t flag)
    """
    # Build command: hdfs dfs -copyToLocal -t <threads> hdfs_dir local_dir/
    cmd = ['hdfs', 'dfs', '-copyToLocal', '-t', str(num_threads), hdfs_dir, local_dir]
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        raise Exception(f"Failed to copy directory {hdfs_dir} to {local_dir}: {result.stderr}")


def test_files_local_copy(output_dir='test_hdfs', num_files=10, size_kb=1048576, parallel_writes=None, temp_dir='/tmp/hdfs_test'):
    """Test writing files to HDFS using hdfs dfs -copyFromLocal in parallel

    Args:
        output_dir: HDFS output directory
        num_files: Number of files to upload
        size_kb: Size of each file in KB (use 1048576 for 1GB, etc.)
        parallel_writes: Max parallel uploads (defaults to num_files if None)
        temp_dir: Temporary directory for pre-created files

    Returns:
        dict: {'write_speed_mbs': float, 'read_speed_mbs': float}
    """
    if parallel_writes is None:
        parallel_writes = num_files

    size_mb = size_kb / 1024
    size_gb = size_kb / (1024 * 1024)
    file_prefix = 'large_file' if size_kb >= 100 * 1024 else 'small_file'

    print(f"\n=== Testing HDFS copyFromLocal: {num_files} files x {size_mb:.2f}MB ({parallel_writes} parallel) ===")

    # Step 1: Pre-create files on disk organized in subdirectories (NOT timed)
    # Every 100 files go into a separate subdirectory
    print(f"Pre-creating {num_files} files on local disk (100 files per subdirectory)...")
    os.makedirs(temp_dir, exist_ok=True)

    files_per_subdir = 100
    subdirs = []
    file_paths = []

    for i in range(num_files):
        # Determine subdirectory for this file
        subdir_index = i // files_per_subdir
        subdir_path = os.path.join(temp_dir, f'subdir_{subdir_index}')

        # Create subdirectory if it doesn't exist
        if subdir_path not in subdirs:
            os.makedirs(subdir_path, exist_ok=True)
            subdirs.append(subdir_path)
            print(f"  Created subdirectory: subdir_{subdir_index}")

        # Create file in the subdirectory
        file_path = os.path.join(subdir_path, f'{file_prefix}_{i}.dat')
        file_paths.append(file_path)

        # Create file
        if size_kb >= 100 * 1024:  # >= 100MB
            write_large_file(file_path, size_gb)
            print(f"  Creating file {i+1}/{num_files}: {file_path}")
        else:
            write_small_file(file_path, size_kb)
            # Progress indicator for many small files
            if (i + 1) % 1000 == 0:
                print(f"  Created {i+1}/{num_files} files...")

    print("Files created. Starting HDFS copy test...")

    # Step 2: Create HDFS output directory
    print(f"Creating HDFS directory: {output_dir}")
    result = subprocess.run(['hdfs', 'dfs', '-mkdir', '-p', output_dir], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error creating {output_dir}: {result.stderr}")
        raise Exception(f"Failed to create HDFS directory {output_dir}")

    # Step 3: Copy all files to HDFS using single command with -t flag (TIMED)
    print(f"Starting HDFS copy with -t {parallel_writes} (HDFS handles threading)...")
    print(f"Copying {num_files} files from {temp_dir} to {output_dir}")

    start_time = time.time()

    copy_dir_to_hdfs_with_threads(temp_dir, output_dir, parallel_writes)

    elapsed = time.time() - start_time
    total_mb = num_files * size_mb
    total_gb = total_mb / 1024
    speed_mbs = total_mb / elapsed

    print(f"Upload time taken: {elapsed:.2f} seconds")
    print(f"Total data written: {total_gb:.2f} GB ({total_mb} MB)")
    print(f"Write speed: {speed_mbs / 1024:.2f} GB/s ({speed_mbs:.2f} MB/s)")
    print(f"Files per second: {num_files / elapsed:.2f}")

    # Step 4: Copy files from HDFS to local using copyToLocal with -t flag (TIMED)
    print(f"\nStarting HDFS copyToLocal with -t {parallel_writes} (HDFS handles threading)...")
    download_dir = temp_dir + '_download'
    os.makedirs(download_dir, exist_ok=True)
    print(f"Downloading {num_files} files from {output_dir} to {download_dir}")

    start_time = time.time()

    copy_dir_from_hdfs_with_threads(output_dir, download_dir, parallel_writes)

    elapsed = time.time() - start_time
    read_speed_mbs = total_mb / elapsed

    print(f"Download time taken: {elapsed:.2f} seconds")
    print(f"Total data read: {total_gb:.2f} GB ({total_mb} MB)")
    print(f"Read speed: {read_speed_mbs / 1024:.2f} GB/s ({read_speed_mbs:.2f} MB/s)")
    print(f"Files per second: {num_files / elapsed:.2f}")

    # Step 5: Cleanup HDFS
    print("\nCleaning up HDFS...")
    subprocess.run(['hdfs', 'dfs', '-rm', '-r', '-f', output_dir], capture_output=True)

    # Step 6: Cleanup downloaded files
    print("Cleaning up downloaded files...")
    try:
        shutil.rmtree(download_dir)
    except:
        pass

    # Step 7: Cleanup local upload files and directories
    print("Cleaning up local upload files...")
    for file_path in file_paths:
        try:
            os.remove(file_path)
        except:
            pass

    # Remove subdirectories
    for subdir in subdirs:
        try:
            os.rmdir(subdir)
        except:
            pass

    # Remove main temp directory
    try:
        os.rmdir(temp_dir)
    except:
        pass

    return {
        'write_speed_mbs': speed_mbs,
        'read_speed_mbs': read_speed_mbs
    }


def test_small_files(output_dir='test_small', total_files=5000, parallel_writes=50, size_kb=1024, temp_dir='/tmp/small_test'):
    """Test writing many small files with limited parallelism and separate directory per thread

    Returns:
        dict: {'write_speed_mbs': float, 'read_speed_mbs': float}
    """
    size_mb = size_kb / 1024

    print(f"\n=== Testing Small Files: {total_files} files x {size_mb:.2f}MB ({parallel_writes} parallel) ===")

    # Step 1: Pre-create files on local disk (NOT timed)
    print(f"Pre-creating {total_files} files on local disk...")
    os.makedirs(temp_dir, exist_ok=True)

    source_files = []
    for i in range(total_files):
        file_path = os.path.join(temp_dir, f'small_file_{i}.dat')
        source_files.append(file_path)
        write_small_file(file_path, size_kb)

        # Progress indicator for large number of files
        if (i + 1) % 1000 == 0:
            print(f"  Created {i+1}/{total_files} files...")

    print("Files created. Starting copy test...")

    # Step 2: Create output directory and subdirectories for each thread
    os.makedirs(output_dir, exist_ok=True)
    target_dirs = []
    for i in range(parallel_writes):
        thread_dir = os.path.join(output_dir, f'thread_{i}')
        os.makedirs(thread_dir, exist_ok=True)
        target_dirs.append(thread_dir)

    # Step 3: Copy files to separate directories in parallel (TIMED)
    # Distribute files across thread directories
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=parallel_writes) as executor:
        futures = []
        for i in range(total_files):
            src_path = source_files[i]
            # Round-robin distribution across thread directories
            thread_id = i % parallel_writes
            dst_path = os.path.join(target_dirs[thread_id], f'small_file_{i}.dat')
            futures.append(executor.submit(copy_file, src_path, dst_path, size_kb))

        for future in as_completed(futures):
            future.result()

    elapsed = time.time() - start_time
    total_mb = total_files * size_mb
    total_gb = total_mb / 1024
    speed_mbs = total_mb / elapsed

    print(f"Write time taken: {elapsed:.2f} seconds")
    print(f"Total data written: {total_mb} MB ({total_gb:.2f} GB)")
    print(f"Write speed: {speed_mbs / 1024:.2f} GB/s ({speed_mbs:.2f} MB/s)")
    print(f"Files per second: {total_files / elapsed:.2f}")

    # Step 4: Read files in parallel (TIMED)
    print("\nStarting read test...")
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=parallel_writes) as executor:
        futures = []
        for i in range(total_files):
            thread_id = i % parallel_writes
            file_path = os.path.join(target_dirs[thread_id], f'small_file_{i}.dat')
            futures.append(executor.submit(read_file, file_path, size_kb))

        for future in as_completed(futures):
            future.result()

    elapsed = time.time() - start_time
    read_speed_mbs = total_mb / elapsed

    print(f"Read time taken: {elapsed:.2f} seconds")
    print(f"Total data read: {total_mb} MB ({total_gb:.2f} GB)")
    print(f"Read speed: {read_speed_mbs / 1024:.2f} GB/s ({read_speed_mbs:.2f} MB/s)")
    print(f"Files per second: {total_files / elapsed:.2f}")

    # Step 5: Cleanup target files and directories
    print("\nCleaning up target files...")
    for i in range(total_files):
        try:
            thread_id = i % parallel_writes
            os.remove(os.path.join(target_dirs[thread_id], f'small_file_{i}.dat'))
        except:
            pass

    for thread_dir in target_dirs:
        try:
            os.rmdir(thread_dir)
        except:
            pass

    try:
        os.rmdir(output_dir)
    except:
        pass

    # Step 6: Cleanup source files
    print("Cleaning up source files...")
    for file_path in source_files:
        try:
            os.remove(file_path)
        except:
            pass

    try:
        os.rmdir(temp_dir)
    except:
        pass

    return {
        'write_speed_mbs': speed_mbs,
        'read_speed_mbs': read_speed_mbs
    }


if __name__ == '__main__':
    print("Disk Write Speed Benchmark")
    print("=" * 50)

    results = {}

    # Test 1: Large files (10 x 5GB in parallel)
    results['Test 1: Large files (hopsfs-mount)'] = test_large_files(output_dir="/hopsfs/Jupyter/test", num_files=5, size_gb=1)

    # Test 2: Small files (5000 x 1MB, 50 parallel writes)
    results['Test 2: Small files (hopsfs-mount)'] = test_small_files(output_dir="/hopsfs/Jupyter/test", total_files=1000, parallel_writes=32, size_kb=100)

    # Test 3: Large files direct minio (5 files x 1GB each)
    results['Test 3: Large files (S3)'] = test_files_s3(bucket_name="test-large", num_files=5, size_kb=1024*1024)

    # Test 4: Small files direct minio (5000 files x 1MB each, 50 parallel)
    results['Test 4: Small files (S3)'] = test_files_s3(bucket_name="test-small", num_files=1000, size_kb=100, parallel_writes=32)

    # Test 5: Large files HDFS copyFromLocal (5 files x 1GB each)
    results['Test 5: Large files (HDFS)'] = test_files_local_copy(output_dir="/Projects/test/test_hdfs_large/tests", num_files=5, size_kb=1024*1024)

    # Test 6: Small files HDFS copyFromLocal (5000 files x 1MB each, 50 parallel)
    results['Test 6: Small files (HDFS)'] = test_files_local_copy(output_dir="/Projects/test/test_hdfs_small/tests", num_files=1000, size_kb=100, parallel_writes=32)

    # Print summary table
    print("\n" + "=" * 80)
    print("BENCHMARK RESULTS SUMMARY")
    print("=" * 80)
    print(f"{'Test Name':<35} {'Write Speed (MB/s)':<20} {'Read Speed (MB/s)':<20}")
    print("-" * 80)

    for test_name, speeds in results.items():
        write_speed = speeds['write_speed_mbs']
        read_speed = speeds['read_speed_mbs']
        print(f"{test_name:<35} {write_speed:>18.2f}  {read_speed:>18.2f}")

    print("=" * 80)

print("\n" + "=" * 50)
print("Benchmark complete!")