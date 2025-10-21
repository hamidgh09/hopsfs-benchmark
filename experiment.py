import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3


def write_large_file(file_path, size_gb):
    """Write a large file of specified size in GB using efficient method"""
    size_bytes = size_gb * 1024 * 1024 * 1024

    # Method 2: Zero-filled (fast and uses actual disk space)
    chunk_size = 10 * 1024 * 1024  # 10MB chunks
    zero_chunk = b'\0' * chunk_size

    with open(file_path, 'wb') as f:
        written = 0
        while written < size_bytes:
            to_write = min(chunk_size, size_bytes - written)
            if to_write == chunk_size:
                f.write(zero_chunk)
            else:
                f.write(b'\0' * to_write)
            written += to_write


def write_small_file(file_path, size_mb):
    """Write a small file of specified size in MB"""
    size_bytes = size_mb * 1024 * 1024
    with open(file_path, 'wb') as f:
        f.write(os.urandom(size_bytes))


def copy_file(src_path, dst_path, chunk_size_mb=1):
    """Copy a file from source to destination using streaming

    Args:
        src_path: Source file path
        dst_path: Destination file path
        chunk_size_mb: Chunk size in MB (default: 1)
    """
    chunk_size = chunk_size_mb * 1024 * 1024  # Convert MB to bytes
    with open(src_path, 'rb') as src:
        with open(dst_path, 'wb') as dst:
            while True:
                chunk = src.read(chunk_size)
                if not chunk:
                    break
                dst.write(chunk)


def upload_file_to_s3(s3_client, bucket_name, object_key, local_file_path):
    """Upload a local file to S3 by streaming (memory efficient)"""
    # Open file and upload directly - boto3 will stream it
    with open(local_file_path, 'rb') as f:
        s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=f)


def test_small_files_s3(bucket_name='test-bucket', total_files=5000, parallel_writes=50, size_mb=1, temp_dir='/tmp/s3_small_test'):
    """Test writing many small files to MinIO/S3 with limited parallelism"""
    print(f"\n=== Testing Small Files (S3): {total_files} files x {size_mb}MB ({parallel_writes} parallel) ===")

    # Step 1: Pre-create files on disk (NOT timed)
    print(f"Pre-creating {total_files} files on local disk...")
    os.makedirs(temp_dir, exist_ok=True)

    source_files = []
    for i in range(total_files):
        file_path = os.path.join(temp_dir, f'small_file_{i}.dat')
        source_files.append(file_path)
        write_small_file(file_path, size_mb)

        # Progress indicator for large number of files
        if (i + 1) % 1000 == 0:
            print(f"  Created {i+1}/{total_files} files...")

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
        for i in range(total_files):
            object_key = f'small_file_{i}.dat'
            local_file_path = source_files[i]
            futures.append(executor.submit(upload_file_to_s3, s3_client, bucket_name, object_key, local_file_path))

        for future in as_completed(futures):
            future.result()

    elapsed = time.time() - start_time
    total_mb = total_files * size_mb
    total_gb = total_mb / 1024
    speed_mbs = total_mb / elapsed

    print(f"Time taken: {elapsed:.2f} seconds")
    print(f"Total data written: {total_mb} MB ({total_gb:.2f} GB)")
    print(f"Write speed: {speed_mbs / 1024:.2f} GB/s ({speed_mbs:.2f} MB/s)")
    print(f"Files per second: {total_files / elapsed:.2f}")

    # Step 4: Cleanup S3
    print("Cleaning up S3...")
    for i in range(total_files):
        try:
            s3_client.delete_object(Bucket=bucket_name, Key=f'small_file_{i}.dat')
        except:
            pass

    try:
        s3_client.delete_bucket(Bucket=bucket_name)
    except:
        pass

    # Step 5: Cleanup local files
    print("Cleaning up local files...")
    for file_path in source_files:
        try:
            os.remove(file_path)
        except:
            pass

    try:
        os.rmdir(temp_dir)
    except:
        pass


def test_large_files_s3(bucket_name='test-bucket', num_files=10, size_gb=1, temp_dir='/tmp/s3_test'):
    """Test writing large files to MinIO/S3 in parallel"""
    print(f"\n=== Testing Large Files (S3): {num_files} files x {size_gb}GB ===")

    # Step 1: Pre-create files on disk (NOT timed)
    print(f"Pre-creating {num_files} files on local disk...")
    os.makedirs(temp_dir, exist_ok=True)

    file_paths = []
    for i in range(num_files):
        file_path = os.path.join(temp_dir, f'large_file_{i}.dat')
        file_paths.append(file_path)
        print(f"  Creating file {i+1}/{num_files}: {file_path}")
        write_large_file(file_path, size_gb)

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

    with ThreadPoolExecutor(max_workers=num_files) as executor:
        futures = []
        for i in range(num_files):
            object_key = f'large_file_{i}.dat'
            local_file_path = file_paths[i]
            futures.append(executor.submit(upload_file_to_s3, s3_client, bucket_name, object_key, local_file_path))

        for future in as_completed(futures):
            future.result()

    elapsed = time.time() - start_time
    total_gb = num_files * size_gb
    speed = total_gb / elapsed

    print(f"Time taken: {elapsed:.2f} seconds")
    print(f"Total data written: {total_gb} GB")
    print(f"Write speed: {speed:.2f} GB/s ({speed * 1024:.2f} MB/s)")

    # Step 4: Cleanup S3
    print("Cleaning up S3...")
    for i in range(num_files):
        try:
            s3_client.delete_object(Bucket=bucket_name, Key=f'large_file_{i}.dat')
        except:
            pass

    try:
        s3_client.delete_bucket(Bucket=bucket_name)
    except:
        pass

    # Step 5: Cleanup local files
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


def test_large_files(output_dir='test_large', num_files=10, size_gb=1, temp_dir='/tmp/local_test'):
    """Test writing large files in parallel with separate directory per thread"""
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
            futures.append(executor.submit(copy_file, src_path, dst_path, 10))

        for future in as_completed(futures):
            future.result()

    elapsed = time.time() - start_time
    total_gb = num_files * size_gb
    speed = total_gb / elapsed

    print(f"Time taken: {elapsed:.2f} seconds")
    print(f"Total data written: {total_gb} GB")
    print(f"Write speed: {speed:.2f} GB/s ({speed * 1024:.2f} MB/s)")

    # Step 4: Cleanup target files and directories
    print("Cleaning up target files...")
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

    # Step 5: Cleanup source files
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


def test_small_files(output_dir='test_small', total_files=5000, parallel_writes=50, size_mb=1, temp_dir='/tmp/small_test'):
    """Test writing many small files with limited parallelism and separate directory per thread"""
    print(f"\n=== Testing Small Files: {total_files} files x {size_mb}MB ({parallel_writes} parallel) ===")

    # Step 1: Pre-create files on local disk (NOT timed)
    print(f"Pre-creating {total_files} files on local disk...")
    os.makedirs(temp_dir, exist_ok=True)

    source_files = []
    for i in range(total_files):
        file_path = os.path.join(temp_dir, f'small_file_{i}.dat')
        source_files.append(file_path)
        write_small_file(file_path, size_mb)

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
            futures.append(executor.submit(copy_file, src_path, dst_path, size_mb))

        for future in as_completed(futures):
            future.result()

    elapsed = time.time() - start_time
    total_mb = total_files * size_mb
    total_gb = total_mb / 1024
    speed_mbs = total_mb / elapsed

    print(f"Time taken: {elapsed:.2f} seconds")
    print(f"Total data written: {total_mb} MB ({total_gb:.2f} GB)")
    print(f"Write speed: {speed_mbs / 1024:.2f} GB/s ({speed_mbs:.2f} MB/s)")
    print(f"Files per second: {total_files / elapsed:.2f}")

    # Step 4: Cleanup target files and directories
    print("Cleaning up target files...")
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

    # Step 5: Cleanup source files
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


if __name__ == '__main__':
    print("Disk Write Speed Benchmark")
    print("=" * 50)

    # Test 1: Large files (10 x 5GB in parallel)
    test_large_files(output_dir="/hopsfs/Jupyter/test4", num_files=5, size_gb=1)

    # Test 2: Small files (5000 x 1MB, 50 parallel writes)
    test_small_files(output_dir="/hopsfs/Jupyter/test3", total_files=5000, parallel_writes=50, size_mb=1)

    # Test 3: Large files direct minio
    test_large_files_s3("test-large", num_files=5, size_gb=1)

    # Test 4: Small files direct minio
    test_small_files_s3("test-small", total_files=5000, parallel_writes=50, size_mb=1)

print("\n" + "=" * 50)
print("Benchmark complete!")