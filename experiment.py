import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO

import boto3


def write_large_file(file_path, size_gb):
    """Write a large file of specified size in GB"""
    size_bytes = size_gb * 1024 * 1024 * 1024
    chunk_size = 1024 * 1024  # 1MB chunks

    with open(file_path, 'wb') as f:
        written = 0
        while written < size_bytes:
            to_write = min(chunk_size, size_bytes - written)
            f.write(os.urandom(to_write))
            written += to_write


def write_small_file(file_path, size_mb):
    """Write a small file of specified size in MB"""
    size_bytes = size_mb * 1024 * 1024
    with open(file_path, 'wb') as f:
        f.write(os.urandom(size_bytes))


def write_large_file_s3(s3_client, bucket_name, object_key, size_gb):
    """Write a large file of specified size in GB to S3"""
    size_bytes = size_gb * 1024 * 1024 * 1024
    chunk_size = 1024 * 1024  # 1MB chunks

    # Create a BytesIO buffer to hold data
    buffer = BytesIO()
    written = 0

    while written < size_bytes:
        to_write = min(chunk_size, size_bytes - written)
        buffer.write(os.urandom(to_write))
        written += to_write

    # Reset buffer position to the beginning
    buffer.seek(0)

    # Upload to S3
    s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=buffer)
    buffer.close()


def test_large_files_s3(bucket_name='test-bucket', num_files=10, size_gb=1):
    """Test writing large files to MinIO/S3 in parallel"""
    print(f"\n=== Testing Large Files (S3): {num_files} files x {size_gb}GB ===")

    # Get credentials from environment variables
    access_key = os.environ.get('AWS_ACCESS_KEY_ID', 'minioadmin')
    secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY', 'minioadmin')
    endpoint_url = 'http://minio.service.consul:9000'

    # Create S3 client
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

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=num_files) as executor:
        futures = []
        for i in range(num_files):
            object_key = f'large_file_{i}.dat'
            futures.append(executor.submit(write_large_file_s3, s3_client, bucket_name, object_key, size_gb))

        for future in as_completed(futures):
            future.result()

    elapsed = time.time() - start_time
    total_gb = num_files * size_gb
    speed = total_gb / elapsed

    print(f"Time taken: {elapsed:.2f} seconds")
    print(f"Total data written: {total_gb} GB")
    print(f"Write speed: {speed:.2f} GB/s ({speed * 1024:.2f} MB/s)")

    # Cleanup
    for i in range(num_files):
        try:
            s3_client.delete_object(Bucket=bucket_name, Key=f'large_file_{i}.dat')
        except:
            pass

    # Try to delete bucket (will only succeed if empty)
    try:
        s3_client.delete_bucket(Bucket=bucket_name)
    except:
        pass


def test_large_files(output_dir='test_large', num_files=10, size_gb=1):
    """Test writing large files in parallel"""
    print(f"\n=== Testing Large Files: {num_files} files x {size_gb}GB ===")

    os.makedirs(output_dir, exist_ok=True)

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=num_files) as executor:
        futures = []
        for i in range(num_files):
            file_path = os.path.join(output_dir, f'large_file_{i}.dat')
            futures.append(executor.submit(write_large_file, file_path, size_gb))

        for future in as_completed(futures):
            future.result()

    elapsed = time.time() - start_time
    total_gb = num_files * size_gb
    speed = total_gb / elapsed

    print(f"Time taken: {elapsed:.2f} seconds")
    print(f"Total data written: {total_gb} GB")
    print(f"Write speed: {speed:.2f} GB/s ({speed * 1024:.2f} MB/s)")

    # Cleanup
    for i in range(num_files):
        try:
            os.remove(os.path.join(output_dir, f'large_file_{i}.dat'))
        except:
            pass
    os.rmdir(output_dir)


def test_small_files(output_dir='test_small', total_files=5000, parallel_writes=50, size_mb=1):
    """Test writing many small files with limited parallelism"""
    print(f"\n=== Testing Small Files: {total_files} files x {size_mb}MB ({parallel_writes} parallel) ===")

    os.makedirs(output_dir, exist_ok=True)

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=parallel_writes) as executor:
        futures = []
        for i in range(total_files):
            file_path = os.path.join(output_dir, f'small_file_{i}.dat')
            futures.append(executor.submit(write_small_file, file_path, size_mb))

        for future in as_completed(futures):
            future.result()

    elapsed = time.time() - start_time
    total_mb = total_files * size_mb
    total_gb = total_mb / 1024
    speed_mbs = total_mb / elapsed

    print(f"Time taken: {elapsed:.2f} seconds")
    print(f"Total data written: {total_mb} MB ({total_gb:.2f} GB)")
    print(f"Write speed: {speed_mbs:.2f} MB/s ({speed_mbs / 1024:.2f} GB/s)")
    print(f"Files per second: {total_files / elapsed:.2f}")

    # Cleanup
    for i in range(total_files):
        try:
            os.remove(os.path.join(output_dir, f'small_file_{i}.dat'))
        except:
            pass
    os.rmdir(output_dir)


if __name__ == '__main__':
    print("Disk Write Speed Benchmark")
    print("=" * 50)

    # Test 1: Large files (10 x 5GB in parallel)
    test_large_files(output_dir="/hopsfs/Jupyter/test2", num_files=10, size_gb=1)

    # Test 2: Small files (5000 x 1MB, 50 parallel writes)
    test_small_files(output_dir="/hopsfs/Jupyter/test2", total_files=5000, parallel_writes=50, size_mb=1)

print("\n" + "=" * 50)
print("Benchmark complete!")