#!/usr/bin/env python3
"""
Benchmark Runner for HopsFS Performance Evaluation

"""

import argparse
import csv
import sys
from experiment import test_large_files, test_small_files, test_files_s3, test_files_local_copy


def run_test_multiple_times(test_func, num_runs, **kwargs):
    """Run a test function multiple times and return all individual results"""
    all_write_fps = []
    all_read_fps = []
    all_write_mbs = []
    all_read_mbs = []

    for run in range(num_runs):
        if num_runs > 1:
            print(f"  [Run {run + 1}/{num_runs}]")
        result = test_func(**kwargs)
        all_write_fps.append(result['write_files_per_sec'])
        all_read_fps.append(result['read_files_per_sec'])
        all_write_mbs.append(result['write_speed_mbs'])
        all_read_mbs.append(result['read_speed_mbs'])

    # Calculate averages
    avg_write_fps = sum(all_write_fps) / len(all_write_fps)
    avg_read_fps = sum(all_read_fps) / len(all_read_fps)
    avg_write_mbs = sum(all_write_mbs) / len(all_write_mbs)
    avg_read_mbs = sum(all_read_mbs) / len(all_read_mbs)

    if num_runs > 1:
        print(f"  Average write speed: {avg_write_fps:.2f} files/sec, {avg_write_mbs:.2f} MB/s")
        print(f"  Average read speed: {avg_read_fps:.2f} files/sec, {avg_read_mbs:.2f} MB/s")

    return {
        'write_files_per_sec': avg_write_fps,
        'read_files_per_sec': avg_read_fps,
        'write_speed_mbs': avg_write_mbs,
        'read_speed_mbs': avg_read_mbs,
        'all_write_fps': all_write_fps,
        'all_read_fps': all_read_fps,
        'all_write_mbs': all_write_mbs,
        'all_read_mbs': all_read_mbs
    }


def run_hopsfs_mount_tests(config):
    """Run tests using HopsFS mount point"""
    print("\n" + "=" * 80)
    print("RUNNING HOPSFS-MOUNT TESTS")
    print("=" * 80)

    results = {}
    num_runs = config.get('runs', 1)

    # Test 1: Large files
    print("\n--- Test 1: Large files (hopsfs-mount) ---")
    results['Large files (hopsfs-mount)'] = run_test_multiple_times(
        test_large_files,
        num_runs,
        output_dir=config.get('output_dir', '/hopsfs/Jupyter/test'),
        num_files=config.get('num_files_large', 5),
        size_gb=config.get('size_gb', 1)
    )

    # Test 2: Small files
    print("\n--- Test 2: Small files (hopsfs-mount) ---")
    results['Small files (hopsfs-mount)'] = run_test_multiple_times(
        test_small_files,
        num_runs,
        output_dir=config.get('output_dir', '/hopsfs/Jupyter/test'),
        total_files=config.get('num_files_small', 1000),
        parallel_writes=config.get('parallel_writes', 32),
        size_kb=config.get('size_kb', 100)
    )

    return results


def run_minio_tests(config):
    """Run tests using direct MinIO/S3 connection"""
    print("\n" + "=" * 80)
    print("RUNNING MINIO/S3 TESTS")
    print("=" * 80)

    results = {}
    num_runs = config.get('runs', 1)

    # Test 3: Large files
    print("\n--- Test 3: Large files (S3) ---")
    results['Large files (S3)'] = run_test_multiple_times(
        test_files_s3,
        num_runs,
        bucket_name=config.get('bucket_large', 'test-large'),
        num_files=config.get('num_files_large', 5),
        size_kb=int(config.get('size_gb', 1) * 1024 * 1024)
    )

    # Test 4: Small files
    print("\n--- Test 4: Small files (S3) ---")
    results['Small files (S3)'] = run_test_multiple_times(
        test_files_s3,
        num_runs,
        bucket_name=config.get('bucket_small', 'test-small'),
        num_files=config.get('num_files_small', 1000),
        size_kb=config.get('size_kb', 100),
        parallel_writes=config.get('parallel_writes', 32)
    )

    return results


def run_java_client_tests(config):
    """Run tests using HDFS Java client (hdfs dfs commands)"""
    print("\n" + "=" * 80)
    print("RUNNING JAVA-CLIENT (HDFS) TESTS")
    print("=" * 80)

    results = {}
    num_runs = config.get('runs', 1)

    # Test 5: Large files
    print("\n--- Test 5: Large files (HDFS) ---")
    results['Large files (HDFS)'] = run_test_multiple_times(
        test_files_local_copy,
        num_runs,
        output_dir=f"{config.get('hdfs_output_dir', '/Projects/test')}/test_hdfs_large/tests",
        num_files=config.get('num_files_large', 5),
        size_kb=int(config.get('size_gb', 1) * 1024 * 1024)
    )

    # Test 6: Small files
    print("\n--- Test 6: Small files (HDFS) ---")
    results['Small files (HDFS)'] = run_test_multiple_times(
        test_files_local_copy,
        num_runs,
        output_dir=f"{config.get('hdfs_output_dir', '/Projects/test')}/test_hdfs_small/tests",
        num_files=config.get('num_files_small', 1000),
        size_kb=config.get('size_kb', 100),
        parallel_writes=config.get('parallel_writes', 32)
    )

    return results


def write_results_to_csv(results):
    """Write results to separate CSV files for write and read speeds"""
    if not results:
        return

    # Determine number of runs from the first result
    first_result = next(iter(results.values()))
    num_runs = len(first_result['all_write_fps'])

    # Write speeds CSV (files/sec)
    with open('write_speeds_fps.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        header = ['Experiment'] + [f'Run {i+1} (files/sec)' for i in range(num_runs)]
        writer.writerow(header)
        for test_name, speeds in results.items():
            row = [test_name] + [f"{speed:.2f}" for speed in speeds['all_write_fps']]
            writer.writerow(row)
    print("\nWrite speeds (files/sec) saved to: write_speeds_fps.csv")

    # Write speeds CSV (MB/s)
    with open('write_speeds_mbs.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        header = ['Experiment'] + [f'Run {i+1} (MB/s)' for i in range(num_runs)]
        writer.writerow(header)
        for test_name, speeds in results.items():
            row = [test_name] + [f"{speed:.2f}" for speed in speeds['all_write_mbs']]
            writer.writerow(row)
    print("Write speeds (MB/s) saved to: write_speeds_mbs.csv")

    # Read speeds CSV (files/sec)
    with open('read_speeds_fps.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        header = ['Experiment'] + [f'Run {i+1} (files/sec)' for i in range(num_runs)]
        writer.writerow(header)
        for test_name, speeds in results.items():
            row = [test_name] + [f"{speed:.2f}" for speed in speeds['all_read_fps']]
            writer.writerow(row)
    print("Read speeds (files/sec) saved to: read_speeds_fps.csv")

    # Read speeds CSV (MB/s)
    with open('read_speeds_mbs.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        header = ['Experiment'] + [f'Run {i+1} (MB/s)' for i in range(num_runs)]
        writer.writerow(header)
        for test_name, speeds in results.items():
            row = [test_name] + [f"{speed:.2f}" for speed in speeds['all_read_mbs']]
            writer.writerow(row)
    print("Read speeds (MB/s) saved to: read_speeds_mbs.csv")


def print_results_summary(results):
    """Print a formatted summary table of all results"""
    if not results:
        print("\nNo tests were run.")
        return

    print("\n" + "=" * 110)
    print("BENCHMARK RESULTS SUMMARY")
    print("=" * 110)
    print(f"{'Test Name':<35} {'Write (files/s)':<15} {'Write (MB/s)':<15} {'Read (files/s)':<15} {'Read (MB/s)':<15}")
    print("-" * 110)

    for test_name, speeds in results.items():
        write_fps = speeds['write_files_per_sec']
        read_fps = speeds['read_files_per_sec']
        write_mbs = speeds['write_speed_mbs']
        read_mbs = speeds['read_speed_mbs']
        print(f"{test_name:<35} {write_fps:>13.2f}  {write_mbs:>13.2f}  {read_fps:>13.2f}  {read_mbs:>13.2f}")

    print("=" * 110)


def main():
    parser = argparse.ArgumentParser(
        description='Run HopsFS performance benchmarks',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run all tests
  python run_benchmark.py --all

  # Run only HopsFS mount tests
  python run_benchmark.py --hopsfs-mount

  # Run MinIO and Java client tests
  python run_benchmark.py --minio --java-client

  # Run with custom configuration
  python run_benchmark.py --hopsfs-mount --num-files-small 5000 --parallel-writes 50

  # Run each experiment 3 times and average the results
  python run_benchmark.py --all --runs 3
        """
    )

    # Test group selection
    group = parser.add_argument_group('test groups')
    group.add_argument('--all', action='store_true',
                      help='Run all test groups')
    group.add_argument('--hopsfs-mount', action='store_true',
                      help='Run HopsFS mount tests')
    group.add_argument('--minio', '--s3', action='store_true', dest='minio',
                      help='Run MinIO/S3 tests')
    group.add_argument('--java-client', '--hdfs', action='store_true', dest='java_client',
                      help='Run HDFS Java client tests')

    # Configuration options
    config_group = parser.add_argument_group('configuration options')
    config_group.add_argument('--output-dir', type=str, default='/hopsfs/Jupyter/test',
                             help='Output directory for HopsFS mount tests (default: /hopsfs/Jupyter/test)')
    config_group.add_argument('--hdfs-output-dir', type=str, default='/Projects/test',
                             help='Output directory for HDFS tests (default: /Projects/test)')
    config_group.add_argument('--num-files-large', type=int, default=5,
                             help='Number of large files to test (default: 5)')
    config_group.add_argument('--num-files-small', type=int, default=1000,
                             help='Number of small files to test (default: 1000)')
    config_group.add_argument('--size-gb', type=float, default=1.0,
                             help='Size of large files in GB (default: 1.0)')
    config_group.add_argument('--size-kb', type=int, default=100,
                             help='Size of small files in KB (default: 100)')
    config_group.add_argument('--parallel-writes', type=int, default=32,
                             help='Number of parallel writers (default: 32)')
    config_group.add_argument('--bucket-large', type=str, default='test-large',
                             help='S3 bucket name for large files (default: test-large)')
    config_group.add_argument('--bucket-small', type=str, default='test-small',
                             help='S3 bucket name for small files (default: test-small)')
    config_group.add_argument('--runs', type=int, default=1,
                             help='Number of times to run each experiment (default: 1)')

    args = parser.parse_args()

    # If no group is specified, show help
    if not (args.all or args.hopsfs_mount or args.minio or args.java_client):
        parser.print_help()
        print("\nError: Please specify at least one test group to run.")
        sys.exit(1)

    # Build configuration dictionary
    config = {
        'output_dir': args.output_dir,
        'hdfs_output_dir': args.hdfs_output_dir,
        'num_files_large': args.num_files_large,
        'num_files_small': args.num_files_small,
        'size_gb': args.size_gb,
        'size_kb': args.size_kb,
        'parallel_writes': args.parallel_writes,
        'bucket_large': args.bucket_large,
        'bucket_small': args.bucket_small,
        'runs': args.runs,
    }

    # Determine which tests to run
    run_hopsfs = args.all or args.hopsfs_mount
    run_minio = args.all or args.minio
    run_hdfs = args.all or args.java_client

    print("HopsFS Performance Benchmark Runner")
    print("=" * 50)
    print(f"Test groups selected:")
    print(f"  - HopsFS Mount: {'Yes' if run_hopsfs else 'No'}")
    print(f"  - MinIO/S3:     {'Yes' if run_minio else 'No'}")
    print(f"  - Java Client:  {'Yes' if run_hdfs else 'No'}")
    print(f"Runs per experiment: {config['runs']}")
    print("=" * 50)

    # Run selected tests
    all_results = {}

    try:
        if run_hopsfs:
            results = run_hopsfs_mount_tests(config)
            all_results.update(results)

        if run_minio:
            results = run_minio_tests(config)
            all_results.update(results)

        if run_hdfs:
            results = run_java_client_tests(config)
            all_results.update(results)

        # Print summary
        print_results_summary(all_results)

        # Write results to CSV files
        write_results_to_csv(all_results)

        print("\n" + "=" * 50)
        print("Benchmark complete!")

    except KeyboardInterrupt:
        print("\n\nBenchmark interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nError during benchmark execution: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
