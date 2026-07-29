import sys
import os
import glob
import re

if len(sys.argv) < 2:
    print("Usage: python summarize_throughput.py <main_dir>")
    sys.exit(1)

main_dir = sys.argv[1]

print("\n=========================================================")
print("EXPERIMENT SUMMARY")
print("=========================================================")

subdirs = sorted([d for d in os.listdir(main_dir) if os.path.isdir(os.path.join(main_dir, d))])

for subdir in subdirs:
    full_subdir = os.path.join(main_dir, subdir)
    csv_files = glob.glob(os.path.join(full_subdir, "*.csv"))
    
    total_throughput = 0.0
    for csv_file in csv_files:
        with open(csv_file, 'r') as f:
            for line in f:
                if "Steady-State Average Throughput (events/sec):" in line:
                    match = re.search(r"Steady-State Average Throughput \(events/sec\):\s*([0-9.]+)", line)
                    if match:
                        total_throughput += float(match.group(1))
    
    print(f"Run {subdir} - Total Processed Throughput: {total_throughput:.2f} events/sec")
    
    with open(os.path.join(full_subdir, "result.txt"), "w") as rf:
        rf.write(f"Total Processed Throughput: {total_throughput:.2f} events/sec\n")
print("=========================================================\n")
