import os
import sys

def main():
    if (len(sys.argv) > 1):
        walk_dir = sys.argv[1]
    else:
        walk_dir = "."

    print('walk_dir = ' + walk_dir)
    print('walk_dir (absolute) = ' + os.path.abspath(walk_dir))

    for root,d_names,f_names in os.walk(walk_dir):
        for f in f_names:
            file_path = os.path.join(root, f)
            print(f'File: {file_path}')

if __name__ == "__main__":
    main()
