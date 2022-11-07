import glob
import time
import threading
import bisect

vals = []
thread_lock = threading.Lock()


class KeyWrapper:
    def __init__(self, iterable, key):
        self.it = iterable
        self.key = key

    def __getitem__(self, i):
        return self.key(self.it[i])

    def __len__(self):
        return len(self.it)


def process_text_file(file_path):
    file1 = open(file_path, 'r', encodings='utf-8')
    count = 0
    current_vals = []

    while True:
        # Get next line from file
        line = file1.readline()
        # if line is empty
        # end of file is reached
        if count == 0:
            count += 1
            continue
        if not line:
            break
        line = line.strip()
        words = line.split(" ")
        try:
            d_metal_col = words[3]
            print(d_metal_col)
            parsed_dmetal = float(d_metal_col)
            if parsed_dmetal > 5:
                global vals
                thread_lock.acquire()
                bslindex = bisect.bisect_left(KeyWrapper(vals, key=lambda c: c['dmetal']), parsed_dmetal)
                print(bslindex)
                row = {
                    'dmetal': parsed_dmetal,
                    'line': line
                }
                vals.insert(bslindex, row)
                thread_lock.release()
        except Exception as e:
            print(e)
            print('Invalid Dmetal Value')
            print("Line{}: {}".format(count, line.strip()))
    file1.close()


res = [f for f in glob.glob("/u/thaison/transfer/Training_Intern_Dmetal/*ascii*")]
# res = [f for f in glob.glob("./*ascii*")]
try:
    t = time.time()
    for f in res:
        print f
        t1 = threading.Thread(target=process_text_file, args=(f,))
        t1.start()
        t1.join()
    print "done in ", time.time() - t
    print(vals)
except:
    print "error"

f = open("report.txt", "w")
f.write("header header header")  # bega copy cai header vao day
for val in vals:
    f.write(val['line'] + "\n")
f.close()
