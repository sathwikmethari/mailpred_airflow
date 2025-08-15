import time
l = [[ele for ele in range(10)] for _ in range(1000)]
start_time = time.time()
unpacked =[]
for ele in l:
    unpacked.extend(ele)
print(f"Time taken: {time.time() - start_time:.4f} sec.")
print(unpacked[:20])