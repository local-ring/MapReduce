import random
import os

test_files_dir = "docs/"
# os.makedirs(test_files_dir, exist_ok=True)


num_files = 15
letters = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']
counts = {letter: 0 for letter in letters}  # To keep track of counts

for file_index in range(num_files):
    for l in letters:
        counts[l] = random.randint(10, 20)
    content = []
    for letter, count in counts.items():
        content.extend([letter] * count)

    random.shuffle(content) # shuffle the content to make mapper and reducer more confused

    file_path = os.path.join(test_files_dir, f"doc-{file_index}.txt")  # write the content to a file
    with open(file_path, 'w') as file:
        file.write(' '.join(content))  

    with open('count.txt', 'a') as f:
        f.write(f"{file_path} {counts}\n")




