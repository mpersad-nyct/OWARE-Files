x=["one", "two", "three", "four"]
final = []
for i in x:
    final.append(i)
    final.append("\n")
final.pop()
with open("test_output.txt", "w+") as txt:
    txt.writelines(final)