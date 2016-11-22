
with open('input') as f:
    lines = f.readlines()

raw_list = []

for line in lines:
    start = line.find('process-envelopes\":')
    start = line[start:].find(':') + start
    end   = line[start:].find(',') + start
    print line[start + 1:end]
    raw_list.append(line[start + 1:end])

start_idx = -1
end_idx = -1

for i in range(len(raw_list)):
    if (start_idx < 0 and i > 0 and raw_list[i] != raw_list[i - 1]):
        start_idx = i
    if (i == len(raw_list) - 2):
        end_idx = i
        break
    if (end_idx < 0 and start_idx >= 0 and i > start_idx and raw_list[i + 1] == raw_list[i + 2]):
        end_idx = i
        break

print start_idx
print end_idx

valid_list = []

for string in raw_list[start_idx : end_idx + 1]:
    valid_list.append(float(string))

print 'valid_list: ' + str(valid_list)

base = valid_list[0]
for i in range(len(valid_list)):
    valid_list[i] -= base

print 'minus base: ' + str(valid_list)

for i in range(len(valid_list)):
    if (i > 0):
        valid_list[i - 1] = (valid_list[i] - valid_list[i - 1]) / 10

print 'result: ' + str(valid_list[:len(valid_list) - 1])
