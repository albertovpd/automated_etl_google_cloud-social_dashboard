import os
from os import listdir
from os.path import isfile, join
#list of csv
only_csv = [f for f in listdir("../tmp") if isfile(join("../tmp", f))]

[os.remove("../tmp/{}".format(e)) for e in only_csv]
print(only_csv," removed ../tmp folder limpito") 