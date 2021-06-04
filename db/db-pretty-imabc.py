
import json
import db_covid

def parse_args():
    import argparse
    parser = argparse.ArgumentParser(description="Show stats for EXPID")
    parser.add_argument("--expid", help="The EXPID")
    args = parser.parse_args()
    return args

args = parse_args()

exp_int = db_covid.connect(args.expid)
if exp_int == "EXCEPTION":
    print("ERROR: could not connect to SQL for expid='%s'" %
          args.expid)
    exit(1)

if args.expid == None:
    expids = db_covid.get_expids()
else:
    expids = [ args.expid ]

def print_spaces(n):
    if n == 0:
        return # speed
    s = ""
    for i in range(0,n):
        s += " "
    print(s, end="")

def json_print(s, indent=0):
    if s is None: return
    J = json.loads(s)
    for k in J.keys():
        print_spaces(indent)
        print("%s: %s" % (k, J[k]))

def timestamp_print(label, d, indent=0):
    if d is None:
        ts = "--"
    else:
        ts = d.strftime("%Y-%m-%d %H:%M:%S")
    print_spaces(indent)
    print("%-6s %s" % (label+":", ts))

def times(d1, d2, indent=0):
    timestamp_print("start", d1, indent=indent)
    timestamp_print("stop",  d2, indent=indent)
    if d1 is not None and d2 is not None:
        diff = d2 - d1
        print_spaces(indent)
        print("diff:  " + str(diff)[0:9])

instances = db_covid.get_instances()
for row in instances:
    # print(str(row))
    instance = db_covid.maybe_string_int(row[2], "%3i")

    status_code = int(row[3])
    status = db_covid.RunStatus(status_code).name
    print("instance: %s %s" % (instance, status))

    times(row[6], row[7])

    print("json in:")
    json_print(row[4])
    print("json out:")
    json_print(row[5])

    print("runs:")
    runs = db_covid.get_runs(int(instance))
    for run in runs:
        status_code = int(run[4])
        status = db_covid.RunStatus(status_code).name
        print("run: %i" % run[3])
        # print(str(run))
        times(run[7], run[8], indent=2)
        print("  json in:")
        json_print(run[5], indent=2)
        print("  json out:")
        json_print(run[6], indent=2)

    print("")
    print("end: instance: " + instance)
