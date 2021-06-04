
import db_covid

def parse_args():
    import argparse
    parser = argparse.ArgumentParser(description="Show stats for EXPID")
    parser.add_argument("expid", 
                        help="The EXPID")
    args = parser.parse_args()
    return args

args = parse_args()

db_covid.connect(args.expid)

count = db_covid.get_instances()

print("EXPID=%s has %i instances." % (args.expid, count))

